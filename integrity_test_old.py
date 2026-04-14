import socket
import struct
import threading
import time
import json
import argparse
import random
import sys

# ==============================================================================
# PROTOCOL CONFIGURATION
# ==============================================================================
LSMT_TYPE_INT = 1
PIPELINE_DEPTH = 1024 

INSERT_CMD_FORMAT_PREFIX = "<QQ" # Key (u64), Timestamp (u64)
# Query Protocol
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN (16)] [MAX (16)]
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q"
QUERY_RESP_HEADER_SIZE = 8 + 8 + 1 + 16 + 16 # 49 bytes

# Body Header: [PAYLOAD_SIZE (8)] [RECORDS_COUNT (4)]
QUERY_BODY_HEAD_FMT = "<Q I"
QUERY_BODY_HEAD_SIZE = 12

# Record Header: [KEY_ID (8)] [KEY_TS (8)] [TYPE (1)] [LEN (4)]
RECORD_HEAD_FMT = "<Q Q B I"
RECORD_HEAD_SIZE = 8 + 8 + 1 + 4 # 21 bytes

# ==============================================================================
# SHARED STATE
# ==============================================================================
class SharedContext:
    def __init__(self):
        # Data
        self.all_requests = []       # Pre-generated write data
        self.committed_history = []  # Metadata of items successfully acked by server
        self.max_committed_id = 0
        
        # Locks
        self.history_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # State Flags
        self.writes_completed = False
        self.stop_benchmark = False

        # Metrics
        self.write_ops = 0
        self.read_ops = 0
        self.read_records = 0
        self.read_bytes = 0
        self.read_errors = 0 # Socket errors
        self.data_mismatches = 0 # Logical verification errors

    def record_write(self, count, items):
        with self.stats_lock:
            self.write_ops += count
        
        # Make data available to readers
        with self.history_lock:
            self.committed_history.extend(items)
            if items:
                self.max_committed_id = max(self.max_committed_id, items[-1]['id'])

    def record_read(self, bytes_len, record_count):
        with self.stats_lock:
            self.read_ops += 1
            self.read_bytes += bytes_len
            self.read_records += record_count

    def record_verification_error(self):
        with self.stats_lock:
            self.data_mismatches += 1

    def get_valid_query_range(self):
        """ Returns a random valid (start_id, end_id) tuple based on committed data. """
        # Optimistic read (no lock for speed)
        limit = self.max_committed_id
        if limit < 10:
            return None
        
        # Pick a random range
        start_id = random.randint(1, limit)
        # Range length between 1 and 1000
        length = random.randint(1, 1000)
        end_id = start_id + length
        
        return start_id, end_id

# ==============================================================================
# UTILS
# ==============================================================================
def create_insert_request(seq_id):
    """ 
    Creates a request where Key ID = seq_id and Value = seq_id.
    This allows stateless verification. 
    """
    real_content = seq_id
    real_content_size = 8 # uint64

    # Inner Payload: [Type][Len][Data]
    inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, real_content_size, real_content)
    inner_payload_size = len(inner_payload)

    # Key generation
    key_id = seq_id 
    key_timestamp = int(time.time() * 1000)

    # Outer Payload
    outer_payload_format = f"{INSERT_CMD_FORMAT_PREFIX}{inner_payload_size}s"
    outer_payload = struct.pack(
            outer_payload_format,
            key_id,
            key_timestamp,
            inner_payload 
            )

    # Prefix with Length for TCP framing
    message_length_prefix = struct.pack("<I", 4 + len(outer_payload))
    binary_packet = message_length_prefix + outer_payload

    metadata = {
            'id': key_id,
            'ts': key_timestamp,
            'val': real_content
            }

    return binary_packet, metadata 

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)["A"]

def read_exact(sock, n):
    data = bytearray()
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
            if not chunk: raise Exception("Socket closed")
            data.extend(chunk)
        except socket.timeout:
            continue # Non-blocking loop
    return bytes(data)

# ==============================================================================
# WRITER LOGIC
# ==============================================================================
class WriteWorker(threading.Thread):
    def __init__(self, ctx, nodes, start_idx, end_idx):
        super().__init__()
        self.ctx = ctx
        self.nodes = nodes
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.sock = None
        self.curr_node_idx = random.randint(0, len(nodes) - 1)

    def connect_leader(self):
        while not self.ctx.stop_benchmark:
            if self.sock:
                try: self.sock.close()
                except: pass
            
            node = self.nodes[self.curr_node_idx]
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((node['host'], node['port']))
                return
            except Exception:
                self.curr_node_idx = (self.curr_node_idx + 1) % len(self.nodes)
                time.sleep(0.1)

    def send_batch(self, packets):
        while not self.ctx.stop_benchmark:
            try:
                if self.sock is None: self.connect_leader()
                self.sock.sendall(b''.join(packets))
                # Wait for ACKs (1 byte per request)
                _ = read_exact(self.sock, len(packets))
                return True
            except Exception:
                self.sock = None
                self.curr_node_idx = (self.curr_node_idx + 1) % len(self.nodes)

    def run(self):
        batch_pkts = []
        batch_meta = []
        
        for i in range(self.start_idx, self.end_idx):
            if self.ctx.stop_benchmark: break
            
            pkt, meta = self.ctx.all_requests[i]
            batch_pkts.append(pkt)
            batch_meta.append(meta)

            if len(batch_pkts) >= PIPELINE_DEPTH:
                self.send_batch(batch_pkts)
                self.ctx.record_write(len(batch_pkts), batch_meta)
                batch_pkts = []
                batch_meta = []

        if batch_pkts and not self.ctx.stop_benchmark:
            self.send_batch(batch_pkts)
            self.ctx.record_write(len(batch_pkts), batch_meta)

# ==============================================================================
# READER / VERIFIER LOGIC
# ==============================================================================
class QueryWorker(threading.Thread):
    def __init__(self, ctx, nodes, worker_id):
        super().__init__()
        self.ctx = ctx
        self.nodes = nodes
        self.worker_id = worker_id
        self.sock = None
        self.active = False
        self.daemon = True 

    def connect(self):
        while not self.ctx.stop_benchmark:
            if self.sock:
                try: self.sock.close()
                except: pass

            node = random.choice(self.nodes)
            read_port = node['port'] + 4000
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Socket timeout is critical so we don't hang forever on partial reads
                self.sock.settimeout(5.0) 
                self.sock.connect((node['host'], read_port))
                self.active = True
                return
            except:
                time.sleep(0.5)

    def sender_loop(self):
        """ Continuously sends random range queries. """
        while self.active and not self.ctx.stop_benchmark:
            try:
                rng = self.ctx.get_valid_query_range()
                if not rng:
                    time.sleep(0.1)
                    continue
                
                start_id, end_id = rng
                req_id = random.randint(1, 1000000)
                
                # Request Format: [ReqID] [StartID] [StartTS] [EndID] [EndTS]
                # We ask for time range 0 to MAX to get latest version
                req_data = struct.pack(QUERY_REQ_FMT, req_id, start_id, 0, end_id, 2**64 - 1)
                
                self.sock.sendall(req_data)
                
                # Simple flow control to prevent overwhelming the socket buffer
                # if the server is slower than the client generator
                time.sleep(0.0001) 
            except Exception as e:
                self.active = False
                # print(f"[Sender {self.worker_id}] Error: {e}")
                break

    def receiver_loop(self):
        """ continuously reads responses and VERIFIES data integrity. """
        while self.active and not self.ctx.stop_benchmark:
            try:
                # 1. Read Response Header
                header_data = read_exact(self.sock, QUERY_RESP_HEADER_SIZE)
                (total_size, req_id, limit_reached, 
                 min_id, min_ts, max_id, max_ts) = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)

                body_size = total_size - QUERY_RESP_HEADER_SIZE
                
                # 2. Read Body Header
                # Even if body_size is small, the protocol likely sends the body struct 
                # if there is any data payload.
                
                records_count = 0
                if body_size > 0:
                    body_head = read_exact(self.sock, QUERY_BODY_HEAD_SIZE)
                    payload_len, records_count = struct.unpack(QUERY_BODY_HEAD_FMT, body_head)
                    
                    # 3. Read and Verify Records
                    for _ in range(records_count):
                        # A. Header
                        rec_head = read_exact(self.sock, RECORD_HEAD_SIZE)
                        key_id, key_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, rec_head)
                        
                        # B. Data
                        val_bytes = read_exact(self.sock, d_len)
                        
                        # C. Verification
                        # We wrote uint64 values. Let's unpack.
                        if d_len == 8:
                            val_int = struct.unpack("<Q", val_bytes)[0]
                            
                            # CRITICAL CHECK: We generated data such that Value == KeyID
                            if val_int != key_id:
                                print(f"\n[FATAL] Data Mismatch! ReqID: {req_id} | Key: {key_id} != Val: {val_int}")
                                self.ctx.record_verification_error()
                        else:
                            print(f"\n[WARN] Unexpected data length {d_len} for key {key_id}")
                
                # 4. Update Stats
                self.ctx.record_read(total_size, records_count)

            except Exception as e:
                self.active = False
                # print(f"[Receiver {self.worker_id}] Error: {e}")
                break

    def run(self):
        while not self.ctx.stop_benchmark:
            self.connect()
            t_send = threading.Thread(target=self.sender_loop, daemon=True)
            t_recv = threading.Thread(target=self.receiver_loop, daemon=True)
            
            t_send.start()
            t_recv.start()
            
            t_recv.join()
            t_send.join()
            
            if not self.ctx.stop_benchmark:
                time.sleep(0.5) # Backoff before reconnect

# ==============================================================================
# FINAL INTEGRITY CHECK
# ==============================================================================
def perform_full_scan(ctx, nodes, total_items_upper_bound):
    print(f"\n[Integrity Check] Starting Full Scan...")
    
    # We start from ID 0, TS 0.
    curr_id = 0
    curr_ts = 0
    
    total_retrieved = 0
    mismatches = 0
    
    sock = None
    node_idx = random.randint(0, len(nodes) - 1)

    while True: # Loop until server says no more data
        # 1. Connection Management (Same as before)
        if sock is None:
            try:
                node = nodes[node_idx]
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0) 
                sock.connect((node['host'], node['port'] + 4000))
            except Exception:
                if sock: sock.close()
                sock = None
                node_idx = (node_idx + 1) % len(nodes)
                time.sleep(0.2)
                continue

        # 2. Attempt Query
        try:
            req_id = 999999
            
            # CRITICAL CHANGE: 
            # We ask for Range: [curr_id, curr_ts] -> [UINT64_MAX, UINT64_MAX]
            # This ensures we get every single version.
            req_data = struct.pack(QUERY_REQ_FMT, req_id, curr_id, curr_ts, 2**64 - 1, 2**64 - 1)
            sock.sendall(req_data)
            
            head = read_exact(sock, QUERY_RESP_HEADER_SIZE)
            total_sz, _, limit, min_id, min_ts, max_id, max_ts = struct.unpack(QUERY_RESP_HEADER_FMT, head)
            
            body_sz = total_sz - QUERY_RESP_HEADER_SIZE
            records_count = 0

            if body_sz > 0:
                b_head = read_exact(sock, QUERY_BODY_HEAD_SIZE)
                p_len, records_count = struct.unpack(QUERY_BODY_HEAD_FMT, b_head)
                
                for _ in range(records_count):
                    r_head = read_exact(sock, RECORD_HEAD_SIZE)
                    k_id, k_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, r_head)
                    v_bytes = read_exact(sock, d_len)
                    
                    if d_len == 8:
                        val = struct.unpack("<Q", v_bytes)[0]
                        # Verify val matches key (stateless verification)
                        if val != k_id:
                            mismatches += 1
            
            total_retrieved += records_count
            
            # Progress bar
            if total_retrieved % 10000 == 0:
                 sys.stdout.write(f"\rScanned {total_retrieved} records...")
                 sys.stdout.flush()

            # 3. Determine Next Start Key
            if records_count == 0:
                # If server returns 0 records, we are definitely done
                break
            
            # CRITICAL LOGIC: Calculate next key based on last received key
            # Assuming your DB sorts by ID ASC, then Timestamp ASC (or DESC)
            # We need to simply increment the timestamp slightly to avoid getting the same record back.
            
            if max_ts < (2**64 - 1):
                curr_id = max_id
                curr_ts = max_ts + 1
            else:
                # Timestamp overflow (unlikely), move to next ID
                curr_id = max_id + 1
                curr_ts = 0

        except Exception as e:
            if sock: sock.close()
            sock = None
            node_idx = (node_idx + 1) % len(nodes)
            time.sleep(0.5)

    print(f"\n[Integrity Check] Complete. Retrieved: {total_retrieved}. Mismatches: {mismatches}")

def perform_full_scan_old_working(ctx, nodes, total_items):
    """
    Performs a sequential scan from Key 1 to Key N.
    Robust against connection drops (e.g., if connected to a Leader).
    """
    print(f"\n[Integrity Check] Starting Full Scan of {total_items} items...")
    
    curr_start = 1
    total_retrieved = 0
    mismatches = 0
    
    sock = None
    node_idx = random.randint(0, len(nodes) - 1)

    while curr_start <= total_items:
        # 1. Connection Management
        if sock is None:
            try:
                # Cycle through nodes to find a Follower
                node = nodes[node_idx]
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0) 
                sock.connect((node['host'], node['port'] + 4000))
            except Exception as e:
                if sock: sock.close()
                sock = None
                # Try next node
                node_idx = (node_idx + 1) % len(nodes)
                time.sleep(0.2)
                continue

        # 2. Attempt Query
        try:
            # Request chunk [curr_start ... total_items]
            req_id = 999999 #arbitrary number, req_id is irrelevant for this test.

            req_data = struct.pack(QUERY_REQ_FMT, req_id, curr_start, 0, total_items, 2**64 - 1)
            sock.sendall(req_data)
            
            # Read Header
            head = read_exact(sock, QUERY_RESP_HEADER_SIZE)
            total_sz, _, limit, min_id, min_ts, max_id, max_ts = struct.unpack(QUERY_RESP_HEADER_FMT, head)
            
            body_sz = total_sz - QUERY_RESP_HEADER_SIZE
            records_count = 0

            # Read Body
            if body_sz > 0:
                b_head = read_exact(sock, QUERY_BODY_HEAD_SIZE)
                p_len, records_count = struct.unpack(QUERY_BODY_HEAD_FMT, b_head)
                
                for _ in range(records_count):
                    r_head = read_exact(sock, RECORD_HEAD_SIZE)
                    k_id, k_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, r_head)
                    v_bytes = read_exact(sock, d_len)
                    
                    # Verification
                    if d_len == 8:
                        val = struct.unpack("<Q", v_bytes)[0]
                        if val != k_id:
                            mismatches += 1
                            if mismatches < 10: print(f"Mismatch Key {k_id} has val {val}")
            
            total_retrieved += records_count
            
            # Determine next start key
            if records_count == 0:
                # If we retrieved 0 records, verify if we are done or if it's a gap
                if not limit: 
                    # Server says no limit reached -> means we are at end of data
                    break 
                else:
                    # Limit reached but 0 records? (Should represent a large skipped gap or empty page)
                    # Advance cautiously
                    curr_start += 1
            else:
                # Standard Paging: Next page starts after the max key we just received
                curr_start = max_id + 1
            
            # Progress bar
            if total_retrieved % 10000 == 0:
                 sys.stdout.write(f"\rScanned {total_retrieved}/{total_items}...")
                 sys.stdout.flush()

        except Exception as e:
            # Socket closed or Timeout -> Leader rejected us, or network blip.
            # print(f"\n[Scan] Connection lost on node {node_idx}: {e}")
            if sock: sock.close()
            sock = None
            # Switch node immediately
            node_idx = (node_idx + 1) % len(nodes)
            # Do NOT increment curr_start; retry the same range on new node
            time.sleep(0.5)

    print(f"\n[Integrity Check] Complete. Retrieved: {total_retrieved}/{total_items}. Mismatches: {mismatches}")


def perform_full_scan_old(ctx, nodes, total_items):
    """
    Performs a sequential scan from Key 1 to Key N to ensure 
    every single item was persisted correctly.
    """
    print(f"\n[Integrity Check] Starting Full Scan of {total_items} items...")
    
    # Connect to a random node
    node = nodes[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((node['host'], node['port'] + 4000))
    except Exception as e:
        print(f"Failed to connect for integrity check: {e}")
        return

    curr_start = 1
    total_retrieved = 0
    mismatches = 0
    
    while curr_start <= total_items:
        # Request a chunk (paging)
        # We ask for [curr, Max], but protocol limits bytes, so we get a page.
        req_id = 999999
        req_data = struct.pack(QUERY_REQ_FMT, req_id, curr_start, 0, total_items, 2**64 - 1)
        sock.sendall(req_data)
        
        # Read Header
        head = read_exact(sock, QUERY_RESP_HEADER_SIZE)
        total_sz, _, limit, min_id, min_ts, max_id, max_ts = struct.unpack(QUERY_RESP_HEADER_FMT, head)
        
        body_sz = total_sz - QUERY_RESP_HEADER_SIZE
        if body_sz == 0:
            print(f"Premature end of data at key {curr_start}")
            break
            
        # Read Body
        b_head = read_exact(sock, QUERY_BODY_HEAD_SIZE)
        p_len, count = struct.unpack(QUERY_BODY_HEAD_FMT, b_head)
        
        for _ in range(count):
            r_head = read_exact(sock, RECORD_HEAD_SIZE)
            k_id, k_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, r_head)
            v_bytes = read_exact(sock, d_len)
            
            # Verify Value
            val = struct.unpack("<Q", v_bytes)[0]
            if val != k_id:
                mismatches += 1
                if mismatches < 10: print(f"Mismatch Key {k_id} has val {val}")
            
            # Verify Sequence (optional, but we expect sorted order)
            # if k_id != curr_start + idx... (not guaranteed if timestamps differ, but usually ordered by key)

        total_retrieved += count
        
        # Determine next start key
        # If limit reached, we continue from max_key found + 1 (or timestamp logic)
        # For simplicity in this check, assuming unique keys:
        if count == 0:
            break
            
        # LSMT iterator usually returns MaxKey. If we aren't done, seek next.
        # Logic: We received up to max_id. Next request starts at max_id + 1 (roughly)
        # Accurate paging requires using the keys returned.
        curr_start = max_id + 1
        
        # Progress bar
        if total_retrieved % 10000 == 0:
             sys.stdout.write(f"\rScanned {total_retrieved}/{total_items}...")
             sys.stdout.flush()

    print(f"\n[Integrity Check] Complete. Retrieved: {total_retrieved}/{total_items}. Mismatches: {mismatches}")

# ==============================================================================
# MAIN
# ==============================================================================
def stats_reporter(ctx):
    start_time = time.time()
    last_ops = 0
    
    print(f"{'Time':<8} | {'Writes':<8} | {'Reads':<8} | {'Recs/s':<8} | {'Errors':<8}")
    print("-" * 55)
    
    while not ctx.stop_benchmark:
        time.sleep(1.0)
        with ctx.stats_lock:
            r_ops = ctx.read_ops
            r_recs = ctx.read_records
            errs = ctx.data_mismatches
        
        # diff = r_ops - last_ops
        # last_ops = r_ops
        elapsed = int(time.time() - start_time)
        print(f"{elapsed:<8} | {ctx.write_ops:<8} | {r_ops:<8} | {r_recs:<8} | {errs:<8}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="cluster_conf.json")
    parser.add_argument("--writers", type=int, default=1)
    parser.add_argument("--readers", type=int, default=2)
    parser.add_argument("--requests", type=int, default=50000)
    args = parser.parse_args()

    ctx = SharedContext()
    nodes = load_config(args.config)
    print(f"Loaded {len(nodes)} nodes.")

    # 1. Generate Data
    print(f"Generating {args.requests} requests...")
    for i in range(args.requests):
        ctx.all_requests.append(create_insert_request(i+1)) # IDs 1 to N

    # 2. Start Stats
    t_stats = threading.Thread(target=stats_reporter, args=(ctx,), daemon=True)
    t_stats.start()

    # 3. Start Concurrent Readers (Verifiers)
    readers = []
    print(f"Starting {args.readers} Verifiers...")
    for i in range(args.readers):
        t = QueryWorker(ctx, nodes, i)
        t.start()
        readers.append(t)

    # 4. Start Writers
    writers = []
    chunk = args.requests // args.writers
    print(f"Starting {args.writers} Writers...")
    t0 = time.time()
    
    for i in range(args.writers):
        s = i * chunk
        e = s + chunk if i < args.writers - 1 else args.requests
        t = WriteWorker(ctx, nodes, s, e)
        t.start()
        writers.append(t)

    # 5. Wait for Writes
    for t in writers: t.join()
    print(f"\nWrites Finished in {time.time()-t0:.2f}s.")
    
    # 6. Let readers verify for a bit longer
    time.sleep(3)
    ctx.stop_benchmark = True
    
    # 7. Final Consistency Check
    perform_full_scan(ctx, nodes, args.requests)

if __name__ == "__main__":
    main()

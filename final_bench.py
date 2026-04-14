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
PIPELINE_DEPTH = 4096 

INSERT_CMD_FORMAT_PREFIX = "<QQ" # Key (u64), Timestamp (u64)
# Query Protocol
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN (16)] [MAX (16)]
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q"
QUERY_RESP_HEADER_SIZE = 8+8+1+16+16

# ==============================================================================
# SHARED STATE
# ==============================================================================
class SharedContext:
    def __init__(self):
        # Data
        self.all_requests = []       # Pre-generated write data
        self.committed_history = []  # Metadata of items successfully acked by server
        
        # Locks
        self.history_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # State Flags
        self.writes_completed = False
        self.stop_benchmark = False

        # Metrics
        self.write_ops = 0
        self.write_batches = 0
        self.read_ops = 0
        self.read_bytes = 0
        self.read_errors = 0
        self.write_errors = 0

    def record_write(self, count, items):
        with self.stats_lock:
            self.write_ops += count
            self.write_batches += 1
        
        # Make data available to readers
        with self.history_lock:
            self.committed_history.extend(items)

    def record_read(self, bytes_len):
        with self.stats_lock:
            self.read_ops += 1
            self.read_bytes += bytes_len

    def record_read_error(self):
        with self.stats_lock:
            self.read_errors += 1

    def get_valid_query_range(self):
        """
        Returns a (start_id, end_id) tuple based on actual data in DB.
        Returns None if DB is empty.
        """
        # Optimization: accessing len() is atomic in Python, 
        # but to be safe we use a lock or accept optimistic concurrency.
        # For speed, we optimistically grab an index.
        try:
            # We don't lock here to prevent readers from blocking writers.
            # Race condition: history might grow while we look, which is fine.
            hist_len = len(self.committed_history)
            if hist_len < 10:
                return None
            
            # Pick a random committed item as start
            idx = random.randint(0, hist_len - 1)
            start_item = self.committed_history[idx]
            start_id = start_item['id']
            
            # Random range length (e.g., 10 to 5000 records)
            range_len = random.randint(10, 5000)
            end_id = start_id + range_len
            
            return start_id, end_id
        except IndexError:
            return None

# ==============================================================================
# UTILS
# ==============================================================================
def create_insert_request(seq_id):
    """ Creates a valid, serialized insert request. """
    #real_content = random.randint(0, 2**64 - 1)
    real_content = seq_id
    real_content_size = 8 

    # Inner Payload: [Type][Len][Data]
    inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, real_content_size, real_content)
    inner_payload_size = len(inner_payload)

    # Key generation
    key_id = real_content 
    key_timestamp = int(time.time() * 1000)

    # Outer Payload
    outer_payload_format = f"{INSERT_CMD_FORMAT_PREFIX}{inner_payload_size}s"
    outer_payload = struct.pack(
            outer_payload_format,
            key_id,
            key_timestamp,
            inner_payload 
            )

    # Prefix with Length
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
            print("Timeout")
            continue 
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
        # Retry loop for Strong Consistency
        while not self.ctx.stop_benchmark:
            try:
                if self.sock is None: self.connect_leader()
                
                # 1. Send all data
                self.sock.sendall(b''.join(packets))
                
                # 2. Wait for ACKs (1 byte per request)
                _ = read_exact(self.sock, len(packets))
                return True
            except Exception:
                # print(f"Write failed, retrying...")
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

        # Flush remainder
        if batch_pkts and not self.ctx.stop_benchmark:
            self.send_batch(batch_pkts)
            self.ctx.record_write(len(batch_pkts), batch_meta)

# ==============================================================================
# READER LOGIC (Pipelined)
# ==============================================================================
class QueryWorker(threading.Thread):
    def __init__(self, ctx, nodes):
        super().__init__()
        self.ctx = ctx
        self.nodes = nodes
        self.sock = None
        self.active = False
        self.daemon = True # Die when main dies

    def connect(self):
        while not self.ctx.stop_benchmark:
            if self.sock:
                try: self.sock.close()
                except: pass

            node = random.choice(self.nodes)
            read_port = node['port'] + 4000
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(None) # Blocking mode for backpressure
                self.sock.connect((node['host'], read_port))
                self.active = True
                return
            except:
                time.sleep(0.5)

    def sender_loop(self):
        while self.active and not self.ctx.stop_benchmark:
            try:
                # 1. Get valid range from Writers
                rng = self.ctx.get_valid_query_range()
                if not rng:
                    time.sleep(0.1)
                    continue
                
                start_id, end_id = rng
                req_id = random.randint(1, 1000000)
                
                # 2. Pack Request (StartTS=0, EndTS=Max)
                req_data = struct.pack(QUERY_REQ_FMT, req_id, start_id, 0, end_id, 2**64 - 1)
                
                # 3. Send
                self.sock.sendall(req_data)
            except Exception as e:
                self.active = False
                print(f"[QuerySender] Error: {e}")
                break

    def receiver_loop(self):
        while self.active and not self.ctx.stop_benchmark:
            try:
                # 1. Read Header
                header_data = read_exact(self.sock, QUERY_RESP_HEADER_SIZE)
                (total_size, _, _, _, _, _, _) = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)

                # 2. Read Body
                body_size = total_size - QUERY_RESP_HEADER_SIZE
                if body_size > 0:
                    _ = read_exact(self.sock, body_size)

                # 3. Record
                self.ctx.record_read(total_size)
            except Exception as e:
                self.active = False
                print(f"[QueryReceiver] Error: {e}")
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
                self.ctx.record_read_error()

# ==============================================================================
# MAIN CONTROLLER
# ==============================================================================
def stats_reporter(ctx):
    last_w_ops = 0
    last_r_ops = 0
    start_time = time.time()
    
    print(f"{'Time':<8} | {'Write OPS':<10} | {'Read OPS':<10} | {'Read BW (MB/s)':<15} | {'Committed':<10}")
    print("-" * 65)
    
    while not ctx.stop_benchmark:
        time.sleep(1.0)
        
        with ctx.stats_lock:
            curr_w_ops = ctx.write_ops
            curr_r_ops = ctx.read_ops
            curr_r_bytes = ctx.read_bytes
            ctx.read_bytes = 0 # Reset BW counter for rate calculation
            
        w_rate = curr_w_ops - last_w_ops
        r_rate = curr_r_ops - last_r_ops
        last_w_ops = curr_w_ops
        last_r_ops = curr_r_ops
        
        bw_mb = curr_r_bytes / (1024 * 1024)
        elapsed = int(time.time() - start_time)
        
        print(f"{elapsed:<8} | {w_rate:<10} | {r_rate:<10} | {bw_mb:<15.2f} | {curr_w_ops:<10}")

def main():
    parser = argparse.ArgumentParser(description="Unified Raft DB Benchmark")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--writers", type=int, default=1, help="Number of Writer Threads")
    parser.add_argument("--readers", type=int, default=1, help="Number of Reader Connections")
    parser.add_argument("--requests", type=int, default=100000, help="Total items to insert")
    args = parser.parse_args()

    # 1. Init Context & Load Config
    ctx = SharedContext()
    nodes = load_config(args.config)
    print(f"Loaded {len(nodes)} nodes.")

    # 2. Pre-generate Data
    print(f"Generating {args.requests} requests in RAM...")
    t0 = time.time()
    # Using sequential IDs 0..N for predictable querying
    for i in range(args.requests):
        ctx.all_requests.append(create_insert_request(i+1))
    print(f"Generation took {time.time() - t0:.2f}s.")

    # 3. Start Reporters
    t_stats = threading.Thread(target=stats_reporter, args=(ctx,), daemon=True)
    t_stats.start()

    # 4. Start Readers (They will idle until data appears)
    readers = []
    print(f"Starting {args.readers} Reader pipelines...")
    for _ in range(args.readers):
        t = QueryWorker(ctx, nodes)
        t.start()
        readers.append(t)

    # 5. Start Writers
    writers = []
    chunk_size = args.requests // args.writers
    print(f"Starting {args.writers} Writers...")
    
    start_time = time.time()
    for i in range(args.writers):
        s = i * chunk_size
        e = s + chunk_size if i < args.writers - 1 else args.requests
        t = WriteWorker(ctx, nodes, s, e)
        t.start()
        writers.append(t)

    # 6. Wait for Writes
    for t in writers:
        t.join()
    
    total_time = time.time() - start_time
    ctx.writes_completed = True
    print(f"\n[Writes Completed] {args.requests} items in {total_time:.2f}s ({args.requests/total_time:.2f} ops/s)")
    
    # 7. Let readers run for a few more seconds to benchmark pure read speed
    print("Letting readers drain for 5 seconds...")
    time.sleep(5)
    
    # 8. Shutdown
    ctx.stop_benchmark = True
    # Readers are daemon threads or check the flag, so we can just exit or join
    print("Benchmark finished.")

if __name__ == "__main__":
    main()

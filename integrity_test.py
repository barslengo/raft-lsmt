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
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response Header
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q"
QUERY_RESP_HEADER_SIZE = 8 + 8 + 1 + 16 + 16 # 49 bytes

# Body Header
QUERY_BODY_HEAD_FMT = "<Q I"
QUERY_BODY_HEAD_SIZE = 12

# Record Header
RECORD_HEAD_FMT = "<Q Q B I"
RECORD_HEAD_SIZE = 8 + 8 + 1 + 4 # 21 bytes

# ==============================================================================
# SHARED STATE
# ==============================================================================
class SharedContext:
    def __init__(self):
        # Data
        self.all_requests = []       
        self.committed_history = []  
        self.max_committed_id = 0
        
        # Locks
        self.history_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # State Flags
        self.stop_benchmark = False

        # Metrics
        self.write_ops = 0

    def record_write(self, count, items):
        with self.stats_lock:
            self.write_ops += count
        with self.history_lock:
            self.committed_history.extend(items)
            if items:
                self.max_committed_id = max(self.max_committed_id, items[-1]['id'])

# ==============================================================================
# UTILS
# ==============================================================================
def create_insert_request(seq_id):
    real_content = seq_id
    real_content_size = 8 
    inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, real_content_size, real_content)
    inner_payload_size = len(inner_payload)
    key_id = seq_id 
    key_timestamp = int(time.time() * 1000)
    outer_payload_format = f"{INSERT_CMD_FORMAT_PREFIX}{inner_payload_size}s"
    outer_payload = struct.pack(outer_payload_format, key_id, key_timestamp, inner_payload)
    message_length_prefix = struct.pack("<I", 4 + len(outer_payload))
    binary_packet = message_length_prefix + outer_payload
    metadata = {'id': key_id, 'ts': key_timestamp, 'val': real_content}
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
        while not self.ctx.stop_benchmark:
            try:
                if self.sock is None: self.connect_leader()
                self.sock.sendall(b''.join(packets))
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
# INTEGRITY & STATS LOGIC
# ==============================================================================
def perform_full_scan_with_stats(ctx, nodes, total_items_upper_bound):
    print(f"\n" + "="*60)
    print(f"[Integrity Check] Starting Full Scan")
    print("="*60)
    
    # State for iteration
    curr_id = 0
    curr_ts = 0
    
    # Metrics
    total_retrieved = 0
    mismatches = 0
    total_bytes_read = 0
    total_queries = 0
    
    start_time = time.time()
    last_print_time = start_time
    last_print_bytes = 0
    last_print_recs = 0

    sock = None
    node_idx = random.randint(0, len(nodes) - 1)

    try:
        while True: # Loop until server says no more data
            # 1. Connection Management
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
                
                # Fetch [curr_id, curr_ts] -> [MAX, MAX]
                req_data = struct.pack(QUERY_REQ_FMT, req_id, curr_id, curr_ts, 2**64 - 1, 2**64 - 1)
                sock.sendall(req_data)
                
                # Metrics: Header Overhead
                total_bytes_read += QUERY_REQ_SIZE 

                # Read Header
                head = read_exact(sock, QUERY_RESP_HEADER_SIZE)
                total_bytes_read += QUERY_RESP_HEADER_SIZE

                total_sz, _, limit, min_id, min_ts, max_id, max_ts = struct.unpack(QUERY_RESP_HEADER_FMT, head)
                
                body_sz = total_sz - QUERY_RESP_HEADER_SIZE
                records_count = 0

                if body_sz > 0:
                    b_head = read_exact(sock, QUERY_BODY_HEAD_SIZE)
                    total_bytes_read += QUERY_BODY_HEAD_SIZE
                    
                    p_len, records_count = struct.unpack(QUERY_BODY_HEAD_FMT, b_head)
                    
                    for _ in range(records_count):
                        r_head = read_exact(sock, RECORD_HEAD_SIZE)
                        k_id, k_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, r_head)
                        v_bytes = read_exact(sock, d_len)
                        
                        total_bytes_read += (RECORD_HEAD_SIZE + d_len)

                        if d_len == 8:
                            val = struct.unpack("<Q", v_bytes)[0]
                            if val != k_id:
                                mismatches += 1
                
                total_retrieved += records_count
                total_queries += 1
                
                # 3. Stats Reporting (Every 0.5s)
                now = time.time()
                if now - last_print_time > 0.5:
                    delta = now - last_print_time
                    
                    # Bandwidth
                    bytes_diff = total_bytes_read - last_print_bytes
                    bw_mbps = (bytes_diff / (1024*1024)) / delta
                    
                    # Throughput
                    recs_diff = total_retrieved - last_print_recs
                    recs_ps = recs_diff / delta
                    
                    sys.stdout.write(f"\r[Scan] Recs: {total_retrieved:<9} | Speed: {recs_ps:<8.1f} rec/s | BW: {bw_mbps:<6.2f} MB/s | Mismatches: {mismatches}")
                    sys.stdout.flush()
                    
                    last_print_time = now
                    last_print_bytes = total_bytes_read
                    last_print_recs = total_retrieved

                # 4. Next Key Logic
                if records_count == 0:
                    break
                
                # Increment slightly to avoid duplicate receipt
                if max_ts < (2**64 - 1):
                    curr_id = max_id
                    curr_ts = max_ts + 1
                else:
                    curr_id = max_id + 1
                    curr_ts = 0

            except Exception as e:
                # print(f"Error: {e}")
                if sock: sock.close()
                sock = None
                node_idx = (node_idx + 1) % len(nodes)
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nScan interrupted.")

    total_time = time.time() - start_time
    if total_time == 0: total_time = 0.001
    
    avg_recs_ps = total_retrieved / total_time
    avg_mbps = (total_bytes_read / (1024*1024)) / total_time
    avg_qps = total_queries / total_time

    print(f"\n\n" + "="*60)
    print(f"FINAL REPORT")
    print(f"="*60)
    print(f"Total Time      : {total_time:.4f} s")
    print(f"Total Records   : {total_retrieved}")
    print(f"Total Bytes     : {total_bytes_read / (1024*1024):.2f} MB")
    print(f"Total Queries   : {total_queries} (Page Fetches)")
    print(f"------------------------------------------------------------")
    print(f"Avg Throughput  : {avg_recs_ps:.2f} records/s")
    print(f"Avg Bandwidth   : {avg_mbps:.2f} MB/s")
    print(f"Avg Latency/Q   : {(1/avg_qps)*1000:.2f} ms")
    print(f"Query Rate      : {avg_qps:.2f} req/s")
    print(f"Mismatches      : {mismatches}")
    print(f"="*60)

# ==============================================================================
# MAIN
# ==============================================================================
def write_stats_reporter(ctx):
    start_time = time.time()
    print(f"{'Time':<8} | {'Writes':<8} | {'Status'}")
    print("-" * 35)
    while not ctx.stop_benchmark:
        time.sleep(1.0)
        elapsed = int(time.time() - start_time)
        with ctx.stats_lock:
            w_ops = ctx.write_ops
        print(f"{elapsed:<8} | {w_ops:<8} | Writing...")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="cluster_conf.json")
    parser.add_argument("--writers", type=int, default=1)
    parser.add_argument("--requests", type=int, default=50000)
    args = parser.parse_args()

    ctx = SharedContext()
    nodes = load_config(args.config)
    print(f"Loaded {len(nodes)} nodes.")

    # 1. Generate Data
    print(f"Generating {args.requests} requests...")
    for i in range(args.requests):
        ctx.all_requests.append(create_insert_request(i+1))

    # 2. Start Write Stats
    t_stats = threading.Thread(target=write_stats_reporter, args=(ctx,), daemon=True)
    t_stats.start()

    # 3. Start Writers
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

    # 4. Wait for Writers
    for t in writers: t.join()
    write_dur = time.time() - t0
    print(f"\nWrites Finished in {write_dur:.2f}s ({args.requests/write_dur:.0f} ops/s).")
    
    # Stop the write stats reporter
    ctx.stop_benchmark = True
    time.sleep(0.5) # Let stats thread finish printing

    # 5. Run Integrity Scan (Single Threaded, Detailed Stats)
    perform_full_scan_with_stats(ctx, nodes, args.requests)

if __name__ == "__main__":
    main()

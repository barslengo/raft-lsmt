import struct
import time
import random
import argparse
import threading
import socket
from client import RaftClient

# --- Configuration ---
LSMT_TYPE_INT = 1
INSERT_CMD_FORMAT_PREFIX = "<QQ" # Key (u64), Timestamp (u64)
PIPELINE_DEPTH = 4096 

# --- Global Shared State ---

# 1. Pre-computed source data: List of (binary_packet, metadata_dict)
ALL_REQUESTS = []

# 2. History of what has actually been written to the DB (for Reads)
COMMITTED_HISTORY = [] 
history_lock = threading.Lock()

# 3. Statistics (Protected by stats_lock)
stats_lock = threading.Lock()
stop_event = threading.Event()

# Counters
total_committed_count = 0  # Writes
total_queries_count = 0    # Read Requests
total_records_fetched = 0  # Read Data (rows)

def create_insert_request(seq_id):
    """ Creates a valid, serialized insert request. """
    real_content = random.randint(0, 2**64 - 1)
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


# -------------------------------------------------------------------------
# WRITE WORKER
# -------------------------------------------------------------------------
def write_worker(thread_id, cluster_conf, start_idx, end_idx):
    """
    Takes a slice of ALL_REQUESTS and sends them via RaftClient.
    """
    print(f"[Writer-{thread_id}] Started. Processing {end_idx - start_idx} items.")
    
    client = RaftClient(cluster_conf)
    
    current_batch_pkts = []
    current_batch_meta = []
    
    global total_committed_count

    for i in range(start_idx, end_idx):
        packet, meta = ALL_REQUESTS[i]
        
        current_batch_pkts.append(packet)
        current_batch_meta.append(meta)

        # Flush Batch
        if len(current_batch_pkts) >= PIPELINE_DEPTH:
            try:
                # 1. Send to Raft (Blocking until ACK)
                client.send_batch_reliable(current_batch_pkts)
                
                # 2. Update Stats & History
                with stats_lock:
                    total_committed_count += len(current_batch_pkts)
                
                with history_lock:
                    COMMITTED_HISTORY.extend(current_batch_meta)
                
                # Reset
                current_batch_pkts = []
                current_batch_meta = []
            except Exception as e:
                print(f"[Writer-{thread_id}] Error: {e}")

    # Flush remaining
    if current_batch_pkts:
        try:
            client.send_batch_reliable(current_batch_pkts)
            with stats_lock:
                total_committed_count += len(current_batch_pkts)
            with history_lock:
                COMMITTED_HISTORY.extend(current_batch_meta)
        except Exception as e:
             print(f"[Writer-{thread_id}] Final flush error: {e}")

    print(f"[Writer-{thread_id}] Finished.")

# -------------------------------------------------------------------------
# READ WORKER
# -------------------------------------------------------------------------
def read_worker(thread_id, cluster_conf):
    """
    Periodically performs range queries on keys strictly picked from COMMITTED_HISTORY.
    """
    print(f"[Reader-{thread_id}] Started.")
    
    node_ids = list(cluster_conf.keys())
    sock = None
    
    global total_queries_count
    global total_records_fetched

    # Local counters to batch stat updates (optional optimization)
    # For now, we update global stats every query to be responsive.

    while not stop_event.is_set():
        time.sleep(0.1) # Frequency of reads
        
        # 1. Get a Snapshot of available data
        target_a = None
        target_b = None
        
        with history_lock:
            limit = len(COMMITTED_HISTORY)
            if limit > 100:
                idx_a = random.randint(0, limit - 1)
                idx_b = random.randint(0, limit - 1)
                target_a = COMMITTED_HISTORY[idx_a]
                target_b = COMMITTED_HISTORY[idx_b]
        
        if target_a is None:
            continue

        # 2. Prepare Composite Keys (Tuple compare for Start <= End)
        key_a_tuple = (target_a['id'], target_a['ts'])
        key_b_tuple = (target_b['id'], target_b['ts'])

        if key_a_tuple < key_b_tuple:
            start_id, start_ts = key_a_tuple
            end_id, end_ts     = key_b_tuple
        else:
            start_id, start_ts = key_b_tuple
            end_id, end_ts     = key_a_tuple

        # 3. Connect (lazy)
        if sock is None:
            try:
                target_id = random.choice(node_ids)
                host, base_port = cluster_conf[target_id]
                read_port = base_port + 4000
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((host, read_port))
            except Exception as e:
                if sock: sock.close()
                sock = None
                continue

        # 4. Send & Recv
        try:
            req = struct.pack("<QQQQ", start_id, start_ts, end_id, end_ts)
            sock.sendall(req)

            header = read_exact(sock, 4)
            payload_len = struct.unpack("<I", header)[0]
            
            payload = read_exact(sock, payload_len - 4)
            count = struct.unpack("<I", payload[0:4])[0]

            # 5. Update Global Read Stats
            with stats_lock:
                total_queries_count += 1
                total_records_fetched += count

        except Exception as e:
            # print(f"[Reader-{thread_id}] Error: {e}")
            if sock: sock.close()
            sock = None
    
    if sock: sock.close()
    print(f"[Reader-{thread_id}] Stopped.")

# -------------------------------------------------------------------------
# MONITOR
# -------------------------------------------------------------------------
def monitor_thread(total_target):
    start_time = time.time()
    
    # Track previous values to calculate deltas
    last_committed = 0
    last_queries = 0
    last_records = 0
    
    print(f"{'Time':<8} | {'CommitTotal':<12} | {'Write RPS':<10} | {'Read QPS':<10} | {'Read Rec/s':<10}")
    print("-" * 65)

    with open("benchmark_results.csv", "w") as f:
        f.write("Time,Committed_Total,Write_RPS,Read_QPS,Read_Recs_Sec\n")
        
        while not stop_event.is_set():
            time.sleep(0.5) # Update every 500ms for readability
            
            with stats_lock:
                curr_committed = total_committed_count
                curr_queries = total_queries_count
                curr_records = total_records_fetched
            
            elapsed = time.time() - start_time
            dt = 0.5 

            # Calculate Rates
            write_rps = (curr_committed - last_committed) / dt
            read_qps  = (curr_queries - last_queries) / dt
            read_recs_sec = (curr_records - last_records) / dt
            
            # Print to Console
            #print(f"{elapsed:<8.1f} | {curr_committed:<12} | {write_rps:<10.0f} | {read_qps:<10.1f} | {read_recs_sec:<10.0f}")

            # Write to CSV
            f.write(f"{elapsed:.1f},{curr_committed},{write_rps:.0f},{read_qps:.1f},{read_recs_sec:.0f}\n")
            #f.flush()
            
            # Update history
            last_committed = curr_committed
            last_queries = curr_queries
            last_records = curr_records

            # If all writes are done, we still might want to monitor reads for a bit, 
            # but usually we stop when writes are done in this script structure.
            if curr_committed >= total_target:
                break

# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--requests", type=int, default=100000)
    parser.add_argument("--threads", type=int, default=1, help="Number of concurrent write threads")
    parser.add_argument("--readers", type=int, default=1, help="Number of concurrent read threads")
    args = parser.parse_args()

    import json
    with open(args.config, 'r') as f:
        data = json.load(f)
        cluster_conf = {n['id']: (n['host'], n['port']) for n in data}

    print(f"--- 1. Pre-computing {args.requests} requests ---")
    gen_start = time.time()
    for i in range(args.requests):
        ALL_REQUESTS.append(create_insert_request(i))
    print(f"Generation took {time.time() - gen_start:.2f}s")

    print(f"--- 2. Starting Benchmark (Writers: {args.threads}, Readers: {args.readers}) ---")
    
    threads = []
    
    # Monitor
    mon = threading.Thread(target=monitor_thread, args=(args.requests,))
    mon.start()

    # Readers
    for i in range(args.readers):
        t = threading.Thread(target=read_worker, args=(i, cluster_conf))
        t.start()
        threads.append(t)

    # Writers
    bench_start = time.time()
    write_threads = []
    chunk_size = args.requests // args.threads
    
    for i in range(args.threads):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < args.threads - 1 else args.requests
        t = threading.Thread(target=write_worker, args=(i, cluster_conf, start_idx, end_idx))
        t.start()
        write_threads.append(t)

    # Join Writers (Wait for all inserts to finish)
    for t in write_threads:
        t.join()
    
    bench_end = time.time()
    duration = bench_end - bench_start
    
    # Stop Readers
    stop_event.set()
    mon.join()
    for t in threads:
        t.join()

    print(f"\nBenchmark Finished.")
    print(f"Total Requests: {args.requests}")
    print(f"Duration:       {duration:.2f} s")
    print(f"Write Throughput: {args.requests/duration:.2f} Req/s")
    
    # Print Read Summary
    with stats_lock:
        print(f"Total Reads:      {total_queries_count} Queries")
        print(f"Total Fetched:    {total_records_fetched} Records")

if __name__ == "__main__":
    main()

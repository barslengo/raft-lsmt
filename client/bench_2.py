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

# 2. History of what has actually been written to the DB
# Readers will pick from this list.
COMMITTED_HISTORY = [] 
history_lock = threading.Lock()

# 3. Stats
total_committed_count = 0
stats_lock = threading.Lock()
stop_event = threading.Event()

# --- NEW: LEADER TRACKING ---
CURRENT_LEADER_ID = None
leader_lock = threading.Lock()

def get_current_leader():
    with leader_lock:
        return CURRENT_LEADER_ID

def update_leader(node_id):
    global CURRENT_LEADER_ID
    with leader_lock:
        if CURRENT_LEADER_ID != node_id:
            CURRENT_LEADER_ID = node_id
            print(f"[System] Leader identified: Node {node_id}")

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
    
    initial_node = get_current_leader()
    client = RaftClient(cluster_conf, leader_id=initial_node)

    # If the client successfully connected, update the global state immediately
    if client.current_node_id is not None:
        update_leader(client.current_node_id)
    
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
                id_before_send = client.current_node_id 
                client.send_batch_reliable(current_batch_pkts)
                
                # Check if failover occurred
                if client.current_node_id != id_before_send:
                    # The leader changed, update the global variable.
                    update_leader(client.current_node_id)
               
                # Update Stats
                with stats_lock:
                    total_committed_count += len(current_batch_pkts)
                
                # Update Read Availability (Thread-safe)
                with history_lock:
                    COMMITTED_HISTORY.extend(current_batch_meta)
                
                # Reset
                current_batch_pkts = []
                current_batch_meta = []
            except Exception as e:
                print(f"[Writer-{thread_id}] Error: {e}")
                # In a real app, you would retry here. 
                # RaftClient.send_batch_reliable already retries connections, 
                # so this catches critical logic errors.

    # Flush remaining
    if current_batch_pkts:
        try:
            id_before_send = client.current_node_id
            client.send_batch_reliable(current_batch_pkts)
            if client.current_node_id != id_before_send:
                update_leader(client.current_node_id)

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
def try_connect_to_any_follower(sock, current_node, all_node_ids, cluster_conf):
    """
    Ensures we have a valid connection to a FOLLOWER.
    Returns: (socket_obj, node_id)
    """
    leader = get_current_leader()

    # 1. Define Candidates (All nodes except leader, unless it's a 1-node cluster)
    if leader is not None and len(all_node_ids) > 1:
        candidates = [n for n in all_node_ids if n != leader]
    else:
        candidates = all_node_ids

    # 2. Validation: If we are currently connected to the Leader, disconnect.
    if sock and current_node == leader and len(all_node_ids) > 1:
        # print(f"Disconnecting from Node {current_node} (It became Leader)")
        sock.close()
        sock = None
        current_node = None

    # 3. Connect if needed
    if sock is None:
        try:
            target_id = random.choice(candidates)
            host, base_port = cluster_conf[target_id]
            read_port = base_port + 4000
            
            new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_sock.settimeout(2.0)
            new_sock.connect((host, read_port))
            
            return new_sock, target_id  # Return new socket and ID
        except Exception:
            if new_sock: new_sock.close()
            return None, None # Signal failure
            
    # If sock was already valid and pointing to a follower, just return it as is
    return sock, current_node

def read_worker(thread_id, cluster_conf):
    """
    Periodically performs range queries on keys strictly picked from COMMITTED_HISTORY.
    Constructs the full 128-bit key (ID + Timestamp) for the range.
    """
    print(f"[Reader-{thread_id}] Started.")
    
    node_ids = list(cluster_conf.keys())
    sock = None
    current_node = None
    
    queries_performed = 0
    records_fetched = 0

    while not stop_event.is_set():
        time.sleep(0.1) # Frequency of reads
        
        # 1. Get a Snapshot of available data
        target_a = None
        target_b = None
        
        with history_lock:
            limit = len(COMMITTED_HISTORY)
            if limit > 100:
                # Pick two random items from what has been written so far
                idx_a = random.randint(0, limit - 1)
                idx_b = random.randint(0, limit - 1)
                target_a = COMMITTED_HISTORY[idx_a]
                target_b = COMMITTED_HISTORY[idx_b]
        
        if target_a is None:
            continue

        # 2. Connection Logic (Refactored)
        # Pass current_node in, get updated current_node back
        sock, current_node = try_connect_to_any_follower(
            sock, current_node, all_node_ids, cluster_conf
        )

        # If connection failed, skip this iteration
        if sock is None:
            continue

        # 2. Prepare Composite Keys
        # The key is [ID (u64)] + [TS (u64)]
        # We perform a tuple comparison to ensure Start <= End
        key_a_tuple = (target_a['id'], target_a['ts'])
        key_b_tuple = (target_b['id'], target_b['ts'])

        if key_a_tuple < key_b_tuple:
            start_id, start_ts = key_a_tuple
            end_id, end_ts     = key_b_tuple
        else:
            start_id, start_ts = key_b_tuple
            end_id, end_ts     = key_a_tuple

        try:
            # Request Format: [StartKey(16B)] [EndKey(16B)]
            # Each Key: <QQ (Little Endian u64, u64)
            # Total pack string: <QQQQ
            req = struct.pack("<QQQQ", start_id, start_ts, end_id, end_ts)
            sock.sendall(req)

            # Read Header: [TotalLen(4)]
            header = read_exact(sock, 4)
            payload_len = struct.unpack("<I", header)[0]
            
            # Read Payload: [Count(4)][Records...]
            payload = read_exact(sock, payload_len - 4)
            
            # Parse result count
            count = struct.unpack("<I", payload[0:4])[0]

            queries_performed += 1
            records_fetched += count
            
            # Optional: Print progress
            # print(f"[Reader-{thread_id}] Range Query found {count} items")

        except Exception as e:
            # print(f"[Reader-{thread_id}] Error: {e}")
            if sock: sock.close()
            sock = None
            current_node = None
    
    if sock: sock.close()
    print(f"[Reader-{thread_id}] Stopped. Queries: {queries_performed}, Keys Fetched: {records_fetched}")

# -------------------------------------------------------------------------
# MONITOR
# -------------------------------------------------------------------------
def monitor_thread(total_target):
    start_time = time.time()
    last_count = 0
    
    with open("benchmark_results.csv", "w") as f:
        f.write("Time,Committed,RPS\n")
        
        while not stop_event.is_set():
            time.sleep(0.1)
            with stats_lock:
                current = total_committed_count
            
            elapsed = time.time() - start_time
            rps = (current - last_count) / 0.1
            
            f.write(f"{elapsed:.1f},{current},{rps:.0f}\n")
            f.flush()
            
            last_count = current
            if current >= total_target:
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
    print(f"Throughput:     {args.requests/duration:.2f} Req/s")

if __name__ == "__main__":
    main()

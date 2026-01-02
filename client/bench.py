import struct
import time
import random
import argparse
import threading
import queue
import json
from client import RaftClient

# --- Configuration ---
LSMT_TYPE_INT = 1
INSERT_CMD_FORMAT_PREFIX = "<QQ" # Key (u64), Timestamp (u64)

# Batch size settings
PIPELINE_DEPTH = 4096 

# Global stats for monitoring
stats_lock = threading.Lock()
total_committed = 0

def create_insert_request(seq_id):
    """
    Creates a valid, serialized insert request. This function correctly
    builds the inner payload in the [type][size][data] format that the
    lsmt.c storage engine expects, and then wraps it in the outer command.
    """
    # --- Step 1: Create the "Real Content" ---
    # This is the actual value you want to store.
    # We will simulate storing a 8-byte unsigned integer.
    real_content = random.randint(0, 2**64 - 1)
    real_content_size = 8 # This corresponds to sizeof(uint64_t)

    # --- Step 2: Create the "Inner Payload" (the value for the LSM-Tree) ---
    # This is the buffer that lsmt_insert() will receive.
    # Format: [content_type (1 byte)] [size_of_real_content (4 bytes)] [real_content (N bytes)]
    inner_payload = struct.pack(
        "<BIQ",  # B=uint8_t, I=uint32_t, Q=uint64_t
        LSMT_TYPE_INT,
        real_content_size,
        real_content
    )
    # The total size of this inner payload. This will become the `content_size` in the outer command.
    inner_payload_size = len(inner_payload) # This will be 1 + 4 + 8 = 13 bytes

    # --- Step 3: Create the "Outer Command" (the full Raft log entry) ---
    # Format: [key (16 bytes)] [content_size (1 byte)] [inner_payload (N bytes)]
    #key_id = random.randint(0, 2**64 - 1)
    key_id = real_content
    key_timestamp = int(time.time())

    # The format string for the full command payload.
    # The 's' format specifier takes a byte string.
    outer_payload_format = f"{INSERT_CMD_FORMAT_PREFIX}{inner_payload_size}s"
    
    outer_payload = struct.pack(
        outer_payload_format,
        key_id,
        key_timestamp,
        #inner_payload_size, # This is the value for the `content_size` field.
        inner_payload       # This is the value for the `encoded_data` field.
    )

    # --- Step 4: Prepend the entire message with its 4-byte network length ---
    message_length_prefix = struct.pack("<I", 4 + len(outer_payload))
    
    return message_length_prefix + outer_payload

def producer_thread(q, num_requests):
    """
    Generates requests and pushes them to the queue.
    """
    print(f"[Producer] Starting to generate {num_requests} requests...")
    
    for i in range(num_requests):
        req = create_insert_request(i)
        q.put(req)
        
        # Optional: throttling if queue gets too big to save RAM
        if q.qsize() > PIPELINE_DEPTH * 10:
            time.sleep(0.01)

    # Signal finished
    q.put(None)
    print("[Producer] Finished generating.")

def consumer_thread(q, cluster_conf):
    """
    Consumes requests and sends them using the Cluster-Aware Consumer.
    """
    # Instantiate with the dictionary
    client = RaftClient(cluster_conf)
    
    current_batch = []
    global total_committed

    while True:
        try:
            # 1. Fill Batch
            while len(current_batch) < PIPELINE_DEPTH:
                try:
                    timeout = 0.05 if len(current_batch) > 0 else 1.0
                    item = q.get(timeout=timeout)
                    if item is None:
                        if current_batch:
                            client.send_batch_reliable(current_batch)
                            with stats_lock: total_committed += len(current_batch)
                        return
                    current_batch.append(item)
                except queue.Empty:
                    break

            # 2. Send Batch (Handles Failover internally)
            if current_batch:
                client.send_batch_reliable(current_batch)
                
                with stats_lock:
                    total_committed += len(current_batch)
                current_batch = [] 

        except Exception as e:
            print(f"[Consumer] Critical Loop Error: {e}")
            time.sleep(1)

def monitor_thread(stop_event, total_target, queue_obj):
    """
    Prints throughput stats every 100ms.
    """
    last_count = 0
    start_time = time.time()
    
    with open("client_stats.csv", "w") as f:
        f.write("Time_Sec,Committed_Total,Pending_Reqs\n")

        while not stop_event.is_set():
            time.sleep(0.1)
            #time.sleep(1.0)
            with stats_lock:
                current = total_committed

            elapsed = time.time() - start_time
            pending_reqs = queue_obj.qsize()

            f.write(f"{elapsed:.2f},{current},{pending_reqs}\n")
            f.flush()
            
            diff = current - last_count
            last_count = current
            
            #print(f"Status: {current}/{total_target} committed | Speed: {diff} RPS")
            
            if current >= total_target:
                break

def main():
    parser = argparse.ArgumentParser(description="Distributed DB Benchmark Client")
    parser.add_argument("--config", type=str, required=True, help="Path to cluster config JSON file")
    parser.add_argument("--requests", type=int, default=100000, help="Total number of requests")
    args = parser.parse_args()

    # --- Load Configuration from JSON ---
    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
        
        cluster_conf = {}
        # Parse JSON list into Dict {id: (host, port)}
        for node in config_data:
            node_id = int(node['id'])
            host = node['host']
            port = int(node['port'])
            cluster_conf[node_id] = (host, port)
            
        print(f"Loaded Cluster Config: {cluster_conf}")
        
        if not cluster_conf:
            print("Error: Config file is empty or invalid.")
            return

    except FileNotFoundError:
        print(f"Error: Config file '{args.config}' not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Config file '{args.config}' is not valid JSON.")
        return
    except KeyError as e:
        print(f"Error: Config file missing required field: {e}")
        return

    # --- Start Benchmark ---
    request_queue = queue.Queue(maxsize=PIPELINE_DEPTH * 20)
    stop_event = threading.Event()

    prod = threading.Thread(target=producer_thread, args=(request_queue, args.requests))
    cons = threading.Thread(target=consumer_thread, args=(request_queue, cluster_conf))
    mon = threading.Thread(target=monitor_thread, args=(stop_event, args.requests,
                                                        request_queue))

    prod.start()
    cons.start()
    mon.start()

    start_time = time.time()
    prod.join()
    cons.join()
    stop_event.set()
    mon.join()

    duration = time.time() - start_time
    print(f"\nBenchmark Complete. Avg RPS: {args.requests/duration:.2f}")

if __name__ == "__main__":
    main()

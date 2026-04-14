import json
import argparse
import random
import time
import threading
import struct
from typing import List, Dict, Set
from benchmark_core import (
    Node, Router, HashRoutingStrategy, RoundRobinRoutingStrategy, LeaderRoutingStrategy,
    StatsTracker, WriteWorker, QueryWorker, create_insert_request, TCPClient,
    QUERY_REQ_FMT, QUERY_RESP_HEADER_FMT, QUERY_RESP_HEADER_SIZE, LeaderRegistry
)

def stats_reporter(stats: StatsTracker, stop_event: threading.Event):
    last_snap = stats.get_snapshot()
    print(f"{'Time':<8} | {'Write OPS':<10} | {'Read OPS':<10} | {'Read BW (MB/s)':<15} | {'Committed':<10} | {'Errors (W/R)':<12}")
    print("-" * 85)
    
    while not stop_event.is_set():
        time.sleep(1.0)
        snap = stats.get_snapshot()
        
        w_rate = snap["write_ops"] - last_snap["write_ops"]
        r_rate = snap["read_ops"] - last_snap["read_ops"]
        r_bytes_delta = snap["read_bytes"] - last_snap["read_bytes"]
        bw_mb = r_bytes_delta / (1024 * 1024)
        
        print(f"{int(snap['elapsed']):<8} | {w_rate:<10} | {r_rate:<10} | {bw_mb:<15.2f} | {snap['write_ops']:<10} | {snap['write_errors']}/{snap['read_errors']:<10}")
        last_snap = snap

def verify_data(router: Router, total_requests: int, leader_registry: LeaderRegistry):
    print(f"\n--- Verification Phase ---")
    print(f"Verifying {total_requests} records...")
    
    found_keys: Set[int] = set()
    client = TCPClient(timeout=10.0)
    
    def connect_to_follower():
        leader = leader_registry.get_leader()
        available = [n for n in router.nodes if n != leader]
        if not available: available = router.nodes
        
        for _ in range(len(available) * 2):
            node = random.choice(available)
            try:
                client.connect(node.host, node.port + 4000)
                return True
            except:
                continue
        return False

    if not connect_to_follower():
        print("Failed to connect for verification.")
        return False

    current_start_id = 1
    batch_size = 100_000 #5000
    start_time = time.time()
    
    while len(found_keys) < total_requests and current_start_id <= total_requests:
        end_id = min(current_start_id + batch_size - 1, total_requests)
        req_id = random.randint(1, 1000000)
        
        # Request from current_start_id to end_id
        req_data = struct.pack(QUERY_REQ_FMT, req_id, current_start_id, 0, end_id, 2**64 - 1)
        
        try:
            client.send(req_data)
            header_data = client.recv_exact(QUERY_RESP_HEADER_SIZE)
            unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
            
            # Indices: 0:total_size, 1:req_id, 2:limit, 3:min_id, 4:min_ts, 5:max_id, 6:max_ts, 7:records_bytes, 8:records_count
            total_size = unpacked[0]
            limit_reached = unpacked[2]
            records_bytes = unpacked[7]
            records_count = unpacked[8]
            
            body_size = total_size - QUERY_RESP_HEADER_SIZE
            
            max_parsed_id = -1
            if body_size > 0:
                body = client.recv_exact(body_size)
                if records_bytes > 0 and records_count > 0:
                    record_size = records_bytes // records_count
                    offset = 0
                    for _ in range(records_count):
                        if offset + 8 > len(body): break
                        key_id = struct.unpack_from("<Q", body, offset)[0]
                        found_keys.add(key_id)
                        max_parsed_id = max(max_parsed_id, key_id)
                        offset += record_size
            
            if limit_reached > 0 and max_parsed_id != -1:
                # If limited, resume from the next ID after the max one we actually parsed
                current_start_id = max_parsed_id + 1
            elif limit_reached > 0 and max_parsed_id == -1:
                # Edge case: limited but no records parsed (shouldn't happen, but prevent infinite loop)
                current_start_id += 1
            else:
                # Not limited, we successfully got everything in this range
                current_start_id = end_id + 1
            
            if len(found_keys) % 10000 < batch_size and len(found_keys) > 0:
                print(f"Retrieved {len(found_keys)} unique keys... (Searched up to ID: {current_start_id - 1})")
                
        except Exception as e:
            print(f"Error during verification at ID {current_start_id}: {e}")
            if not connect_to_follower():
                break
            time.sleep(1)

    elapsed = time.time() - start_time
    print(f"Verification completed in {elapsed:.2f}s")
    print(f"Total unique keys found: {len(found_keys)} / {total_requests}")
    
    if len(found_keys) == total_requests:
        print("SUCCESS: All data correctly stored and retrieved.")
        return True
    else:
        missing = total_requests - len(found_keys)
        print(f"FAILURE: {missing} keys are missing from the database.")
        return False

def main():
    parser = argparse.ArgumentParser(description="Modular Raft DB Benchmark Runner")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--writers", type=int, default=1, help="Number of Writer Threads")
    parser.add_argument("--readers", type=int, default=1, help="Number of Reader Connections")
    parser.add_argument("--requests", type=int, default=100000, help="Total items to insert")
    parser.add_argument("--routing-strategy", choices=["hash", "round-robin", "leader"], default="hash", help="Routing strategy to use")
    parser.add_argument("--verify", action="store_true", help="Run verification phase after benchmark")
    args = parser.parse_args()

    # 1. Load Config
    with open(args.config, 'r') as f:
        config_data = json.load(f)["A"]
    nodes = [Node(n['id'], n['host'], n['port']) for n in config_data]
    
    # 2. Setup Router
    strategy = {
        "hash": HashRoutingStrategy(),
        "round-robin": RoundRobinRoutingStrategy(),
        "leader": LeaderRoutingStrategy()
    }[args.routing_strategy]
    router = Router(nodes, strategy)
    
    # 3. Pre-generate Data
    print(f"Generating {args.requests} requests...")
    requests = [create_insert_request(i + 1) for i in range(args.requests)]
    
    # 4. Start Telemetry
    stats = StatsTracker()
    leader_registry = LeaderRegistry()
    stop_event = threading.Event()
    history = []
    history_lock = threading.Lock()
    
    reporter_thread = threading.Thread(target=stats_reporter, args=(stats, stop_event), daemon=True)
    reporter_thread.start()

    # 5. Start Workers
    readers = [QueryWorker(router, stats, stop_event, history, history_lock, leader_registry) for _ in range(args.readers)]
    for r in readers: r.start()

    writers = []
    chunk_size = args.requests // args.writers
    start_time = time.time()
    for i in range(args.writers):
        start = i * chunk_size
        end = start + chunk_size if i < args.writers - 1 else args.requests
        w = WriteWorker(requests[start:end], router, stats, stop_event, history, history_lock, leader_registry)
        w.start()
        writers.append(w)

    # 6. Wait for completion
    for w in writers: w.join()
    
    total_write_time = time.time() - start_time
    print(f"\nWrites completed in {total_write_time:.2f}s ({args.requests/total_write_time:.2f} ops/s)")
    
    print(f"Draining readers for 5s...")
    time.sleep(5)
    stop_event.set()
    
    # 7. Verification Phase
    if args.verify:
        time.sleep(3)
        verify_data(router, args.requests, leader_registry)

    print("Benchmark finished.")

if __name__ == "__main__":
    main()

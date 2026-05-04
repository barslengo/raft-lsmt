import json
import argparse
import random
import time
import struct
import os
import glob
from typing import List, Dict, Set

from benchmark_core import (
    Node, Router, HashRoutingStrategy, TCPClient,
    QUERY_REQ_FMT, QUERY_RESP_HEADER_FMT, QUERY_RESP_HEADER_SIZE, LeaderRegistry
)


def load_dump_files(file_paths: List[str]) -> List[Dict]:
    """Load all key ID and timestamp pairs from JSON dump files.
    
    JSON format: [{'id': int, 'ts': int}, ...]
    Returns list of dicts with 'id' and 'ts' keys.
    """
    all_records = []
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            data = json.load(f)
        all_records.extend(data)
    return all_records


def find_dump_files(folder_path: str) -> List[str]:
    """Find all JSON dump files in a folder."""
    return glob.glob(os.path.join(folder_path, '*-dump.json'))


def check_integrity(router: Router, expected_records: List[Dict], leader_registry: LeaderRegistry) -> bool:
    """Check if all expected {id, ts} records are present in the database.
    
    Uses per-cluster paging: each cluster maintains its own query range.
    """
    print(f"\n--- Integrity Check ---")
    print(f"Checking {len(expected_records)} records...")
    
    # Build set of expected (id, ts) pairs
    expected_set = set((r['id'], r['ts']) for r in expected_records)
    
    # Get min and max ID from expected records for the query range
    min_id = min(r['id'] for r in expected_records)
    max_id = max(r['id'] for r in expected_records)
    
    found_records: Set = set()  # Will store (id, ts) tuples
    clients: Dict[str, TCPClient] = {}
    
    # Per-cluster paging state
    cluster_current_start = {}
    cluster_limit_reached = {}
    
    def connect_to_followers():
        success = True
        for cluster_name, nodes in router.clusters.items():
            if cluster_name not in clients:
                clients[cluster_name] = TCPClient(timeout=10.0)
            
            client = clients[cluster_name]
            if client.sock is not None:
                continue
            
            leader = leader_registry.get_leader(cluster_name)
            available = [n for n in nodes if n != leader]
            if not available: available = nodes
            
            connected = False
            for _ in range(len(available) * 2):
                node = random.choice(available)
                try:
                    client.connect(node.host, node.port + 4000)
                    connected = True
                    break
                except:
                    continue
            if not connected:
                success = False
        return success
    
    if not connect_to_followers():
        print("Failed to connect to all clusters for verification.")
        return False
    
    # Initialize per-cluster state
    for cluster_name in router.clusters.keys():
        cluster_current_start[cluster_name] = min_id
        cluster_limit_reached[cluster_name] = False
    
    batch_size = 5000
    start_time = time.time()
    
    while True:
        all_clusters_done = True
        any_found = False
        
        for cluster_name, client in clients.items():
            current_start_id = cluster_current_start[cluster_name]
            
            if current_start_id > max_id:
                continue
            
            end_id = min(current_start_id + batch_size - 1, max_id)
            req_id = random.randint(1, 1000000)
            
            req_data = struct.pack(QUERY_REQ_FMT, req_id, current_start_id, 0, end_id, 2**64 - 1)
            
            try:
                client.send(req_data)
                
                header_data = client.recv_exact(QUERY_RESP_HEADER_SIZE)
                if not header_data:
                    raise Exception("Connection closed")
                unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                
                total_size = unpacked[0]
                limit_reached = unpacked[2]
                records_bytes = unpacked[7]
                records_count = unpacked[8]
                
                cluster_limit_reached[cluster_name] = (limit_reached > 0)
                
                body_size = total_size - QUERY_RESP_HEADER_SIZE
                if body_size > 0:
                    body = client.recv_exact(body_size)
                    if records_bytes > 0 and records_count > 0:
                        record_size = records_bytes // records_count
                        offset = 0
                        cluster_max_parsed_id = -1
                        for _ in range(records_count):
                            if offset + 16 > len(body):
                                break
                            # Each record is key_id (8 bytes) + timestamp (8 bytes)
                            key_id = struct.unpack_from("<Q", body, offset)[0]
                            timestamp = struct.unpack_from("<Q", body, offset + 8)[0]
                            pair = (key_id, timestamp)
                            if pair in expected_set:
                                found_records.add(pair)
                                any_found = True
                            cluster_max_parsed_id = max(cluster_max_parsed_id, key_id)
                            offset += record_size
                        
                        # Per-cluster paging logic
                        if cluster_limit_reached[cluster_name] and cluster_max_parsed_id != -1:
                            cluster_current_start[cluster_name] = cluster_max_parsed_id + 1
                        elif cluster_limit_reached[cluster_name] and cluster_max_parsed_id == -1:
                            cluster_current_start[cluster_name] += 1
                        else:
                            cluster_current_start[cluster_name] = end_id + 1
                        
                        if cluster_current_start[cluster_name] <= max_id:
                            all_clusters_done = False
                else:
                    # No body, advance this cluster
                    cluster_current_start[cluster_name] = end_id + 1
                    if cluster_current_start[cluster_name] <= max_id:
                        all_clusters_done = False
                
            except Exception as e:
                print(f"Error during verification for {cluster_name} at ID {current_start_id}: {e}")
                client.close()
                if not connect_to_followers():
                    return False
                else:
                    # Reset this cluster's state to retry from same position
                    cluster_current_start[cluster_name] = current_start_id
                    all_clusters_done = False
                time.sleep(0.1)
        
        # Check termination: all clusters done OR all expected records found
        if all_clusters_done:
            break
        
        if len(found_records) >= len(expected_set):
            break
        
        if any_found and len(found_records) % 10000 < batch_size:
            min_current = min(cluster_current_start.values())
            print(f"Retrieved {len(found_records)} records... (Searched up to ID: {min_current - 1})")
    
    # Final check: query each missing record individually with exact (id, ts)
    missing = expected_set - found_records
    if missing:
        print(f"\n{len(missing)} records missing from batch queries, checking individually...")
        still_missing = set()
        for key_id, ts in sorted(missing):
            req_id = random.randint(1, 1000000)
            # Query exact (id, ts) range on all clusters
            found = False
            for client in clients.values():
                req_data = struct.pack(QUERY_REQ_FMT, req_id, key_id, ts, key_id, ts)
                try:
                    client.send(req_data)
                    header_data = client.recv_exact(QUERY_RESP_HEADER_SIZE)
                    if not header_data:
                        continue
                    unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                    total_size = unpacked[0]
                    body_size = total_size - QUERY_RESP_HEADER_SIZE
                    if body_size > 0:
                        body = client.recv_exact(body_size)
                        records_bytes = unpacked[7]
                        records_count = unpacked[8]
                        if records_bytes > 0 and records_count > 0:
                            record_size = records_bytes // records_count
                            offset = 0
                            for _ in range(records_count):
                                if offset + 16 > len(body):
                                    break
                                rid = struct.unpack_from("<Q", body, offset)[0]
                                rts = struct.unpack_from("<Q", body, offset + 8)[0]
                                if rid == key_id and rts == ts:
                                    found_records.add((key_id, ts))
                                    found = True
                                    break
                                offset += record_size
                except Exception as e:
                    pass
                if found:
                    break
            if not found:
                still_missing.add((key_id, ts))
        missing = still_missing
    
    elapsed = time.time() - start_time
    print(f"Integrity check completed in {elapsed:.2f}s")
    print(f"Total unique records found: {len(found_records)} / {len(expected_set)}")
    
    # Close all clients
    for client in clients.values():
        client.close()
    
    if len(found_records) == len(expected_set):
        print("SUCCESS: All records are present in the database.")
        return True
    else:
        print(f"FAILURE: {len(missing)} records are missing from the database.")
        if len(missing) <= 10:
            print(f"Missing records: {sorted(missing)}")
        else:
            print(f"Missing records (first 10): {sorted(list(missing))[:10]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Integrity Check - Verify JSON dump keys are in the database")
    
    # Mutually exclusive group for records input
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--records-folder", type=str, 
                       help="Folder containing JSON dump files (loads all *-dump.json files)")
    group.add_argument("--records", type=str, nargs='+',
                       help="Space-separated list of JSON dump files")
    
    parser.add_argument("--config", required=True, 
                       help="Path to cluster_conf.json")
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    clusters = {}
    for cluster_name, nodes_data in config_data.items():
        clusters[cluster_name] = [
            Node(cluster_name=cluster_name, id=n['id'], host=n['host'], port=n['port'])
            for n in nodes_data
        ]
    
    # Setup Router (using hash strategy for querying)
    router = Router(clusters, HashRoutingStrategy())
    leader_registry = LeaderRegistry()
    
    # Load dump files
    if args.records_folder:
        dump_files = find_dump_files(args.records_folder)
        if not dump_files:
            print(f"No dump files found in {args.records_folder}")
            return 0
        print(f"Loaded {len(dump_files)} dump files from {args.records_folder}")
    else:
        dump_files = args.records
        print(f"Loaded {len(dump_files)} dump files from arguments")
    
    # Extract all expected records
    expected_records = load_dump_files(dump_files)
    print(f"Total records to verify: {len(expected_records)}")
    
    # Run integrity check
    success = check_integrity(router, expected_records, leader_registry)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())

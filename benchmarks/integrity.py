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


def load_dump_files(file_paths: List[str]) -> Set[int]:
    """Load all key IDs from JSON dump files."""
    all_keys = set()
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            data = json.load(f)
        for entry in data:
            all_keys.add(entry['id'])
    return all_keys


def find_dump_files(folder_path: str) -> List[str]:
    """Find all JSON dump files in a folder."""
    return glob.glob(os.path.join(folder_path, '*-dump.json'))


def connect_to_followers(router: Router, leader_registry: LeaderRegistry) -> Dict[str, TCPClient]:
    """Connect to follower nodes in all clusters."""
    clients = {}
    for cluster_name, nodes in router.clusters.items():
        clients[cluster_name] = TCPClient(timeout=10.0)
        client = clients[cluster_name]
        
        leader = leader_registry.get_leader(cluster_name)
        available = [n for n in nodes if n != leader]
        if not available:
            available = nodes
        
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
            raise Exception(f"Failed to connect to any follower in cluster {cluster_name}")
    return clients


def query_range(client: TCPClient, start_id: int, end_id: int, req_id: int) -> Set[int]:
    """Query a range of keys from a single client and return found keys."""
    found_keys = set()
    req_data = struct.pack(QUERY_REQ_FMT, req_id, start_id, 0, end_id, 2**64 - 1)
    
    try:
        client.send(req_data)
        header_data = client.recv_exact(QUERY_RESP_HEADER_SIZE)
        if not header_data:
            return found_keys
        
        unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
        total_size = unpacked[0]
        records_bytes = unpacked[7]
        records_count = unpacked[8]
        
        body_size = total_size - QUERY_RESP_HEADER_SIZE
        if body_size > 0:
            body = client.recv_exact(body_size)
            if records_bytes > 0 and records_count > 0:
                record_size = records_bytes // records_count
                offset = 0
                for _ in range(records_count):
                    if offset + 8 > len(body):
                        break
                    key_id = struct.unpack_from("<Q", body, offset)[0]
                    found_keys.add(key_id)
                    offset += record_size
    except Exception as e:
        print(f"Error querying range {start_id}-{end_id}: {e}")
    
    return found_keys


def check_integrity(router: Router, expected_keys: Set[int], leader_registry: LeaderRegistry) -> bool:
    """Check if all expected keys are present in the database."""
    print(f"\n--- Integrity Check ---")
    print(f"Checking {len(expected_keys)} keys...")
    
    if not expected_keys:
        print("No keys to check.")
        return True
    
    min_id = min(expected_keys)
    max_id = max(expected_keys)
    
    # Sort expected keys for efficient range queries
    sorted_keys = sorted(expected_keys)
    
    clients = {}
    # Connect to all clusters
    try:
        clients = connect_to_followers(router, leader_registry)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return False
    
    found_keys: Set[int] = set()
    start_time = time.time()
    
    # Query in batches to find all keys
    batch_size = 5000
    current_start = min_id
    
    while len(found_keys) < len(expected_keys) and current_start <= max_id:
        current_end = min(current_start + batch_size - 1, max_id)
        req_id = random.randint(1, 1000000)
        
        # Scatter query to all clusters
        all_batch_keys = set()
        for client in clients.values():
            batch_keys = query_range(client, current_start, current_end, req_id)
            all_batch_keys.update(batch_keys)
        
        found_keys.update(all_batch_keys)
        
        # Print progress every batch
        if len(found_keys) % 10000 == 0 or len(found_keys) > 0:
            print(f"Retrieved {len(found_keys)} unique keys... (Searched up to ID: {current_end})")
        
        # If we found keys in this batch, check if we can skip ahead
        if all_batch_keys:
            max_found_in_batch = max(all_batch_keys)
            # Move to next batch after the last found key
            current_start = max_found_in_batch + 1
            # But ensure we don't skip over any expected keys
            # Find the next expected key after max_found_in_batch
            for key in sorted_keys:
                if key > max_found_in_batch:
                    current_start = key
                    break
        else:
            # No keys found, move forward
            current_start = current_end + 1
    
    # Also check if any expected keys are beyond max_id (shouldn't happen but just in case)
    # Do a final check for any remaining keys
    remaining_keys = expected_keys - found_keys
    if remaining_keys:
        print(f"\n{len(remaining_keys)} keys not found, checking individually...")
        for key in sorted(remaining_keys):
            req_id = random.randint(1, 1000000)
            for client in clients.values():
                batch_keys = query_range(client, key, key, req_id)
                if key in batch_keys:
                    found_keys.add(key)
                    break
    
    elapsed = time.time() - start_time
    print(f"Integrity check completed in {elapsed:.2f}s")
    print(f"Total unique keys found: {len(found_keys)} / {len(expected_keys)}")
    
    # Close all clients
    for client in clients.values():
        client.close()
    
    missing = expected_keys - found_keys
    if not missing:
        print("SUCCESS: All keys are present in the database.")
        return True
    else:
        print(f"FAILURE: {len(missing)} keys are missing from the database.")
        if len(missing) <= 10:
            print(f"Missing keys: {sorted(missing)}")
        else:
            print(f"Missing keys (first 10): {sorted(list(missing))[:10]}")
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
    
    # Extract all expected keys
    expected_keys = load_dump_files(dump_files)
    print(f"Total keys to verify: {len(expected_keys)}")
    
    # Run integrity check
    success = check_integrity(router, expected_keys, leader_registry)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())

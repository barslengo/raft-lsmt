import argparse
import json
import random
import sys
import time
from typing import List, Optional

from client import Node, Record, QueryRequest
from client import DbClient, DbClientConfig
from client import Router, HashRoutingStrategy, RoundRobinRoutingStrategy, LeaderRoutingStrategy

from concurrent.futures import wait

batch_metrics_list = []

def write_batch_cb(m: BatchMetrics) -> None:
    batch_metrics_list.append(m)
    return

class ZipfGenerator:
    """Generates keys following a Zipfian distribution."""
    def __init__(self, n: int, alpha: float = 1.0):
        self.n = n
        size = min(n, 100000)
        self.population = list(range(1, size + 1))
        self.weights = [1.0 / (i ** alpha) for i in range(1, size + 1)]

    def next(self) -> int:
        return random.choices(self.population, weights=self.weights, k=1)[0]


def bulk_records(amount: int, data_distribution: str, base_timestamp: int = 0) -> List[Record]:
    """Generate a list of Record objects based on the specified distribution."""
    print(f"Generating {amount} records (Data Dist: {data_distribution})...")
    
    records = []
    current_ts = base_timestamp
    
    if data_distribution == 'sequential':
        keys = list(range(1, amount + 1))
    elif data_distribution == 'uniform':
        keys = list(range(1, amount + 1))
        random.shuffle(keys)
    elif data_distribution == 'zipfian':
        zipf = ZipfGenerator(amount)
        keys = [zipf.next() for _ in range(amount)]
    else:
        raise ValueError(f"Unknown distribution: {data_distribution}")
    
    for key_id in keys:
        records.append(Record(key_id=key_id, timestamp=current_ts, content=key_id))
        current_ts += 1
    
    return records


def show(results: dict, verbose: bool = False, max_depth: int = 5, max_length: int = 10):
    """Display query results."""
    
    def convert_and_truncate(obj, depth=0):
        """Recursively convert objects for JSON serialization and truncate long lists/depth."""
        if depth > max_depth:
            return "<Max Depth Reached>"
            
        # Convert dataclasses (like Node, Record) to dicts
        if hasattr(obj, '__dataclass_fields__'):
            obj = obj.__dict__
            
        if isinstance(obj, dict):
            return {str(k): convert_and_truncate(v, depth + 1) for k, v in obj.items()}
            
        elif isinstance(obj, (list, tuple)):
            if len(obj) > max_length:
                half = max_length // 2
                first_part =[convert_and_truncate(x, depth + 1) for x in obj[:half]]
                last_part =[convert_and_truncate(x, depth + 1) for x in obj[-half:]]
                return first_part +[f"... ({len(obj) - max_length} more items omitted) ..."] + last_part
            else:
                return [convert_and_truncate(x, depth + 1) for x in obj]
                
        else:
            # Basic types returned as-is, everything else cast to string
            if isinstance(obj, (int, float, bool, str, type(None))):
                return obj
            return str(obj)

    if verbose:
        truncated_results = convert_and_truncate(results)
        print(json.dumps(truncated_results, indent=2))
    else:
        if not isinstance(results, dict):
            print(results)
            return

        for cluster_name, node_list in results.items():
            print(f"\n=== Cluster: {cluster_name} ===")
            
            if not isinstance(node_list, list):
                print(node_list)
                continue

            for node_entry in node_list:
                if not isinstance(node_entry, dict):
                    continue
                
                for node_name, node_data in node_entry.items():
                    print(f"\n[ Node: {node_name} ]")
                    
                    # Unpack the (node, response) tuple returned by read()
                    if isinstance(node_data, tuple) and len(node_data) == 2:
                        _, response = node_data
                    else:
                        response = node_data
                        
                    if not isinstance(response, dict):
                        print(f"    {response}")
                        continue
                        
                    # Print metadata
                    for k, v in response.items():
                        if k == "records":
                            continue
                        if isinstance(v, str) and len(v) > 100:
                            v = f"{v[:100]}..."
                        print(f"    {k}: {v}")
                        
                    # Print records with length truncation
                    records = response.get("records",[])
                    print(f"    records ({len(records)} total items):")
                    
                    if len(records) > max_length:
                        half = max_length // 2
                        for rec in records[:half]:
                            print(f"      - {rec}")
                        print(f"      ... ({len(records) - max_length} records omitted) ...")
                        for rec in records[-half:]:
                            print(f"      - {rec}")
                    else:
                        for rec in records:
                            print(f"      - {rec}")
                            
                    if not records:
                        print("      (empty)")



def write_sync(dbclient: DbClient, records: List[Record], verbose: bool):
    wait(dbclient.write(records, verbose=verbose))
    if batch_metrics_list:
        total_records = sum(m.record_count for m in batch_metrics_list)
        total_bytes = sum(m.batch_bytes for m in batch_metrics_list)
        min_send_time = min(m.send_time_ms for m in batch_metrics_list)
        max_ack_time = max(m.ack_recv_time_ms for m in batch_metrics_list)
        total_time_sec = (max_ack_time - min_send_time) / 1000.0

        if total_time_sec > 0:
            print(f"Total throughput: {total_records // total_time_sec} ops/s")
            print(f"Total throughput: {total_bytes // 1024 // total_time_sec} kb/s")
    else:
        print("No metrics recorded.") 


def loop(dbclient: DbClient, verbose: bool = False):
    """Interactive CLI loop."""
    print("\n=== Raft DB CLI ===")
    print("Commands:")
    print("  GENERATE <amount> [distribution] - Generate and insert records (dist: sequential/uniform/zipfian)")
    print("  INSERT <key_id> <timestamp> <content> - Insert a single record")
    print("  QUERY <min_id> <min_ts> <max_id> <max_ts> - Query records by key range")
    print("  FLUSH - Flush all buffered writes")
    print("  EXIT - Disconnect and exit")
    print()
    
    while True:
        try:
            user_input = input("> ").strip()
            if not user_input:
                continue
            
            parts = user_input.split()
            cmd = parts[0].upper()
            args = parts[1:]
            
            if cmd == "GENERATE":
                if len(args) < 1:
                    print("Usage: GENERATE <amount> [distribution]")
                    continue

                amount = int(args[0])
                data_dist = args[1] if len(args) > 1 else "sequential"
                records = bulk_records(amount, data_dist, int(time.time() * 1000))
                print(f"Sending {len(records)} records to the database")
                write_sync(dbclient, records, verbose)
            elif cmd == "INSERT":
                if len(args) < 3:
                    print("Usage: INSERT <key_id> <timestamp> <content>")
                    continue
                key_id = int(args[0])
                timestamp = int(args[1])
                content = int(args[2])
                record = Record(key_id=key_id, timestamp=timestamp, content=content)
                dbclient.write([record], verbose=verbose)
                print(f"Queued record: key_id={key_id}, timestamp={timestamp}, content={content}")
                
            elif cmd == "QUERY":
                if len(args) < 4:
                    print("Usage: QUERY <min_id> <min_ts> <max_id> <max_ts>")
                    continue
                min_id = int(args[0])
                min_ts = int(args[1])
                max_id = int(args[2])
                max_ts = int(args[3])
                query = QueryRequest(min_id=min_id, min_ts=min_ts, max_id=max_id, max_ts=max_ts)
                results = dbclient.read(query).result()
                show(results, verbose, max_depth=10)
                
            elif cmd == "FLUSH":
                dbclient.flush()
                print("Flushed all buffered writes")
                
            elif cmd in ("EXIT", "QUIT"):
                print("Disconnecting...")
                dbclient.disconnect()
                print("Goodbye!")
                break
                
            else:
                print(f"Unknown command: {cmd}")
                
        except KeyboardInterrupt:
            print("\nUse EXIT to disconnect and quit")
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Raft DB CLI")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--routing-strategy", 
                        choices=["hash", "round-robin", "leader"], 
                        default="hash", 
                        help="Routing strategy to use")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    # 1. Load Config
    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    clusters = {}
    for cluster_name, nodes_data in config_data.items():
        clusters[cluster_name] = [
            Node(cluster_name=cluster_name, id=n['id'], host=n['host'], port=n['port'])
            for n in nodes_data
        ]
    
    # 2. Setup Router
    strategy_map = {
        "hash": HashRoutingStrategy(),
        "round-robin": RoundRobinRoutingStrategy(),
        "leader": LeaderRoutingStrategy()
    }
    strategy = strategy_map[args.routing_strategy]
    router = Router(strategy)
    router.update_topology(clusters)
    
    # 3. Setup DbClient
    client_config = DbClientConfig(
        thread_pool_size=32,
        batch_size=4096,
        write_timeout=5.0,
        read_timeout=10.0,
        write_cb = write_batch_cb
    )

    db_client = DbClient(client_config, router)
    db_client.connect(clusters)
    
    print(f"Connected to {sum(len(nodes) for nodes in clusters.values())} nodes across {len(clusters)} clusters")
    print(f"Routing strategy: {args.routing_strategy}")
    
    # 4. Start interactive loop
    try:
        loop(db_client, args.verbose)
    except Exception as e:
        print(f"Error: {e}")
        db_client.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()

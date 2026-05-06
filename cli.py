import argparse
import json
import random
import sys
import time
from typing import List, Optional

from client import Node, Record, QueryRequest
from client import DbClient, DbClientConfig
from client import Router, HashRoutingStrategy, RoundRobinRoutingStrategy, LeaderRoutingStrategy

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


def show(results: dict, verbose: bool = False):
    """Display query results."""
    max_str_len = 100

    def node_to_dict(node):
        """Convert Node dataclass to dict for JSON serialization."""
        if hasattr(node, 'cluster_name'):
            return {'cluster_name': node.cluster_name, 'id': node.id, 'host': node.host, 'port': node.port}
        return node
    
    def convert_for_json(obj):
        """Convert objects for JSON serialization."""

        if hasattr(obj, '__dataclass_fields__'):
            return { k: convert_for_json(v) for k, v in obj.__dict__.items() }
        else:
            return obj
   
    if verbose:
        print(json.dumps(convert_for_json(results), indent=2, default=str))
    else:
        if isinstance(results, dict):
            for key, value in results.items():
                if key == "records":
                    continue
                if (isinstance(value, str)) and len(value) > max_str_len:
                    print(f"{key}: {value[:max_str_len]}...")
                else:
                    print(f"{key}: {value}")
        else:
            print(results)


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
                dbclient.write(records, verbose=verbose)
                print(f"Queued {len(records)} records for insertion")
                
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
                results = dbclient.read_sync(query, verbose=verbose)
                show(results, verbose)
                
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
        thread_pool_size=16,
        batch_size=4096,
        write_timeout=5.0,
        read_timeout=10.0,
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

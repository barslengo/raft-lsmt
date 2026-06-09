import argparse
import json
import math
import random
import sys
import time
from typing import Iterator

from client import Node, QueryRequest
from client import DbClient, DbClientConfig
from client import Router, HashRoutingStrategy, RoundRobinRoutingStrategy, LeaderRoutingStrategy

from concurrent.futures import wait, FIRST_COMPLETED
import threading

# --- GLOBAL METRICS TRACKER ---
metrics_lock = threading.Lock()
total_queries = 0
total_records_read = 0
total_bytes_read = 0
first_request_time = 0.0
last_ack_time = 0.0

def update_read_metrics(future):
    """Callback di utilità agganciata a ogni Future generato da dbclient.read()"""
    global total_queries, total_records_read, total_bytes_read, first_request_time, last_ack_time
    
    current_time_ms = time.time() * 1000
    
    try:
        results = future.result()
        
        # results e' un dict: { cluster_name: [ { "id:ip:port": (node, response) }, ... ] }
        # Dobbiamo scompattarlo per contare i byte e i record letti.
        batch_records = 0
        batch_bytes = 0
        
        for cluster_results in results.values():
            for node_dict in cluster_results:
                for _, node_tuple in node_dict.items():
                    _, response = node_tuple
                    if response:
                        batch_records += response.get("records_count", 0)
                        batch_bytes += response.get("records_bytes", 0)
        
        with metrics_lock:
            total_queries += 1
            total_records_read += batch_records
            total_bytes_read += batch_bytes
            
            if first_request_time == 0.0:
                first_request_time = current_time_ms
            if current_time_ms > last_ack_time:
                last_ack_time = current_time_ms

    except Exception as e:
        # Se la lettura è andata in timeout o il cluster è giù
        # print(f"[Error] Query failed: {e}")
        pass

def throughput_reporter(stop_event: threading.Event):
    """
    Thread in background che raccoglie metriche per le letture.
    - Scrive nel CSV ogni 1 secondo.
    - Stampa a schermo e forza il flush sul disco ogni 60 secondi.
    """
    prev_queries_1s = 0
    prev_bytes_1s = 0

    prev_queries_60s = 0
    prev_bytes_60s = 0

    seconds_elapsed = 0
    csv_filename = f"read_throughput_{int(time.time())}.csv"

    with open(csv_filename, "w") as f:
        f.write("Timestamp,Total_Queries,Total_Bytes_Read,Total_Records_Read,QPS,MBps\n")

        while not stop_event.wait(1.0):
            seconds_elapsed += 1

            with metrics_lock:
                curr_queries = total_queries
                curr_bytes = total_bytes_read
                curr_records = total_records_read

            delta_qps_1s = curr_queries - prev_queries_1s
            delta_mb_1s = (curr_bytes - prev_bytes_1s) / (1024.0 * 1024.0)

            f.write(f"{int(time.time())},{curr_queries},{curr_bytes},{curr_records},{delta_qps_1s},{delta_mb_1s:.2f}\n")

            prev_queries_1s = curr_queries
            prev_bytes_1s = curr_bytes

            if seconds_elapsed % 60 == 0:
                delta_qps_60s = curr_queries - prev_queries_60s
                delta_mb_60s = (curr_bytes - prev_bytes_60s) / (1024.0 * 1024.0)

                qps_sec_avg = delta_qps_60s / 60.0
                mb_sec_avg = delta_mb_60s / 60.0

                timestamp_str = time.strftime('%H:%M:%S')
                print(f"[{timestamp_str}] ⚡ LAST MINUTE AVG: {qps_sec_avg:,.0f} QPS | {mb_sec_avg:.2f} MB/s (Total Queries: {curr_queries:,})")
                f.flush()

                prev_queries_60s = curr_queries
                prev_bytes_60s = curr_bytes


class ZipfGenerator:
    """Highly optimized Zipfian generator with O(1) time and memory complexity."""
    def __init__(self, n: int, alpha: float = 1.0):
        self.n = max(1, n)
        self.alpha = alpha
        self.is_alpha_one = abs(alpha - 1.0) < 1e-9
        
        if self.is_alpha_one:
            self.c = math.log(self.n)
        else:
            self.c = (self.n ** (1.0 - alpha) - 1.0) / (1.0 - alpha)

    def next(self) -> int:
        u = random.random()
        if self.is_alpha_one:
            val = math.exp(u * self.c)
        else:
            val = (u * self.c * (1.0 - self.alpha) + 1.0) ** (1.0 / (1.0 - self.alpha))
        return min(self.n, max(1, int(val)))


def query_stream(amount: int, data_distribution: str, max_key: int, range_size: int) -> Iterator[QueryRequest]:
    """Generate a stream of QueryRequest objects according to the specified distribution."""
    print(f"Initializing {amount} queries stream (Data Dist: {data_distribution}, Max Key: {max_key}, Range Size: {range_size})...")
    
    max_ts = (2**64) - 1

    if data_distribution == 'sequential':
        for i in range(amount):
            start_id = (i * range_size) % max_key
            if start_id == 0: start_id = 1
            yield QueryRequest(min_id=start_id, min_ts=0, max_id=start_id + range_size - 1, max_ts=max_ts)
            
    elif data_distribution == 'uniform':
        for _ in range(amount):
            start_id = random.randint(1, max_key)
            yield QueryRequest(min_id=start_id, min_ts=0, max_id=start_id + range_size - 1, max_ts=max_ts)
            
    elif data_distribution == 'zipfian':
        zipf = ZipfGenerator(max_key)
        for _ in range(amount):
            start_id = zipf.next()
            yield QueryRequest(min_id=start_id, min_ts=0, max_id=start_id + range_size - 1, max_ts=max_ts)
    else:
        raise ValueError(f"Unknown distribution: {data_distribution}")


def bench(dbclient: DbClient, data_dist: str, requests: int, max_key: int, range_size: int):
    """
    Query benchmark loop. Uses the client's internal ThreadPoolExecutor.
    """
    futures = []
    max_pending_futures = 512  # Backpressure per non saturare la RAM
    
    stop_event = threading.Event()
    reporter_thread = threading.Thread(target=throughput_reporter, args=(stop_event,), daemon=True)
    reporter_thread.start()

    stream = query_stream(requests, data_dist, max_key, range_size)
    total_processed = 0
    
    for query in stream:
        total_processed += 1

        if total_processed % 100_000 == 0:
            timestamp = time.strftime('%H:%M:%S')
            print(f"[{timestamp}] ⏳ Progress: {total_processed:,} queries fired...")

        # Invia la richiesta al client. dbclient.read() restituisce un Future.
        f = dbclient.read(query)
        # Agganciamo la nostra callback per contare i risultati appena il thread finisce
        f.add_done_callback(update_read_metrics)
        futures.append(f)
        
        # Applichiamo Backpressure in stile "write-bench"
        if len(futures) > max_pending_futures:
            done, not_done = wait(futures, return_when=FIRST_COMPLETED)
            futures = list(not_done)
    
    print("All queries generated. Waiting for remaining queries to finish...")
    
    # Aspetta i future finali
    if len(futures) > 0:
        wait(futures)

    stop_event.set()
    reporter_thread.join()

    global total_queries, total_records_read, total_bytes_read, first_request_time, last_ack_time

    total_time_sec = (last_ack_time - first_request_time) / 1000.0

    print("\n" + "="*50)
    print("🏁 READ BENCHMARK COMPLETED")
    print("="*50)
    if total_time_sec > 0:
        print(f"Total time elapsed:  {total_time_sec:.2f} s")
        print(f"Total Queries:       {total_queries:,}")
        print(f"Total Records read:  {total_records_read:,}")
        print(f"Overall QPS:         {total_queries / total_time_sec:,.2f} qps")
        print(f"Overall Bandwidth:   {total_bytes_read / 1024 / 1024 / total_time_sec:.2f} MB/s")
    else:
        print("No metrics recorded or time was too short.")

    dbclient.disconnect()

def main():
    parser = argparse.ArgumentParser(description="Raft DB READ benchmark")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--requests", type=int, default=1000000, help="Number of read queries to execute")
    parser.add_argument("--routing-strategy", 
                        choices=["hash", "round-robin", "leader"], 
                        default="hash", 
                        help="Routing strategy to use")
    parser.add_argument("--data-dist", 
                        choices=["sequential", "uniform", "zipfian"], 
                        default="uniform", 
                        help="Query key distribution strategy")
    parser.add_argument("--max-key", type=int, default=5000000, help="The maximum key ID existing in the database")
    parser.add_argument("--range-size", type=int, default=10, help="How many keys to fetch per query (Range Size)")
    
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    clusters = {}
    for cluster_name, nodes_data in config_data.items():
        clusters[cluster_name] = [
            Node(cluster_name=cluster_name, id=n['id'], host=n['host'], port=n['tcp_port'])
            for n in nodes_data
        ]
    
    strategy_map = {
        "hash": HashRoutingStrategy(),
        "round-robin": RoundRobinRoutingStrategy(),
        "leader": LeaderRoutingStrategy()
    }
    strategy = strategy_map[args.routing_strategy]
    router = Router(strategy)
    router.update_topology(clusters)
    
    client_config = DbClientConfig(
        thread_pool_size=16,
        batch_size=4096,
        write_timeout=5.0,
        read_timeout=15.0,
    )

    db_client = DbClient(client_config, router)
    db_client.connect(clusters)
    
    print(f"Connected to {sum(len(nodes) for nodes in clusters.values())} nodes across {len(clusters)} clusters")
    print(f"Routing strategy: {args.routing_strategy}")
    
    try:
        bench(db_client, args.data_dist, args.requests, args.max_key, args.range_size)
    except Exception as e:
        print(f"Error: {e}")
        db_client.disconnect()
        sys.exit(1)

if __name__ == "__main__":
    main()

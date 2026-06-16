import argparse
import json
import math
import os
import random
import sys
import time
from typing import Iterator

from client import Node, Record, BatchMetrics
from client import DbClient, DbClientConfig
from client import Router, HashRoutingStrategy, RoundRobinRoutingStrategy, RangeRoutingStrategy

from concurrent.futures import wait, FIRST_COMPLETED
import threading

metrics_lock = threading.Lock()
total_acked_records = 0
total_acked_bytes = 0
first_request_time = 0.0
last_ack_time = 0.0
batch_metrics_list = []

def write_batch_cb(m: BatchMetrics) -> None:
    global total_acked_records, total_acked_bytes, first_request_time, last_ack_time
    with metrics_lock:
        total_acked_records += m.record_count
        total_acked_bytes += m.batch_bytes
        
        # Traccia il tempo per il riassunto finale
        if first_request_time == 0.0 or m.send_time_ms < first_request_time:
            first_request_time = m.send_time_ms
        if m.ack_recv_time_ms > last_ack_time:
            last_ack_time = m.ack_recv_time_ms

def throughput_reporter(stop_event: threading.Event):
    """
    Thread in background che raccoglie metriche.
    - Scrive nel CSV ogni 1 secondo: salva sia i TOTALI (per robustezza), sia i DELTA (per comodità visiva).
    - Stampa a schermo e forza il flush sul disco ogni 60 secondi.
    """
    prev_records_1s = 0
    prev_bytes_1s = 0

    prev_records_60s = 0
    prev_bytes_60s = 0

    seconds_elapsed = 0

    csv_filename = f"client_throughput_{int(time.time())}_{os.getpid()}.csv"

    with open(csv_filename, "w") as f:
        f.write("Timestamp,Total_ACKed_Records,Total_ACKed_Bytes,OPS,MBps\n")

        while not stop_event.wait(1.0):
            seconds_elapsed += 1

            with metrics_lock:
                curr_records = total_acked_records
                curr_bytes = total_acked_bytes

            delta_ops_1s = curr_records - prev_records_1s
            delta_mb_1s = (curr_bytes - prev_bytes_1s) / (1024.0 * 1024.0)

            f.write(f"{int(time.time())},{curr_records},{curr_bytes},{delta_ops_1s},{delta_mb_1s:.2f}\n")

            prev_records_1s = curr_records
            prev_bytes_1s = curr_bytes

            if seconds_elapsed % 60 == 0:
                delta_ops_60s = curr_records - prev_records_60s
                delta_mb_60s = (curr_bytes - prev_bytes_60s) / (1024.0 * 1024.0)

                ops_sec_avg = delta_ops_60s / 60.0
                mb_sec_avg = delta_mb_60s / 60.0

                timestamp_str = time.strftime('%H:%M:%S')
                print(f"[{timestamp_str}] LAST MINUTE AVG: {ops_sec_avg:,.0f} OPS | {mb_sec_avg:.2f} MB/s (Total Inserted: {curr_records:,})")

                f.flush()

                prev_records_60s = curr_records
                prev_bytes_60s = curr_bytes

        # Write final remaining records if any, or if no data row has been written yet
        with metrics_lock:
            curr_records = total_acked_records
            curr_bytes = total_acked_bytes

        if curr_records > prev_records_1s or seconds_elapsed == 0:
            delta_ops_1s = curr_records - prev_records_1s
            delta_mb_1s = (curr_bytes - prev_bytes_1s) / (1024.0 * 1024.0)
            f.write(f"{int(time.time())},{curr_records},{curr_bytes},{delta_ops_1s},{delta_mb_1s:.2f}\n")
            f.flush()


class ZipfGenerator:
    """
    Highly optimized Zipfian generator with O(1) time and memory complexity.
    Uses continuous mathematical inverse-CDF mapping to support arbitrary ranges (up to trillions).
    """
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


def record_stream(amount: int, data_distribution: str, key_start: int, key_end: int, base_timestamp: int = 0) -> Iterator[Record]:
    """Generate a stream of Record objects according to the specified distribution."""
    print(f"Initializing {amount} records stream (Data Dist: {data_distribution}, Key Range: [{key_start}, {key_end}])...")
    
    current_ts = base_timestamp
    
    if data_distribution == 'sequential':
        for key_id in range(key_start, key_start + amount):
            yield Record(key_id=key_id, timestamp=current_ts, content=key_id)
            current_ts += 100
    elif data_distribution == 'uniform':
        # Generating a unique shuffled space consumes O(N) memory, causing crash/OOM for large values.
        # Instead, we generate keys uniformly at random within the keyspace range, which takes O(1) memory.
        for _ in range(amount):
            key_id = random.randint(key_start, key_end)
            yield Record(key_id=key_id, timestamp=current_ts, content=key_id)
            current_ts += 100
    elif data_distribution == 'zipfian':
        zipf_range = key_end - key_start + 1
        zipf = ZipfGenerator(zipf_range)
        for _ in range(amount):
            key_id = key_start + zipf.next() - 1
            yield Record(key_id=key_id, timestamp=current_ts, content=key_id)
            current_ts += 100
    else:
        raise ValueError(f"Unknown distribution: {data_distribution}")


def bench(dbclient: DbClient, data_dist: str, requests: int, key_start: int, key_end: int, batch_jitter: float = 0.0, worker_id: int = 0):
    """
    Consumer/producer system implementation:
    Producer (record_stream) yields new records one by one.
    Consumer adds them to a small local chunk buffer before submitting to DbClient.
    """
    futures = []
    batch_buffer = []
    buffer_size = 8192

    stop_event = threading.Event()
    reporter_thread = threading.Thread(target=throughput_reporter, args=(stop_event,), daemon=True)
    reporter_thread.start()

    base_ts = int(time.time() * 1000) * 100 + (worker_id % 100)
    stream = record_stream(requests, data_dist, key_start, key_end, base_ts)
    total_processed = 0
    
    for record in stream:
        batch_buffer.append(record)
        total_processed += 1

        if total_processed % 1_000_000 == 0:
            timestamp = time.strftime('%H:%M:%S')
            print(f"[{timestamp}] Progress: {total_processed:,} records queued and flying...")

        # Dispatch small batches to the client buffer 
        if len(batch_buffer) >= buffer_size:
            fs = dbclient.write(batch_buffer)
            if fs:
                futures.extend(fs)
                if batch_jitter > 0.0:
                    time.sleep(random.uniform(0.0, batch_jitter))
            batch_buffer = []
            
            # Throttle if there are too many in-flight request futures
            if len(futures) > 256:
                print("Throttling ...")
                done, not_done = wait(futures, return_when=FIRST_COMPLETED)
                for f in done:
                    try:
                        f.result()
                    except Exception as e:
                        print(f"[Error] Batch failed: {e}")
                futures = list(not_done)
    
    # Send any remaining records
    if batch_buffer:
        fs = dbclient.write(batch_buffer)
        if fs:
            futures.extend(fs)
            if batch_jitter > 0.0:
                time.sleep(random.uniform(0.0, batch_jitter))
            
    # Flush remaining buffers from internal DbClient write queue
    dbclient.flush()
    
    # Wait for all remaining futures to complete
    if len(futures) > 0:
        done, _ = wait(futures)
        for f in done:
            try:
                f.result()
            except Exception as e:
                print(f"[Error] Trailing batch failed: {e}")

    stop_event.set()
    reporter_thread.join()

    global total_acked_records, total_acked_bytes, first_request_time, last_ack_time

    total_time_sec = (last_ack_time - first_request_time) / 1000.0

    print("\n" + "="*50)
    print("BENCHMARK COMPLETED")
    print("="*50)
    if total_time_sec > 0:
        print(f"Total time elapsed: {total_time_sec:.2f} s")
        print(f"Total ACKed records: {total_acked_records:,}")
        print(f"Overall Throughput: {total_acked_records / total_time_sec:,.2f} ops/s")
        print(f"Overall Bandwidth:  {total_acked_bytes / 1024 / 1024 / total_time_sec:.2f} MB/s")
    else:
        print("No metrics recorded or time was too short.")

    dbclient.disconnect()

def main():
    parser = argparse.ArgumentParser(description="Raft DB write benchmark")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--requests", type=int, default=5000000, help="Number of write requests to send")
    parser.add_argument("--routing-strategy", 
                        choices=["hash", "round-robin", "range"], 
                        default="hash", 
                        help="Routing strategy to use")
    parser.add_argument("--data-dist", 
                        choices=["sequential", "uniform", "zipfian"], 
                        default="uniform", 
                        help="Data distribution strategy")
    parser.add_argument("--key-start", type=int, default=1, help="Start of the key range (inclusive)")
    parser.add_argument("--key-end", type=int, default=None, help="End of the key range (inclusive)")
    parser.add_argument("--max-key", type=int, default=None, help="The maximum key ID in the global keyspace (for range routing)")
    parser.add_argument("--worker-id", type=int, default=0, help="Unique worker process ID to avoid timestamp collisions")
    parser.add_argument("--batch-jitter", type=float, default=0.0, help="Max random jitter sleep (in seconds) after sending each batch")
    args = parser.parse_args()

    if args.key_end is None:
        args.key_end = args.key_start + args.requests - 1

    if args.max_key is None:
        args.max_key = args.key_end

    # 1. Load Config
    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    clusters = {}
    for cluster_name, nodes_data in config_data.items():
        clusters[cluster_name] = [
            Node(cluster_name=cluster_name, id=n['id'], host=n['host'], port=n['tcp_port'])
            for n in nodes_data
        ]
    
    # 2. Setup Router
    strategy_map = {
        "hash": HashRoutingStrategy(),
        "round-robin": RoundRobinRoutingStrategy(),
        "range": RangeRoutingStrategy(max_keyspace=args.max_key)
    }
    strategy = strategy_map[args.routing_strategy]
    router = Router(strategy)
    router.update_topology(clusters)
    
    # 3. Setup DbClient
    client_config = DbClientConfig(
        thread_pool_size=3,
        batch_size=8192,
        write_timeout=10.0,
        read_timeout=10.0,
        write_cb=write_batch_cb
    )

    db_client = DbClient(client_config, router)
    db_client.connect(clusters)
    
    print(f"Connected to {sum(len(nodes) for nodes in clusters.values())} nodes across {len(clusters)} clusters")
    print(f"Routing strategy: {args.routing_strategy}")
    
    try:
        bench(db_client, args.data_dist, args.requests, args.key_start, args.key_end, args.batch_jitter, args.worker_id)
    except Exception as e:
        print(f"Error: {e}")
        db_client.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()

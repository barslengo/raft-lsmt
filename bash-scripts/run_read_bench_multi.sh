#!/bin/bash

# Default values
CONFIG_FILE="servers.json"
TOTAL_REQUESTS=1000000
ROUTING_STRATEGY="hash"
DATA_DIST="uniform"
MAX_KEY=5000000
RANGE_SIZE=10
THREAD_POOL_SIZE=2
BATCH_SIZE=32
KEY_START_BASE=1
GLOBAL_KEYSPACE="false"
BATCH_JITTER=0.0

# Auto-detect cores
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
WORKERS=$CORES

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -w, --workers   <num>    Number of worker processes (default: $CORES)"
    echo "  -r, --requests  <num>    Total number of read requests to distribute (default: 1000000)"
    echo "  -c, --config    <path>   Path to cluster configuration file (default: servers.json)"
    echo "  -s, --strategy  <strat>  Routing strategy: hash, round-robin, leader (default: hash)"
    echo "  -d, --dist      <dist>   Query key distribution: sequential, uniform, zipfian (default: uniform)"
    echo "  -k, --max-key   <num>    Maximum key ID existing in the database (default: 5000000)"
    echo "  -g, --range     <num>    How many keys to fetch per query (default: 10)"
    echo "  -t, --threads   <num>    Thread pool size for concurrent requests per worker (default: 16)"
    echo "  -b, --batch     <num>    Query request batch size per worker (default: 32)"
    echo "  -j, --jitter    <sec>    Max random batch jitter sleep in seconds (default: 0.0)"
    echo "  --key-start     <num>    Start of the key range (default: 1)"
    echo "  --global-keyspace        All workers share the same global key range (default: false)"
    echo "  -h, --help               Show this help message"
    echo ""
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--config)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            CONFIG_FILE="$2"
            shift 2
            ;;
        -r|--requests)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            TOTAL_REQUESTS="$2"
            shift 2
            ;;
        -w|--workers)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            WORKERS="$2"
            shift 2
            ;;
        -s|--strategy)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            ROUTING_STRATEGY="$2"
            shift 2
            ;;
        -d|--dist)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            DATA_DIST="$2"
            shift 2
            ;;
        -k|--max-key)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            MAX_KEY="$2"
            shift 2
            ;;
        -g|--range)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            RANGE_SIZE="$2"
            shift 2
            ;;
        -t|--threads)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            THREAD_POOL_SIZE="$2"
            shift 2
            ;;
        -b|--batch)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            BATCH_SIZE="$2"
            shift 2
            ;;
        --key-start)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            KEY_START_BASE="$2"
            shift 2
            ;;
        --global-keyspace)
            GLOBAL_KEYSPACE="true"
            shift 1
            ;;
        -j|--jitter)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            BATCH_JITTER="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Error: Unknown parameter: $1"
            show_help
            exit 1
            ;;
    esac
done

echo "=========================================================="
echo "🚀 Starting Multi-Process Read Benchmark"
echo "Configuration        : $CONFIG_FILE"
echo "Total Requests       : $TOTAL_REQUESTS"
echo "Workers (Processes)  : $WORKERS"
echo "Routing Strategy     : $ROUTING_STRATEGY"
echo "Data Distribution    : $DATA_DIST"
echo "Max Key              : $MAX_KEY"
echo "Range Size           : $RANGE_SIZE"
echo "Thread Pool Size     : $THREAD_POOL_SIZE"
echo "Batch Size           : $BATCH_SIZE"
echo "Key Start Base       : $KEY_START_BASE"
echo "Global Keyspace      : $GLOBAL_KEYSPACE"
echo "Batch Jitter         : $BATCH_JITTER"
echo "=========================================================="

# Compute requests and key range per worker
REQS_PER_WORKER=$((TOTAL_REQUESTS / WORKERS))
REMAINDER_REQS=$((TOTAL_REQUESTS % WORKERS))

KEY_RANGE_PER_WORKER=$((MAX_KEY / WORKERS))
REMAINDER_KEYS=$((MAX_KEY % WORKERS))

pids=()

# Clean up background jobs on exit/interrupt
trap 'echo -e "\n🛑 Terminating all workers..."; kill "${pids[@]}" 2>/dev/null; exit 1' SIGINT SIGTERM

for i in $(seq 0 $((WORKERS - 1))); do
    # Calculate requests for this worker
    WORKER_REQS=$REQS_PER_WORKER
    if [ $i -eq $((WORKERS - 1)) ]; then
        WORKER_REQS=$((REQS_PER_WORKER + REMAINDER_REQS))
    fi
    
    # Calculate key range
    if [ "$GLOBAL_KEYSPACE" = "true" ]; then
        KEY_START=$KEY_START_BASE
        KEY_END=$MAX_KEY
    else
        KEY_START=$((KEY_START_BASE + i * KEY_RANGE_PER_WORKER))
        KEY_END=$((KEY_START + KEY_RANGE_PER_WORKER - 1))
        if [ $i -eq $((WORKERS - 1)) ]; then
            KEY_END=$((KEY_END + REMAINDER_KEYS))
        fi
    fi
    
    echo "▶️ Launching Worker $i with range [$KEY_START, $KEY_END] ($WORKER_REQS requests)..."
    
    # Run python read_benchmark.py in background
    if [ -f "read_benchmark.py" ]; then
        PYTHON_SCRIPT="read_benchmark.py"
    else
        PYTHON_SCRIPT="../read_benchmark.py"
    fi
    
    python3 "$PYTHON_SCRIPT" \
        --config "$CONFIG_FILE" \
        --requests "$WORKER_REQS" \
        --routing-strategy "$ROUTING_STRATEGY" \
        --data-dist "$DATA_DIST" \
        --max-key "$MAX_KEY" \
        --range-size "$RANGE_SIZE" \
        --thread-pool-size "$THREAD_POOL_SIZE" \
        --batch-size "$BATCH_SIZE" \
        --key-start "$KEY_START" \
        --key-end "$KEY_END" \
        --batch-jitter "$BATCH_JITTER" &
        
    pids+=($!)
done

echo "----------------------------------------------------------"
echo "All workers launched. Waiting for completion..."
echo "----------------------------------------------------------"

# Wait for all background processes to complete and check their exit status
exit_code=0
for pid in "${pids[@]}"; do
    wait "$pid"
    status=$?
    if [ $status -ne 0 ]; then
        echo "❌ Worker process $pid failed with exit code $status"
        exit_code=$status
    fi
done

if [ $exit_code -eq 0 ]; then
    echo "=========================================================="
    echo "🎉 All benchmark processes completed successfully!"
    echo "=========================================================="
    
    # Wait for a few seconds to let any pending CSV writes flush
    echo "⏳ Waiting 3 seconds for files to flush..."
    sleep 3
    
    # Find all CSV files generated by our workers
    csv_files=()
    for pid in "${pids[@]}"; do
        for f in read_throughput_*_"${pid}".csv; do
            if [ -f "$f" ]; then
                csv_files+=("$f")
            fi
        done
    done
    
    if [ ${#csv_files[@]} -gt 0 ]; then
        echo "📊 Merging throughput data from ${#csv_files[@]} workers..."
        python3 - "${csv_files[@]}" << 'EOF'
import sys
import os
import time
from collections import defaultdict

files = sys.argv[1:]
data_by_time = defaultdict(list)
all_timestamps = set()

for fpath in files:
    if not os.path.exists(fpath):
        continue
    with open(fpath, 'r') as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) < 8:
                continue
            try:
                ts = int(parts[0])
                total_rec = int(parts[1])
                total_bytes = int(parts[2])
                qps_cum = int(parts[3])
                mb_cum = float(parts[4])
                avg_l = float(parts[5])
                p50_l = float(parts[6])
                p95_l = float(parts[7])
                data_by_time[ts].append((fpath, total_rec, total_bytes, qps_cum, mb_cum, avg_l, p50_l, p95_l))
                all_timestamps.add(ts)
            except ValueError:
                continue

if not all_timestamps:
    print("No valid throughput metrics found to merge.")
    sys.exit(0)

sorted_ts = sorted(list(all_timestamps))
latest_by_file_rec = {}
latest_by_file_bytes = {}
latest_by_file_qps = {}
latest_by_file_mb = {}
merged_rows = []

for ts in sorted_ts:
    for fpath, total_rec, total_bytes, qps_cum, mb_cum, avg_l, p50_l, p95_l in data_by_time[ts]:
        latest_by_file_rec[fpath] = total_rec
        latest_by_file_bytes[fpath] = total_bytes
        latest_by_file_qps[fpath] = qps_cum
        latest_by_file_mb[fpath] = mb_cum
        
    sum_total_rec = sum(latest_by_file_rec.values())
    sum_total_bytes = sum(latest_by_file_bytes.values())
    sum_qps = sum(latest_by_file_qps.values())
    sum_mb = sum(latest_by_file_mb.values())
    
    # Average latency over workers reporting at this ts
    active_rows = data_by_time[ts]
    avg_l_val = sum(r[5] for r in active_rows) / len(active_rows)
    p50_l_val = sum(r[6] for r in active_rows) / len(active_rows)
    p95_l_val = sum(r[7] for r in active_rows) / len(active_rows)
    
    merged_rows.append((ts, sum_total_rec, sum_total_bytes, sum_qps, sum_mb, avg_l_val, p50_l_val, p95_l_val))

merged_filename = f"read_throughput_merged_{int(time.time())}.csv"
with open(merged_filename, 'w') as f:
    f.write("Timestamp,Total_ACKed_Records,Total_ACKed_Bytes,QPS,MBps,Avg_Latency_ms,P50_Latency_ms,P95_Latency_ms\n")
    for row in merged_rows:
        f.write(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]:.2f},{row[5]:.2f},{row[6]:.2f},{row[7]:.2f}\n")

print(f"✅ Merged throughput CSV generated: {merged_filename}")
EOF
    else
        echo "Warning: No worker throughput CSV files were found to merge."
    fi
else
    echo "=========================================================="
    echo "⚠️ Benchmark completed with errors!"
    echo "=========================================================="
    exit $exit_code
fi

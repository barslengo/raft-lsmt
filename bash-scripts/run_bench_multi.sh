#!/bin/bash

# Auto-detect cores
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Default values
CONFIG_FILE="servers.json"
TOTAL_REQUESTS=5000000
ROUTING_STRATEGY="hash"
DATA_DIST="uniform"
WORKERS=$CORES
KEY_START_BASE=1
BATCH_JITTER=0.0

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -w, --workers   <num>    Number of worker processes (default: $CORES)"
    echo "  -r, --requests  <num>    Total number of write requests to distribute (default: 5000000)"
    echo "  -c, --config    <path>   Path to cluster configuration file (default: servers.json)"
    echo "  -s, --strategy  <strat>  Routing strategy: hash, round-robin, leader (default: hash)"
    echo "  -d, --dist      <dist>   Data distribution: sequential, uniform, zipfian (default: uniform)"
    echo "  -k, --key-start <num>    Start of the key range (default: 1)"
    echo "  -j, --jitter    <sec>    Max random batch jitter sleep in seconds (default: 0.0)"
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
        -k|--key-start)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            KEY_START_BASE="$2"
            shift 2
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
echo "🚀 Starting Multi-Process Write Benchmark"
echo "Configuration        : $CONFIG_FILE"
echo "Total Requests       : $TOTAL_REQUESTS"
echo "Workers (Processes)  : $WORKERS"
echo "Routing Strategy     : $ROUTING_STRATEGY"
echo "Data Distribution    : $DATA_DIST"
echo "Key Start Base       : $KEY_START_BASE"
echo "Batch Jitter         : $BATCH_JITTER"
echo "=========================================================="

# Compute requests per worker
REQS_PER_WORKER=$((TOTAL_REQUESTS / WORKERS))
REMAINDER=$((TOTAL_REQUESTS % WORKERS))

pids=()

# Clean up background jobs on exit/interrupt
trap 'echo -e "\n🛑 Terminating all workers..."; kill "${pids[@]}" 2>/dev/null; exit 1' SIGINT SIGTERM

for i in $(seq 0 $((WORKERS - 1))); do
    # Calculate requests for this worker
    WORKER_REQS=$REQS_PER_WORKER
    if [ $i -eq $((WORKERS - 1)) ]; then
        WORKER_REQS=$((REQS_PER_WORKER + REMAINDER))
    fi
    
    # Calculate disjoint key range
    KEY_START=$((KEY_START_BASE + i * REQS_PER_WORKER))
    KEY_END=$((KEY_START + WORKER_REQS - 1))
    
    echo "▶️ Launching Worker $i with range [$KEY_START, $KEY_END] ($WORKER_REQS requests)..."
    
    # Run python write-bench.py in background
    if [ -f "write-bench.py" ]; then
        PYTHON_SCRIPT="write-bench.py"
    else
        PYTHON_SCRIPT="../write-bench.py"
    fi
    
    python3 "$PYTHON_SCRIPT" \
        --config "$CONFIG_FILE" \
        --requests "$WORKER_REQS" \
        --routing-strategy "$ROUTING_STRATEGY" \
        --data-dist "$DATA_DIST" \
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
        # Use glob to match files containing the pid in the current directory
        for f in client_throughput_*_"${pid}".csv; do
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
            if len(parts) < 5:
                continue
            try:
                ts = int(parts[0])
                total_rec = int(parts[1])
                total_bytes = int(parts[2])
                ops = int(parts[3])
                mbps = float(parts[4])
                data_by_time[ts].append((fpath, total_rec, total_bytes, ops, mbps))
                all_timestamps.add(ts)
            except ValueError:
                continue

if not all_timestamps:
    print("No valid throughput metrics found to merge.")
    sys.exit(0)

sorted_ts = sorted(list(all_timestamps))
latest_by_file_rec = {}
latest_by_file_bytes = {}
merged_rows = []

for ts in sorted_ts:
    for fpath, total_rec, total_bytes, ops, mbps in data_by_time[ts]:
        latest_by_file_rec[fpath] = total_rec
        latest_by_file_bytes[fpath] = total_bytes
        
    sum_total_rec = sum(latest_by_file_rec.values())
    sum_total_bytes = sum(latest_by_file_bytes.values())
    sum_ops = sum(row[3] for row in data_by_time[ts])
    sum_mbps = sum(row[4] for row in data_by_time[ts])
    
    merged_rows.append((ts, sum_total_rec, sum_total_bytes, sum_ops, sum_mbps))

merged_filename = f"client_throughput_merged_{int(time.time())}.csv"
with open(merged_filename, 'w') as f:
    f.write("Timestamp,Total_ACKed_Records,Total_ACKed_Bytes,OPS,MBps\n")
    for row in merged_rows:
        f.write(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]:.2f}\n")

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

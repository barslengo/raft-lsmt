#!/bin/bash
set -e

# --- Argument Parsing ---
SOURCE_DIR=""
CLUSTER_NAME=""
NODE_ID=""

while getopts "s:c:n:" opt; do
    case $opt in
        s) SOURCE_DIR="$OPTARG" ;;
        c) CLUSTER_NAME="$OPTARG" ;;
        n) NODE_ID="$OPTARG" ;;
        *) echo "Usage: $0 -s <source_dir> [-c <cluster_name>] [-n <node_id>]"; exit 1 ;;
    esac
done

if [ -z "$SOURCE_DIR" ]; then
    echo "Error: Source directory (-s) is required."
    echo "Usage: $0 -s <source_dir> [-c <cluster_name>] [-n <node_id>]"
    exit 1
fi

# Resolve source directory to absolute path
SOURCE_DIR=$(realpath "$SOURCE_DIR")
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# --- Helper Function: Stop a Single Node ---
stop_single_node() {
    local cluster="$1"
    local node="$2"
    local pid_file="$SOURCE_DIR/$cluster/$node/.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        echo "=== Stopping node $node of cluster $cluster (PID: $pid) ==="
        
        if kill "$pid" 2>/dev/null; then
            # Wait up to 3 seconds for the process to terminate
            local count=0
            local stopped=false
            while [ "$count" -lt 30 ]; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    stopped=true
                    break
                fi
                sleep 0.1
                count=$((count + 1))
            done

            if [ "$stopped" = false ]; then
                echo "  Warning: Node $node did not terminate within 3 seconds, forcing with SIGKILL..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            echo "  Node $node stopped."
        else
            echo "  Warning: Process with PID $pid not found. It might have crashed or stopped on its own."
        fi
        # Remove the PID file after termination
        rm -f "$pid_file"
    else
        echo "Warning: PID file not found for node $node of cluster $cluster ($pid_file). Is it running?"
    fi
}

# --- Main Dispatch Logic ---

# Case 1: Both CLUSTER_NAME and NODE_ID are specified
if [ -n "$CLUSTER_NAME" ] && [ -n "$NODE_ID" ]; then
    stop_single_node "$CLUSTER_NAME" "$NODE_ID"

# Case 2: Only NODE_ID is specified, stop this node ID in all clusters where it's present
elif [ -z "$CLUSTER_NAME" ] && [ -n "$NODE_ID" ]; then
    FOUND=false
    for c_dir in "$SOURCE_DIR"/*; do
        if [ -d "$c_dir" ] && [ -f "$c_dir/cluster.config" ]; then
            c_name=$(basename "$c_dir")
            if [ -d "$c_dir/$NODE_ID" ]; then
                stop_single_node "$c_name" "$NODE_ID"
                FOUND=true
            fi
        fi
    done
    if [ "$FOUND" = false ]; then
        echo "Warning: Node ID '$NODE_ID' data directory not found in any cluster on this machine."
    fi

# Case 3: Only CLUSTER_NAME is specified, stop all local nodes in that cluster
elif [ -n "$CLUSTER_NAME" ] && [ -z "$NODE_ID" ]; then
    CLUSTER_DIR="$SOURCE_DIR/$CLUSTER_NAME"
    if [ ! -d "$CLUSTER_DIR" ]; then
        echo "Error: Cluster directory '$CLUSTER_DIR' not found."
        exit 1
    fi

    LOCAL_NODES=$(find "$CLUSTER_DIR" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' -exec basename {} \; | sort -n)
    if [ -z "$LOCAL_NODES" ]; then
        echo "No local nodes found for cluster '$CLUSTER_NAME' on this machine."
        exit 0
    fi

    for node in $LOCAL_NODES; do
        stop_single_node "$CLUSTER_NAME" "$node"
    done

# Case 4: Both CLUSTER_NAME and NODE_ID are omitted, stop all local nodes in all clusters
else
    echo "=== Stopping all local nodes in all clusters ==="
    for c_dir in "$SOURCE_DIR"/*; do
        if [ -d "$c_dir" ] && [ -f "$c_dir/cluster.config" ]; then
            c_name=$(basename "$c_dir")
            LOCAL_NODES=$(find "$c_dir" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' -exec basename {} \; | sort -n)
            for node in $LOCAL_NODES; do
                stop_single_node "$c_name" "$node"
            done
        fi
    done
fi

echo "=== Stop dispatch complete ==="

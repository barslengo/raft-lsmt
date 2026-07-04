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

# --- Helper Function: Check if Node is Running ---
is_node_running() {
    local cluster="$1"
    local node="$2"
    local pid_file="$SOURCE_DIR/$cluster/$node/.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0 # running
        fi
    fi
    return 1 # not running
}

# --- Helper Function: Run a Single Node ---
run_single_node() {
    local cluster="$1"
    local node="$2"
    local cluster_dir="$SOURCE_DIR/$cluster"
    local node_dir="$cluster_dir/$node"
    local log_file="$cluster_dir/server_${node}.log"

    if [ ! -d "$node_dir" ]; then
        echo "Error: Node directory '$node_dir' does not exist."
        return 1
    fi

    if is_node_running "$cluster" "$node"; then
        local pid=$(cat "$node_dir/.pid")
        echo "Node $node of cluster $cluster is already running (PID: $pid)."
        return 0
    fi

    echo "=== Starting node $node of cluster $cluster ==="
    
    # Run the server in a subshell to avoid changing working directory
    (
        cd "$cluster_dir"
        nohup ./build/server "./$node" "$node" cluster.config > "$log_file" 2>&1 &
        echo $! > "$node/.pid"
    )

    # Let the process initialize and check if it survived
    sleep 0.2
    if is_node_running "$cluster" "$node"; then
        local pid=$(cat "$node_dir/.pid")
        echo "  Node $node started successfully (PID: $pid, log: $cluster_dir/server_${node}.log)"
    else
        echo "  Error: Node $node failed to start or crashed immediately. Logs:"
        if [ -f "$log_file" ]; then
            tail -n 10 "$log_file" | sed 's/^/    /'
        else
            echo "    (Log file not created)"
        fi
        # Clean up stale PID file if any
        rm -f "$node_dir/.pid"
        return 1
    fi
}

# --- Main Dispatch Logic ---

# Case 1: Both CLUSTER_NAME and NODE_ID are specified
if [ -n "$CLUSTER_NAME" ] && [ -n "$NODE_ID" ]; then
    run_single_node "$CLUSTER_NAME" "$NODE_ID"

# Case 2: Only NODE_ID is specified, find which cluster it belongs to
elif [ -z "$CLUSTER_NAME" ] && [ -n "$NODE_ID" ]; then
    FOUND=false
    for c_dir in "$SOURCE_DIR"/*; do
        if [ -d "$c_dir" ] && [ -f "$c_dir/cluster.config" ]; then
            c_name=$(basename "$c_dir")
            if [ -d "$c_dir/$NODE_ID" ]; then
                run_single_node "$c_name" "$NODE_ID"
                FOUND=true
            fi
        fi
    done
    if [ "$FOUND" = false ]; then
        echo "Error: Node ID '$NODE_ID' not found on this machine under any cluster."
        exit 1
    fi

# Case 3: Only CLUSTER_NAME is specified, run all local nodes in that cluster
elif [ -n "$CLUSTER_NAME" ] && [ -z "$NODE_ID" ]; then
    CLUSTER_DIR="$SOURCE_DIR/$CLUSTER_NAME"
    if [ ! -d "$CLUSTER_DIR" ]; then
        echo "Error: Cluster directory '$CLUSTER_DIR' not found."
        exit 1
    fi
    
    # Find all subdirectories that are numeric (local node IDs)
    LOCAL_NODES=$(find "$CLUSTER_DIR" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' -exec basename {} \; | sort -n)
    if [ -z "$LOCAL_NODES" ]; then
        echo "No local nodes found for cluster '$CLUSTER_NAME' on this machine."
        exit 0
    fi

    for node in $LOCAL_NODES; do
        run_single_node "$CLUSTER_NAME" "$node"
    done

# Case 4: Both CLUSTER_NAME and NODE_ID are omitted, run all local nodes in all clusters
else
    echo "=== Starting all local nodes in all clusters ==="
    for c_dir in "$SOURCE_DIR"/*; do
        if [ -d "$c_dir" ] && [ -f "$c_dir/cluster.config" ]; then
            c_name=$(basename "$c_dir")
            LOCAL_NODES=$(find "$c_dir" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' -exec basename {} \; | sort -n)
            for node in $LOCAL_NODES; do
                run_single_node "$c_name" "$node"
            done
        fi
    done
fi

echo "=== Execution dispatch complete ==="

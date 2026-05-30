#!/bin/bash
set -e

# --- Input Validation ---
if [ -z "$1" ]; then
    echo "Error: No arguments provided."
    echo "Usage: $0 [all | <cluster_name_1> <cluster_name_2> ...]"
    echo "Example: $0 all"
    echo "Example: $0 A B C"
    exit 1
fi

# --- Source topology ---
TOPLOGY_ENV="topology.env"
if [ ! -f "$TOPLOGY_ENV" ]; then
    echo "Error: $TOPLOGY_ENV not found. Run setup-clusters.sh first."
    exit 1
fi

source "$TOPLOGY_ENV"

# --- Determine clusters to run ---
if [ "$1" = "all" ]; then
    # Get all CLUSTER_*_NODE_ID variables and extract cluster names
    CLUSTER_NAMES=$(compgen -v | grep '^CLUSTER_' | grep '_NODE_ID$' | sed 's/CLUSTER_\(.*\)_NODE_ID/\1/' | sort)
    if [ -z "$CLUSTER_NAMES" ]; then
        echo "Error: No clusters found in $TOPLOGY_ENV"
        exit 1
    fi
else
    CLUSTER_NAMES="$@"
fi

# --- Run each cluster ---
for CLUSTER_NAME in $CLUSTER_NAMES; do
    echo "=== Starting cluster: $CLUSTER_NAME ==="
    
    if [ ! -d "$CLUSTER_NAME" ]; then
        echo "Warning: Cluster folder '$CLUSTER_NAME' not found. Skipping."
        continue
    fi
    
    # Get the node ID from environment
    UPPER_CLUSTER=$(echo "$CLUSTER_NAME" | tr '[:lower:]' '[:upper:]')
    NODE_ID_VAR="CLUSTER_${UPPER_CLUSTER}_NODE_ID"
    
    if [ -z "${!NODE_ID_VAR}" ]; then
        echo "Warning: Node ID for cluster '$CLUSTER_NAME' not found in $TOPLOGY_ENV. Skipping."
        continue
    fi
    
    NODE_ID="${!NODE_ID_VAR}"
    echo "  Using Node ID: $NODE_ID"
    
    # Start the database node
    cd "$CLUSTER_NAME"
    mkdir -p "$NODE_ID"

    LOG_FILE="server_${NODE_ID}.log"
    nohup ./build/server "./$NODE_ID" "$NODE_ID" cluster.config > "$LOG_FILE" 2>&1 &
    
    # Capture PID and save
    PID=$!
    echo "$PID" > .pid
    echo "  Started with PID: $PID (data_dir: ./$NODE_ID), log: $CLUSTER_NAME/$LOG_FILE)"
    
    cd - > /dev/null
done

echo "=== All clusters started ==="

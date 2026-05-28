#!/bin/bash
set -e

# --- Input Validation ---
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
    echo "Error: Missing arguments."
    echo "Usage: $0 <src_folder> <idx_stride> <idx_node>"
    echo "Example: $0 /path/to/db-src 3 1"
    exit 1
fi

SRC_FOLDER="$1"
IDX_STRIDE="$2"
IDX_NODE="$3"

# --- Step 1: Build ---
echo "=== Building database in $SRC_FOLDER ==="
cd "$SRC_FOLDER"
make clean
make release
echo "=== Build complete ==="

# --- Step 2: Prepare topology.env ---
cd - > /dev/null
TOPLOGY_ENV="topology.env"
> "$TOPLOGY_ENV"
echo "=== Created/cleared $TOPLOGY_ENV ==="

# --- Step 3: Parse and process clusters ---
SERVERS_JSON="$SRC_FOLDER/servers.json"

if [ ! -f "$SERVERS_JSON" ]; then
    echo "Error: servers.json not found at $SERVERS_JSON"
    exit 1
fi

# Get sorted top-level keys
CLUSTER_NAMES=$(jq -r 'keys | @tsv' "$SERVERS_JSON" | tr '\t' '\n' | sort)

LOOP_COUNTER=0

for CLUSTER_NAME in $CLUSTER_NAMES; do
    echo "=== Processing cluster: $CLUSTER_NAME ==="
    
    # Remove and create cluster folder
    if [ -d "$CLUSTER_NAME" ]; then
        rm -rf "$CLUSTER_NAME"
    fi
    mkdir -p "$CLUSTER_NAME"
    
    # Copy build folder
    cp -r "$SRC_FOLDER/build" "$CLUSTER_NAME/"
    
    # Generate cluster.config
    jq -r --arg cluster "$CLUSTER_NAME" '.[$cluster][] | "\(.id) \(.host):\(.raft_port) \(.tcp_port)"' "$SERVERS_JSON" > "$CLUSTER_NAME/cluster.config"
    echo "  Created $CLUSTER_NAME/cluster.config"
    
    # Calculate Node ID
    CALCULATED_ID=$((IDX_NODE + (LOOP_COUNTER * IDX_STRIDE)))
    
    # Append to topology.env (uppercase cluster name)
    UPPER_CLUSTER=$(echo "$CLUSTER_NAME" | tr '[:lower:]' '[:upper:]')
    echo "CLUSTER_${UPPER_CLUSTER}_NODE_ID=$CALCULATED_ID" >> "$TOPLOGY_ENV"
    echo "  Node ID for $CLUSTER_NAME: $CALCULATED_ID"
    
    LOOP_COUNTER=$((LOOP_COUNTER + 1))
done

echo "=== Setup complete ==="
echo "Topology written to $TOPLOGY_ENV"

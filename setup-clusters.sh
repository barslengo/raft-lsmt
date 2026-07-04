#!/bin/bash
set -e

# Resolve script directory to compile and find build files
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# --- Argument Parsing ---
if [ "$#" -lt 3 ]; then
    echo "Error: Missing arguments."
    echo "Usage: $0 <topology_json> -o <output_dir>"
    echo "Example: $0 local-topology.json -o my-clusters"
    exit 1
fi

TOPOLOGY_JSON="$1"
shift

# Parse -o option
OUTPUT_DIR=""
while getopts "o:" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG" ;;
        *) echo "Usage: $0 <topology_json> -o <output_dir>"; exit 1 ;;
    esac
done

if [ -z "$OUTPUT_DIR" ]; then
    echo "Error: Output directory (-o) is required."
    exit 1
fi

# Resolve paths to absolute
TOPOLOGY_JSON=$(realpath "$TOPOLOGY_JSON")
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(realpath "$OUTPUT_DIR")

# --- Step 1: Build the Server ---
echo "=== Building database server ==="
cd "$SCRIPT_DIR"
make clean
make release
echo "=== Build complete ==="

# --- Helper Function: Check if Host is Local ---
is_local_host() {
    local target="$1"

    # Pre-populate list of local identifiers
    local local_idents=("127.0.0.1" "localhost" "::1" "$(hostname)")
    for ip in $(hostname -I 2>/dev/null); do
        local_idents+=("$ip")
    done

    # 1. Direct comparison
    for ident in "${local_idents[@]}"; do
        if [ "$target" = "$ident" ]; then
            return 0
        fi
    done

    # 2. DNS/Name resolution comparison
    local resolved_ips
    resolved_ips=$(getent ahosts "$target" 2>/dev/null | awk '{print $1}' | sort -u)
    for rip in $resolved_ips; do
        # Strip interface scope index like %4 from IPv6 if present
        local clean_rip="${rip%%%*}"
        for ident in "${local_idents[@]}"; do
            if [ "$clean_rip" = "$ident" ]; then
                return 0
            fi
        done
    done

    return 1
}

# --- Step 2: Parse and Process Clusters ---
if [ ! -f "$TOPOLOGY_JSON" ]; then
    echo "Error: Topology JSON not found at $TOPOLOGY_JSON"
    exit 1
fi

# Get sorted cluster names
CLUSTER_NAMES=$(jq -r 'keys[]' "$TOPOLOGY_JSON" | sort)

for CLUSTER_NAME in $CLUSTER_NAMES; do
    # Extract nodes for this cluster
    NODES_JSON=$(jq -c --arg cluster "$CLUSTER_NAME" '.[$cluster][]' "$TOPOLOGY_JSON")

    # Check if this cluster contains any nodes local to this machine
    HAS_LOCAL_NODES=false
    while read -r node_line; do
        if [ -z "$node_line" ]; then continue; fi
        NODE_HOST=$(echo "$node_line" | jq -r '.host')
        if is_local_host "$NODE_HOST"; then
            HAS_LOCAL_NODES=true
            break
        fi
    done <<< "$NODES_JSON"

    if [ "$HAS_LOCAL_NODES" = true ]; then
        echo "=== Setting up cluster: $CLUSTER_NAME (contains local nodes) ==="
        CLUSTER_DIR="$OUTPUT_DIR/$CLUSTER_NAME"
        
        # Recreate cluster folder
        if [ -d "$CLUSTER_DIR" ]; then
            rm -rf "$CLUSTER_DIR"
        fi
        mkdir -p "$CLUSTER_DIR"

        # Copy the build directory containing build/server
        cp -r "$SCRIPT_DIR/build" "$CLUSTER_DIR/"

        # Generate cluster.config containing ALL nodes in this cluster
        jq -r --arg cluster "$CLUSTER_NAME" '.[$cluster][] | "\(.id) \(.host):\(.raft_port) \(.tcp_port)"' "$TOPOLOGY_JSON" > "$CLUSTER_DIR/cluster.config"
        echo "  Created $CLUSTER_DIR/cluster.config"

        # Create data directories for local nodes
        while read -r node_line; do
            if [ -z "$node_line" ]; then continue; fi
            NODE_ID=$(echo "$node_line" | jq -r '.id')
            NODE_HOST=$(echo "$node_line" | jq -r '.host')

            if is_local_host "$NODE_HOST"; then
                mkdir -p "$CLUSTER_DIR/$NODE_ID"
                echo "  Created data directory for local node $NODE_ID: $CLUSTER_DIR/$NODE_ID/"
            fi
        done <<< "$NODES_JSON"
    else
        echo "=== Skipping cluster: $CLUSTER_NAME (no local nodes) ==="
    fi
done

echo "=== Setup complete ==="

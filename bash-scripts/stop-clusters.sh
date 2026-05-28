#!/bin/bash

# --- Input Validation ---
if [ -z "$1" ]; then
    echo "Error: No arguments provided."
    echo "Usage: $0 [all | <cluster_name_1> <cluster_name_2> ...]"
    echo "Example: $0 all"
    echo "Example: $0 A B C"
    exit 1
fi

# --- Determine clusters to stop ---
TOPLOGY_ENV="topology.env"
if [ "$1" = "all" ]; then
    if [ ! -f "$TOPLOGY_ENV" ]; then
        echo "Error: $TOPLOGY_ENV not found. Run setup-clusters.sh first."
        exit 1
    fi
    source "$TOPLOGY_ENV"
    # Get all CLUSTER_*_NODE_ID variables and extract cluster names
    CLUSTER_NAMES=$(compgen -v | grep '^CLUSTER_' | grep '_NODE_ID$' | sed 's/CLUSTER_\(.*\)_NODE_ID/\1/' | sort)
    if [ -z "$CLUSTER_NAMES" ]; then
        echo "Error: No clusters found in $TOPLOGY_ENV"
        exit 1
    fi
else
    CLUSTER_NAMES="$@"
fi

# --- Stop each cluster ---
for CLUSTER_NAME in $CLUSTER_NAMES; do
    PID_FILE="$CLUSTER_NAME/.pid"
    
    if [ ! -f "$PID_FILE" ]; then
        echo "Warning: PID file not found for cluster '$CLUSTER_NAME'. Already stopped?"
        continue
    fi
    
    PID=$(cat "$PID_FILE")
    
    if kill "$PID" 2>/dev/null; then
        echo "Stopped cluster '$CLUSTER_NAME' (PID: $PID)"
        rm -f "$PID_FILE"
    else
        echo "Warning: Failed to kill PID $PID for cluster '$CLUSTER_NAME'. Process may have already stopped."
        rm -f "$PID_FILE"
    fi
done

echo "=== All specified clusters stopped ==="

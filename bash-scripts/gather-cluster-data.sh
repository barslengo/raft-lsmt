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

# --- Generate timestamp ---
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATA_DIR="data-${TIMESTAMP}"

# --- Determine clusters to gather ---
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

# --- Create master directory ---
mkdir -p "$DATA_DIR"

# --- Gather data from each cluster ---
for CLUSTER_NAME in $CLUSTER_NAMES; do
    if [ ! -d "$CLUSTER_NAME" ]; then
        echo "Warning: Cluster folder '$CLUSTER_NAME' not found. Skipping."
        continue
    fi
    
    DEST_DIR="$DATA_DIR/$CLUSTER_NAME"
    mkdir -p "$DEST_DIR"
    
    # Find and copy all CSV files, preserving relative path within cluster folder
    find "$CLUSTER_NAME" -name '*.csv' | while read -r csv_file; do
        # Get relative path from cluster folder
        rel_path="${csv_file#$CLUSTER_NAME/}"
        # Create destination path
        dest="$DEST_DIR/$rel_path"
        mkdir -p "$(dirname "$dest")"
        cp "$csv_file" "$dest"
    done
    
    echo "  Gathered data from cluster '$CLUSTER_NAME'"
done

# --- Print absolute path ---
ABS_PATH=$(cd "$DATA_DIR" && pwd)
echo ""
echo "Data gathered to: $ABS_PATH"

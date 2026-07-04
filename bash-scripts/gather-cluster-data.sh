#!/bin/bash
set -e

# --- Argument Parsing ---
SOURCE_DIR=""
CLUSTER_NAME=""

while getopts "s:c:" opt; do
    case $opt in
        s) SOURCE_DIR="$OPTARG" ;;
        c) CLUSTER_NAME="$OPTARG" ;;
        *) echo "Usage: $0 -s <source_dir> [-c <cluster_name>]"; exit 1 ;;
    esac
done

if [ -z "$SOURCE_DIR" ]; then
    echo "Error: Source directory (-s) is required."
    echo "Usage: $0 -s <source_dir> [-c <cluster_name>]"
    exit 1
fi

# Resolve source directory
SOURCE_DIR=$(realpath "$SOURCE_DIR")
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

# --- Generate timestamp ---
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DATA_DIR="data-${TIMESTAMP}"

# --- Create master directory ---
mkdir -p "$DATA_DIR"

# --- Helper Function: Gather Data for a Cluster ---
gather_cluster_data() {
    local c_name="$1"
    local cluster_path="$SOURCE_DIR/$c_name"
    if [ ! -d "$cluster_path" ]; then
        echo "Warning: Cluster folder '$cluster_path' not found. Skipping."
        return 1
    fi
    
    local dest_dir="$DATA_DIR/$c_name"
    mkdir -p "$dest_dir"
    
    # Find and copy all CSV files and server logs, preserving relative path within cluster folder
    find "$cluster_path" \( -name '*.csv' -o -name 'server_*.log' \) | while read -r src_file; do
        # Get relative path from cluster folder
        local rel_path="${src_file#$cluster_path/}"
        # Create destination path
        local dest="$dest_dir/$rel_path"
        mkdir -p "$(dirname "$dest")"
        cp "$src_file" "$dest"
    done
    
    echo "  Gathered data from cluster '$c_name'"
}

# --- Main Dispatch ---
if [ -n "$CLUSTER_NAME" ]; then
    gather_cluster_data "$CLUSTER_NAME"
else
    # Gather all clusters present in SOURCE_DIR (containing a cluster.config)
    for d in "$SOURCE_DIR"/*; do
        if [ -d "$d" ] && [ -f "$d/cluster.config" ]; then
            gather_cluster_data "$(basename "$d")"
        fi
    done
fi

# --- Print absolute path ---
ABS_PATH=$(cd "$DATA_DIR" && pwd)
echo ""
echo "Data gathered to: $ABS_PATH"

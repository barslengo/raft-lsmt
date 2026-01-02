#!/bin/bash

# --- Configuration (Defaults) ---
SERVER_EXECUTABLE="./build/server"
DEFAULT_DATA_DIR="/tmp/raft"
DEFAULT_CLUSTER_CONF="cluster.conf"

# --- Script Logic ---

# 1. Input Validation: Check if a Server ID was provided.
if [ -z "$1" ]; then
    echo "Error: No Server ID provided."
    echo "Usage: $0 <server_id> [data_dir] [cluster_conf_path]"
    echo "Example (Defaults): $0 1"
    echo "Example (Custom):   $0 1 ./my_data ./my_configs/nodes.conf"
    exit 1
fi

# 2. Parse Arguments (with defaults)
SERVER_ID="$1"
# If $2 is set, use it; otherwise use default
TOP_LEVEL_DIR="${2:-$DEFAULT_DATA_DIR}"
# If $3 is set, use it; otherwise use default
CLUSTER_CONF="${3:-$DEFAULT_CLUSTER_CONF}"

# 3. Prerequisite Check: Verify server executable.
if [ ! -x "$SERVER_EXECUTABLE" ]; then
    echo "Error: Server executable not found at '$SERVER_EXECUTABLE'"
    echo "Please compile the server first."
    exit 1
fi

# 4. Prerequisite Check: Verify config file exists.
if [ ! -f "$CLUSTER_CONF" ]; then
    echo "Error: Cluster configuration file not found at '$CLUSTER_CONF'"
    exit 1
fi

# 5. Directory Setup
SERVER_DIR="$TOP_LEVEL_DIR/$SERVER_ID"

echo "------------------------------------------------"
echo "Starting Server ID: $SERVER_ID"
echo "Data Directory:     $SERVER_DIR"
echo "Configuration:      $CLUSTER_CONF"
echo "------------------------------------------------"

# Ensure the required directories exist.
mkdir -p "$SERVER_DIR"

# 6. Execution
echo "Executing: $SERVER_EXECUTABLE $SERVER_DIR $SERVER_ID $CLUSTER_CONF"
echo "--- Server Log Output (Press Ctrl+C to stop) ---"

"$SERVER_EXECUTABLE" "$SERVER_DIR" "$SERVER_ID" "$CLUSTER_CONF"

echo "--- Server (ID: $SERVER_ID) has shut down. ---"

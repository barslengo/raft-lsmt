#!/bin/bash

# --- Configuration ---
# Set the path to the compiled server executable
SERVER_EXECUTABLE="./build/server"

# Set the top-level directory where all server data will be stored
TOP_LEVEL_DIR="/tmp/raft"

# --- Script Logic ---

# 1. Input Validation: Check if a Server ID was provided as an argument.
if [ -z "$1" ]; then
    echo "Error: No Server ID provided."
    echo "Usage: $0 <server_id>"
    echo "Example: $0 1"
    exit 1
fi

# Assign the first argument ($1) to a more readable variable name
SERVER_ID="$1"

# 2. Prerequisite Check: Verify that the server program exists and is executable.
if [ ! -x "$SERVER_EXECUTABLE" ]; then
    echo "Error: Server executable not found or not executable at '$SERVER_EXECUTABLE'"
    echo "Please make sure you have compiled the server program first."
    exit 1
fi

# 3. Directory Setup: Construct the specific path for this server's data.
SERVER_DIR="$TOP_LEVEL_DIR/$SERVER_ID"

echo "Starting Server with ID: $SERVER_ID"

# Ensure the required directories exist. The '-p' flag creates parent directories
# as needed (e.g., /tmp/raft) and doesn't complain if they already exist.
echo "Ensuring data directory exists at: $SERVER_DIR"
mkdir -p "$SERVER_DIR"

# 4. Execution: Run the server program in the FOREGROUND.
# The script will now hand over control to the server process.
# Its output will appear directly in this terminal.
echo "Executing: $SERVER_EXECUTABLE $SERVER_DIR $SERVER_ID"
echo "--- Server Log Output (Press Ctrl+C to stop) ---"

"$SERVER_EXECUTABLE" "$SERVER_DIR" "$SERVER_ID"

# This line will only be reached after the server process has terminated.
echo "--- Server (ID: $SERVER_ID) has shut down. ---"

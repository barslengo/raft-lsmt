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

# --- Helper Function: Check Roles for a Cluster ---
check_cluster_roles() {
    local c_name="$1"
    local cluster_dir="$SOURCE_DIR/$c_name"
    if [ ! -d "$cluster_dir" ]; then
        echo "Error: Cluster directory '$cluster_dir' does not exist."
        return 1
    fi
    
    echo "=== Ruoli Correnti dei Nodi Attivi nel Cluster: $c_name ==="
    
    local ANY_ACTIVE=false
    # Find all numeric subdirectories inside the cluster folder
    local local_nodes
    local_nodes=$(find "$cluster_dir" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' | sort)
    
    for node_dir in $local_nodes; do
        local node_id
        node_id=$(basename "$node_dir")
        local pid_file="${node_dir}/.pid"
        local node_active=false
        local pid=""
        
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                node_active=true
                ANY_ACTIVE=true
            fi
        fi

        if [ "$node_active" = "true" ]; then
            # Find the most recent stats CSV file
            local stats_file
            stats_file=$(ls -t "${node_dir}"/stats_${node_id}_*.csv 2>/dev/null | head -n 1)
            
            if [ -n "$stats_file" ] && [ -f "$stats_file" ]; then
                # Read penultimate line
                local penultimate_line
                penultimate_line=$(tail -n 2 "$stats_file" | head -n 1)
                
                # Extract role (second column)
                local role
                role=$(echo "$penultimate_line" | cut -d',' -f2)
                
                # Fallback to last line if penultimate is the header or empty
                if [ -z "$role" ] || [ "$role" = "Role" ]; then
                    local last_line
                    last_line=$(tail -n 1 "$stats_file")
                    role=$(echo "$last_line" | cut -d',' -f2)
                fi
                
                if [ -n "$role" ] && [ "$role" != "Role" ]; then
                    echo "Cluster: $c_name | Node ID: $node_id | Role: $role (PID: $pid)"
                else
                    echo "Cluster: $c_name | Node ID: $node_id | Role: Unknown (no data in CSV) (PID: $pid)"
                fi
            else
                echo "Cluster: $c_name | Node ID: $node_id | Role: Starting/No stats CSV yet (PID: $pid)"
            fi
        else
            echo "Cluster: $c_name | Node ID: $node_id | Role: Offline/Stopped"
        fi
    done

    if [ "$ANY_ACTIVE" = "false" ]; then
        echo "Nessun nodo attivo trovato per il cluster $c_name (tutti offline)"
    fi
    echo "======================================"
}

# --- Main Dispatch ---
if [ -n "$CLUSTER_NAME" ]; then
    check_cluster_roles "$CLUSTER_NAME"
else
    # Iterate over all directories in SOURCE_DIR that contain a cluster.config
    for d in "$SOURCE_DIR"/*; do
        if [ -d "$d" ] && [ -f "$d/cluster.config" ]; then
            check_cluster_roles "$(basename "$d")"
        fi
    done
fi

#!/bin/bash
set -e

if [ -z "$NODE_ID" ]; then
    echo "Error: NODE_ID environment variable is not set."
    exit 1
fi

# Generazione dinamica di cluster.conf
if [ -n "$CLUSTER_NODES" ]; then
    echo "Generating cluster.conf from CLUSTER_NODES..."
    > cluster.conf
    # Splitta per punto e virgola
    IFS=';' read -ra NODES <<< "$CLUSTER_NODES"
    for i in "${NODES[@]}"; do
        # Splitta per virgola
        IFS=',' read -r id raft_addr client_port <<< "$i"
        echo "$id $raft_addr $client_port" >> cluster.conf
    done
fi

echo "--- cluster.conf ---"
cat cluster.conf
echo "--------------------"

DATA_DIR="/data"
mkdir -p "$DATA_DIR"

# Pulizia di file di vecchie esecuzioni (per un riavvio pulito)
#echo "Cleaning up old database files from $DATA_DIR..."
#rm -f "$DATA_DIR"/*.sbrolf "$DATA_DIR"/*.prot "$DATA_DIR"/.lsmt_metadata

echo "Starting Server ID: $NODE_ID..."
# Sostituisce il processo corrente con il tuo server
exec ./build/server "$DATA_DIR" "$NODE_ID" "cluster.conf"

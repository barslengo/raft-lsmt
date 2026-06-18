#!/bin/bash

# Abilita modalità per terminare in caso di errori imprevisti
set -e

CLUSTER_DIR="${1}"
if [ -z "$CLUSTER_DIR" ]; then
    echo "Errore: specificare la cartella del cluster come primo argomento (e.g. A, B)"
    echo "Usage: $0 <cluster_dir>"
    exit 1
fi

if [ ! -d "$CLUSTER_DIR" ]; then
    echo "Errore: la cartella '$CLUSTER_DIR' non esiste."
    exit 1
fi

# Rimuove lo slash finale se presente
cluster_dir="${CLUSTER_DIR%/}"
cluster_name=$(basename "$cluster_dir")

echo "=== Ruoli Correnti dei Nodi Attivi nel Cluster: $cluster_name ==="

# Rileva il file .pid del server del cluster
PID_FILE="${cluster_dir}/.pid"
NODE_ACTIVE=false
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        NODE_ACTIVE=true
    fi
fi

# Se il nodo non è attivo, lo segnaliamo ed usciamo
if [ "$NODE_ACTIVE" = "false" ]; then
    echo "Nessun nodo attivo trovato per il cluster $cluster_name (PID offline o non trovato)"
    exit 0
fi

# Itera sulle sotto-cartelle numeriche del nodo per trovare i dati
for node_dir in "$cluster_dir"/*/; do
    [ -d "$node_dir" ] || continue
    node_id=$(basename "$node_dir")
    
    if [[ "$node_id" =~ ^[0-9]+$ ]]; then
        # Trova il file stats CSV più recente
        stats_file=$(ls -t "${node_dir}"stats_${node_id}_*.csv 2>/dev/null | head -n 1)
        
        if [ -n "$stats_file" ] && [ -f "$stats_file" ]; then
            # Legge la penultima riga
            penultimate_line=$(tail -n 2 "$stats_file" | head -n 1)
            
            # Estrae il ruolo (seconda colonna)
            role=$(echo "$penultimate_line" | cut -d',' -f2)
            
            # Fallback sull'ultima riga se la penultima è l'header (o vuota)
            if [ -z "$role" ] || [ "$role" = "Role" ]; then
                last_line=$(tail -n 1 "$stats_file")
                role=$(echo "$last_line" | cut -d',' -f2)
            fi
            
            if [ -n "$role" ] && [ "$role" != "Role" ]; then
                echo "Cluster: $cluster_name | Node ID: $node_id | Role: $role (PID: $PID)"
            else
                echo "Cluster: $cluster_name | Node ID: $node_id | Role: Unknown (no data in CSV)"
            fi
        else
            echo "Cluster: $cluster_name | Node ID: $node_id | Role: Starting/No stats CSV yet (PID: $PID)"
        fi
    fi
done
echo "======================================"

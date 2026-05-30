#!/bin/bash

# Genera un timestamp nel formato YYYYMMDD_HHMMSS
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARGET_DIR="stats-folder-${TIMESTAMP}"

echo "=== Creazione cartella: $TARGET_DIR ==="
mkdir -p "$TARGET_DIR"

# Impostazioni di connessione
REMOTE_USER="Garavagliaadmin"

# ELENCO DEGLI IP DEI NODI
IPS=("10.0.1.4" "10.0.1.8" "10.0.1.5")

echo ""
echo "=== Download archivi dai nodi remoti ==="
for IP in "${IPS[@]}"; do
    echo "Scaricando da $IP..."
    # Esegue scp. Il costrutto || permette allo script di continuare anche se un nodo è offline.

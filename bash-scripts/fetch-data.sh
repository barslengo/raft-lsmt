#!/bin/bash

# Default values
PREFIX="stats-folder"
MINUTES=10

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -p, --prefix <name>   Folder name prefix (default: stats-folder)"
    echo "  -m, --minutes <num>   Minutes limit to search for recent CSV files (default: 10)"
    echo "  -h, --help            Show this help message"
    echo ""
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -p|--prefix)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            PREFIX="$2"
            shift 2
            ;;
        -m|--minutes)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument."
                exit 1
            fi
            MINUTES="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Error: Unknown parameter: $1"
            show_help
            exit 1
            ;;
    esac
done

# Genera un timestamp nel formato YYYYMMDD_HHMMSS
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARGET_DIR="${PREFIX}-${TIMESTAMP}"

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
    # Usiamo ConnectTimeout per evitare che il comando si blocchi per minuti se il nodo è offline.
    scp -o ConnectTimeout=2 -o ConnectionAttempts=1 "${REMOTE_USER}@${IP}:metrics-${IP}.tar.gz" "${TARGET_DIR}/" || echo "Warning: Failed to copy metrics from $IP"
done

echo ""
echo "=== Spostamento file di throughput (ultimi $MINUTES minuti) ==="
# Trova e sposta tutti i file CSV di throughput recenti nella cartella di destinazione
find . -maxdepth 1 \( -name "client_throughput_*.csv" -o -name "read_throughput_*.csv" \) -mmin -"${MINUTES}" -type f | while read -r csv_file; do
    echo "Spostando $csv_file -> $TARGET_DIR/"
    mv "$csv_file" "$TARGET_DIR/"
done

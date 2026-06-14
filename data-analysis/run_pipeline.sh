#!/bin/bash
set -e

# Controlla gli argomenti
if [ "$#" -lt 4 ]; then
    echo "Errore: Argomenti mancanti."
    echo "Uso: $0 <SOURCE_DIR> <STATS_DIR> <CLIENT_CSV> <OUTPUT_DIR>"
    exit 1
fi

SOURCE_DIR="$1"
STATS_DIR="$2"
CLIENT_CSV="$3"
OUTPUT_DIR="$4"

echo "=========================================="
echo "Avvio Pipeline di Analisi"
echo "SOURCE_DIR: $SOURCE_DIR"
echo "STATS_DIR:  $STATS_DIR"
echo "CLIENT_CSV: $CLIENT_CSV"
echo "OUTPUT_DIR: $OUTPUT_DIR"
echo "=========================================="

# 1. Raccogli le statistiche dai nodi
echo "Fase 1/2: Raccolta statistiche dai nodi..."
./gather-stats.sh "$SOURCE_DIR" "$STATS_DIR"

# 2. Genera la dashboard con Python
echo "Fase 2/3: Generazione dashboard e grafici..."
python3 generate-dashboard_v2.py -s "$STATS_DIR" -c "$CLIENT_CSV" -o "$OUTPUT_DIR"

# 3. Genera il report di recovery in HTML e grafici dedicati
echo "Fase 3/3: Generazione report di failure recovery e grafici dedicati..."
python3 generate_recovery_report.py -s "$STATS_DIR" -c "$CLIENT_CSV" -o "$OUTPUT_DIR" -r "$(dirname "$OUTPUT_DIR")/recovery_report.html"

echo "=========================================="
echo "Pipeline completata con successo!"
echo "I risultati sono disponibili in: $OUTPUT_DIR"
echo "Report recovery: $(dirname "$OUTPUT_DIR")/recovery_report.html"
echo "=========================================="

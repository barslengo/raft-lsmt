#!/bin/bash
set -e

# Controlla gli argomenti e supporta sia l'uso semplificato che dettagliato
if [ "$#" -eq 1 ] || [ "$#" -eq 2 ]; then
    SOURCE_DIR="$1"
    STATS_DIR="$SOURCE_DIR/stats"
    CLIENT_CSV="$SOURCE_DIR"
    OUTPUT_DIR="$SOURCE_DIR/plots_custom"
    TYPE="${2:-all}"
elif [ "$#" -eq 4 ] || [ "$#" -eq 5 ]; then
    SOURCE_DIR="$1"
    STATS_DIR="$2"
    CLIENT_CSV="$3"
    OUTPUT_DIR="$4"
    TYPE="${5:-all}"
else
    echo "Errore: Numero di argomenti non valido."
    echo "Uso semplificato (Consigliato):"
    echo "  $0 <SOURCE_DIR> [TYPE]"
    echo "  Esempio: $0 ./reads/read-pagination-v2-20260611_115650 read"
    echo ""
    echo "Uso dettagliato (Avanzato):"
    echo "  $0 <SOURCE_DIR> <STATS_DIR> <CLIENT_CSV> <OUTPUT_DIR> [TYPE]"
    exit 1
fi

# Convert to lowercase
TYPE=$(echo "$TYPE" | tr '[:upper:]' '[:lower:]')

# Validate TYPE
if [[ "$TYPE" != "recovery" && "$TYPE" != "write" && "$TYPE" != "read" && "$TYPE" != "all" ]]; then
    echo "Errore: Tipo sconosciuto '$TYPE'."
    echo "Tipi validi: recovery, write, read, all"
    exit 1
fi

echo "=========================================="
echo "Avvio Pipeline di Analisi"
echo "SOURCE_DIR:  $SOURCE_DIR"
echo "STATS_DIR:   $STATS_DIR"
echo "CLIENT_CSV:  $CLIENT_CSV"
echo "OUTPUT_DIR:  $OUTPUT_DIR"
echo "REPORT TYPE: $TYPE"
echo "=========================================="

# 1. Raccogli le statistiche dai nodi
echo "Fase 1/2: Raccolta statistiche dai nodi..."
./gather-stats.sh "$SOURCE_DIR" "$STATS_DIR"

# 2. Genera i report specifici richiesti
echo "Fase 2/2: Generazione grafici e report in HTML..."
mkdir -p "$OUTPUT_DIR/plots"

if [[ "$TYPE" == "recovery" || "$TYPE" == "all" ]]; then
    echo "-> Generazione Report Failure Recovery..."
    python3 generate_report.py -s "$STATS_DIR" -c "$CLIENT_CSV" -o "$OUTPUT_DIR/plots" -r "$OUTPUT_DIR/recovery_report.html" -t recovery
fi

if [[ "$TYPE" == "write" || "$TYPE" == "all" ]]; then
    echo "-> Generazione Report Write Performance..."
    python3 generate_report.py -s "$STATS_DIR" -c "$CLIENT_CSV" -o "$OUTPUT_DIR/plots" -r "$OUTPUT_DIR/write_report.html" -t write
fi

if [[ "$TYPE" == "read" || "$TYPE" == "all" ]]; then
    echo "-> Generazione Report Read Performance..."
    python3 generate_report.py -s "$STATS_DIR" -c "$CLIENT_CSV" -o "$OUTPUT_DIR/plots" -r "$OUTPUT_DIR/read_report.html" -t read
fi

echo "=========================================="
echo "Pipeline completata con successo!"
echo "I risultati sono disponibili in: $OUTPUT_DIR"
if [[ "$TYPE" == "recovery" || "$TYPE" == "all" ]]; then
    echo "  - Report recovery: $OUTPUT_DIR/recovery_report.html"
fi
if [[ "$TYPE" == "write" || "$TYPE" == "all" ]]; then
    echo "  - Report write: $OUTPUT_DIR/write_report.html"
fi
if [[ "$TYPE" == "read" || "$TYPE" == "all" ]]; then
    echo "  - Report read: $OUTPUT_DIR/read_report.html"
fi
echo "=========================================="

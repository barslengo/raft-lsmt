#!/bin/bash

# Interrompe lo script in caso di errori critici
set -e

# --- CONFIGURAZIONE (con defaults) ---
CONFIG_FILE="${1:-servers.json}"
REQUESTS="${2:-30000000}"
READ_REQUESTS="${3:-100000}"
JITTER="${4:-0.0}"
GLOBAL_KEYSPACE="${5:-false}"
ROUTING_STRATEGY="${6:-hash}"
DATA_DIST="${7}"

if [ -z "$DATA_DIST" ]; then
    echo "Errore: specificare la data distribution come settimo argomento (e.g. sequential, uniform, zipfian)"
    echo "Usage: $0 [config] [write_req] [read_req] [jitter] [global_keyspace] [routing_strat] <data-dist>"
    exit 1
fi

echo "=========================================================="
echo "🚀 Avvio del Benchmark di Routing (Singola Distribuzione)"
echo "Distribuzione dati  : $DATA_DIST"
echo "Configurazione      : $CONFIG_FILE"
echo "Richieste Scrittura : $REQUESTS"
echo "Richieste Lettura   : $READ_REQUESTS"
echo "Routing strategy    : $ROUTING_STRATEGY"
echo "Batch Jitter        : $JITTER"
echo "Global Keyspace     : $GLOBAL_KEYSPACE"
echo "=========================================================="

# Rileva il percorso corretto per lo script multi-processo di scrittura
if [ -f "bash-scripts/run_bench_multi.sh" ]; then
    MULTI_SCRIPT="./bash-scripts/run_bench_multi.sh"
else
    MULTI_SCRIPT="./run_bench_multi.sh"
fi

# Rileva il percorso corretto per lo script multi-processo di lettura
if [ -f "bash-scripts/run_read_bench_multi.sh" ]; then
    MULTI_READ_SCRIPT="./bash-scripts/run_read_bench_multi.sh"
else
    MULTI_READ_SCRIPT="./run_read_bench_multi.sh"
fi

echo ""
echo "----------------------------------------------------------"
echo "▶️ INIZIO TEST: Data Distribution -> [ $DATA_DIST ]"
echo "----------------------------------------------------------"

EXTRA_ARGS=""
if [ "$GLOBAL_KEYSPACE" = "true" ]; then
    EXTRA_ARGS="--global-keyspace"
fi

echo "➡️ Avvio benchmark di SCRITTURA (distribuzione: $DATA_DIST)..."
# Esecuzione dello script multi-processo di scrittura
"$MULTI_SCRIPT" \
    --config "$CONFIG_FILE" \
    --requests $REQUESTS \
    --strategy "$ROUTING_STRATEGY" \
    --dist "$DATA_DIST" \
    --jitter "$JITTER" \
    $EXTRA_ARGS
    
echo "⏳ Pausa di 30 secondi prima del test di lettura..."
sleep 30

echo "➡️ Avvio benchmark di LETTURA (distribuzione: $DATA_DIST)..."
# Esecuzione dello script multi-processo di lettura
"$MULTI_READ_SCRIPT" \
    --config "$CONFIG_FILE" \
    --requests $READ_REQUESTS \
    --strategy "$ROUTING_STRATEGY" \
    --dist "$DATA_DIST" \
    --jitter "$JITTER" \
    -t 4 \
    --range 4000 \
    $EXTRA_ARGS

echo "✅ Test con distribuzione '$DATA_DIST' completato!"

echo "⏳ Pausa di 5 secondi per consentire il completamento della scrittura dei file..."
sleep 5

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARGET_DIR="${ROUTING_STRATEGY}-${DATA_DIST}-${TIMESTAMP}"

echo "📁 Creazione della cartella per i risultati: $TARGET_DIR"
mkdir -p "$TARGET_DIR"

# Copia i file CSV di throughput (ultimi 10 minuti)
find . -maxdepth 1 \( -name "client_throughput_*.csv" -o -name "read_throughput_*.csv" \) -mmin -10 -type f | while read -r csv_file; do
    echo "Copiando $csv_file -> $TARGET_DIR/"
    cp "$csv_file" "$TARGET_DIR/"
done

echo "=========================================================="

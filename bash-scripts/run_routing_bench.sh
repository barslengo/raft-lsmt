#!/bin/bash

# Interrompe lo script in caso di errori critici
set -e

# --- CONFIGURAZIONE (con defaults) ---
CONFIG_FILE="${1:-servers.json}"
REQUESTS="${2:-30000000}"
ROUTING_STRATEGY="${3:-hash}"
JITTER="${4:-0.0}"
GLOBAL_KEYSPACE="${5:-false}"

# --- DISTRIBUZIONI DA TESTARE ---
DISTRIBUTIONS=("sequential" "uniform" "zipfian")

echo "=========================================================="
echo "🚀 Avvio della suite di Benchmark in Scrittura (Multi-Process)"
echo "Configurazione     : $CONFIG_FILE"
echo "Richieste per test : $REQUESTS"
echo "Routing strategy   : $ROUTING_STRATEGY"
echo "Batch Jitter       : $JITTER"
echo "Global Keyspace    : $GLOBAL_KEYSPACE"
echo "=========================================================="

# Rileva il percorso corretto per lo script multi-processo
if [ -f "bash-scripts/run_bench_multi.sh" ]; then
    MULTI_SCRIPT="./bash-scripts/run_bench_multi.sh"
else
    MULTI_SCRIPT="./run_bench_multi.sh"
fi

for DIST in "${DISTRIBUTIONS[@]}"; do
    echo ""
    echo "----------------------------------------------------------"
    echo "▶️ INIZIO TEST: Data Distribution -> [ $DIST ]"
    echo "----------------------------------------------------------"
    
    EXTRA_ARGS=""
    if [ "$GLOBAL_KEYSPACE" = "true" ]; then
        EXTRA_ARGS="--global-keyspace"
    fi

    # Esecuzione dello script multi-processo
    "$MULTI_SCRIPT" \
        --config "$CONFIG_FILE" \
        --requests $REQUESTS \
        --strategy "$ROUTING_STRATEGY" \
        --dist "$DIST" \
        --jitter "$JITTER" \
        $EXTRA_ARGS
        
    echo "✅ Test con distribuzione '$DIST' completato!"
    
    # Pausa di raffreddamento solo se non siamo all'ultimo ciclo
    if [ "$DIST" != "${DISTRIBUTIONS[-1]}" ]; then
        echo "⏳ Pausa di 30 secondi per stabilizzare l'LSM-Tree e Raft..."
        sleep 30
    fi
done

echo ""
echo "🎉 Tutti i benchmark sono stati completati con successo!"
echo "=========================================================="

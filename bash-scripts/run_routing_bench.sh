#!/bin/bash

# Interrompe lo script in caso di errori critici
set -e

# --- CONFIGURAZIONE (con defaults) ---
CONFIG_FILE="${1:-servers.json}"
REQUESTS="${2:-30000000}"
ROUTING_STRATEGY="${3:-hash}"
JITTER="${4:-0.0}"

# --- DISTRIBUZIONI DA TESTARE ---
DISTRIBUTIONS=("sequential" "uniform" "zipfian")

echo "=========================================================="
echo "🚀 Avvio della suite di Benchmark in Scrittura (Multi-Process)"
echo "Configurazione     : $CONFIG_FILE"
echo "Richieste per test : $REQUESTS"
echo "Routing strategy   : $ROUTING_STRATEGY"
echo "Batch Jitter       : $JITTER"
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
    
    # Esecuzione dello script multi-processo
    "$MULTI_SCRIPT" \
        --config "$CONFIG_FILE" \
        --requests $REQUESTS \
        --strategy "$ROUTING_STRATEGY" \
        --dist "$DIST" \
        --jitter "$JITTER"
        
    echo "✅ Test con distribuzione '$DIST' completato!"
    
    # Pausa di raffreddamento solo se non siamo all'ultimo ciclo
    if [ "$DIST" != "${DISTRIBUTIONS[-1]}" ]; then
        echo "⏳ Pausa di 15 secondi per stabilizzare l'LSM-Tree e Raft..."
        sleep 15
    fi
done

echo ""
echo "🎉 Tutti i benchmark sono stati completati con successo!"
echo "=========================================================="

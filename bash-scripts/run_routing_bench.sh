#!/bin/bash

# Interrompe lo script in caso di errori critici
set -e

# --- PARAMETRI FISSI ---
CONFIG_FILE="servers.json"
REQUESTS=30000000
ROUTING_STRATEGY="hash"

# --- DISTRIBUZIONI DA TESTARE ---
DISTRIBUTIONS=("sequential" "uniform" "zipfian")

echo "=========================================================="
echo "🚀 Avvio della suite di Benchmark in Scrittura"
echo "Configurazione     : $CONFIG_FILE"
echo "Richieste per test : $REQUESTS"
echo "Routing strategy   : $ROUTING_STRATEGY"
echo "=========================================================="

for DIST in "${DISTRIBUTIONS[@]}"; do
    echo ""
    echo "----------------------------------------------------------"
    echo "▶️ INIZIO TEST: Data Distribution -> [ $DIST ]"
    echo "----------------------------------------------------------"
    
    # Esecuzione dello script Python
    python3 write-bench.py \
        --config "$CONFIG_FILE" \
        --requests $REQUESTS \
        --routing-strategy "$ROUTING_STRATEGY" \
        --data-dist "$DIST"
        
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

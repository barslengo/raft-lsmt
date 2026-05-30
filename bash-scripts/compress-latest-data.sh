#!/bin/bash
set -e

echo "=== Compressione Dati LSMT ==="

# 1. Recupera il primo IP della macchina (ignorando interfacce virtuali o IP secondari)
IP=$(hostname -I | awk '{print $1}')

if [ -z "$IP" ]; then
    echo "Errore: Impossibile rilevare l'indirizzo IP della macchina."
    exit 1
fi
echo "IP rilevato: $IP"

# 2. Trova l'ultima cartella creata che inizia con "data-"
# ls -td ordina per data (il più recente in alto), head -n 1 prende solo il primo
LATEST_DIR=$(ls -td data-*/ 2>/dev/null | head -n 1)

if [ -z "$LATEST_DIR" ]; then
    echo "Errore: Nessuna cartella 'data-*' trovata in questa directory."
    exit 1
fi

# Rimuove lo slash finale per pulizia (es. "data-20260529_110242/" diventa "data-20260529_110242")
LATEST_DIR=${LATEST_DIR%/}
echo "Ultima cartella rilevata: $LATEST_DIR"

# 3. Nome dell'archivio finale
ARCHIVE_NAME="metrics-${IP}.tar.gz"

# 4. Esegue la compressione (senza la 'v' in -czf per non intasare il terminale con mille stampe)
echo "Compressione in corso in '$ARCHIVE_NAME'..."
tar -czf "$ARCHIVE_NAME" "$LATEST_DIR"

# 5. Conferma finale
echo "Fatto! Archivio pronto:"
ls -lh "$ARCHIVE_NAME"

#!/bin/bash
set -e

# Controlla se sono stati passati entrambi gli argomenti
if [ "$#" -lt 2 ]; then
    echo "Errore: Argomenti mancanti."
    echo "Uso: $0 <cartella_sorgente_dati> <cartella_destinazione_stats>"
    echo "Esempio: $0 /home/user/my_db_tests /home/user/my_db_tests/stats"
    exit 1
fi

SOURCE_DIR="$1"
TARGET_DIR="$2"

echo "Ricerca cartelle 'data-*' in: $SOURCE_DIR"
echo "Creazione della cartella destinazione: $TARGET_DIR"
mkdir -p "$TARGET_DIR"

# Cerca tutte le cartelle che iniziano con "data-" nel percorso fornito come SOURCE_DIR
for data_dir in "$SOURCE_DIR"/data-*/; do
    # Se non ci sono cartelle che matchano, esci dal loop
    [ -e "$data_dir" ] || continue

    # Rimuovi lo slash finale per pulizia visiva nel log
    dir_name=$(basename "$data_dir")
    echo "Analizzando: $dir_name..."
    
    # Itera sui Cluster (es. A, B, C) dentro la cartella data
    for cluster_dir in "$data_dir"*/; do
        [ -e "$cluster_dir" ] || continue
        cluster_name=$(basename "$cluster_dir")
        
        # Itera sui Nodi (es. 1, 2, 3) dentro il Cluster
        for node_dir in "$cluster_dir"*/; do
            [ -e "$node_dir" ] || continue
            node_id=$(basename "$node_dir")
            
            # Crea il percorso di destinazione (es. <TARGET_DIR>/A/3)
            target_path="$TARGET_DIR/$cluster_name/$node_id"
            mkdir -p "$target_path"
            
            # Copia tutti i file .csv trovati
            # Il 2>/dev/null evita errori a schermo se una cartella nodo non ha csv
            if ls "$node_dir"*.csv 1> /dev/null 2>&1; then
                cp "$node_dir"*.csv "$target_path/"
                echo "  -> Copiati file da $cluster_name/$node_id"
            fi
        done
    done
done

echo "=========================================="
echo "Operazione completata! Tutti i file CSV sono stati uniti in $TARGET_DIR/"


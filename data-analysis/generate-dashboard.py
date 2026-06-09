import os
import glob
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Impostazioni grafiche
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_context("notebook", font_scale=1.1)

# NB: Dai tuoi sample gli header sembrano separati da spazi. 
# Se sono file CSV standard usa ',', se sono separati da spazi/tab usa r'\s+'
CSV_SEP = ',' 

def parse_args():
    parser = argparse.ArgumentParser(description="Genera dashboard per DB distribuito Raft + LSMT")
    parser.add_argument('-s', '--stats-dir', type=str, required=True,
                        help="Path della cartella contenente i log delle stats (es. ./stats)")
    parser.add_argument('-c', '--client-csv', type=str, required=True,
                        help="Path del file CSV aggregato del client (es. ./client_throughput_1780152378.csv)")
    parser.add_argument('-o', '--output-dir', type=str, default='dashboards_output',
                        help="Cartella di output dove verranno salvati i grafici (default: dashboards_output)")
    return parser.parse_args()

def load_all_stats(base_dir):
    """Carica e unisce tutti i file stats, ordinandoli per nodo e tempo."""
    all_dfs = []
    pattern = os.path.join(base_dir, '*', '*', 'stats_*.csv')
    
    for file_path in glob.glob(pattern):
        parts = file_path.split(os.sep)
        # Assumiamo che la struttura sia base_dir / Cluster / Node / stats_X.csv
        cluster_id = parts[-3]
        node_id = parts[-2]
        
        df = pd.read_csv(file_path, sep=CSV_SEP)
        df['Cluster'] = cluster_id
        df['Node'] = node_id
        all_dfs.append(df)
        
    if not all_dfs:
        raise ValueError(f"Nessun file stats_*.csv trovato in {base_dir}!")
        
    df_merged = pd.concat(all_dfs, ignore_index=True)
    return df_merged

def calculate_rates(df):
    """Calcola OPS e MB/s partendo dai contatori monotoni, gestendo i riavvii."""
    df.sort_values(by=['Cluster', 'Node', 'Timestamp_ms'], inplace=True)
    
    df['Relative_Time_s'] = (df['Timestamp_ms'] - df['Timestamp_ms'].min()) / 1000.0
    df['Time_Diff_s'] = df.groupby(['Cluster', 'Node'])['Timestamp_ms'].diff() / 1000.0
    
    cols = ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes']
    for col in cols:
        df[f'{col}_Diff'] = df.groupby(['Cluster', 'Node'])[col].diff()
        
    # Filtro riavvii
    valid_mask = (df['Time_Diff_s'] > 0) & (df['Total_Write_Requests_Diff'] >= 0)
    df_valid = df[valid_mask].copy()
    
    df_valid['Write_OPS'] = df_valid['Total_Write_Requests_Diff'] / df_valid['Time_Diff_s']
    df_valid['Write_MBps'] = (df_valid['Total_Write_Bytes_Diff'] / df_valid['Time_Diff_s']) / (1024 * 1024)
    df_valid['Read_OPS'] = df_valid['Total_Read_Requests_Diff'] / df_valid['Time_Diff_s']
    df_valid['Read_MBps'] = (df_valid['Total_Read_Bytes_Diff'] / df_valid['Time_Diff_s']) / (1024 * 1024)
    
    return df_valid

def plot_client_throughput(client_file, output_dir):
    df = pd.read_csv(client_file, sep=CSV_SEP)
    df['Relative_Time_s'] = (df['Timestamp'] - df['Timestamp'].min())
    
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.set_xlabel('Tempo (s)')
    ax1.set_ylabel('Write OPS', color='tab:blue')
    ax1.plot(df['Relative_Time_s'], df['OPS'], color='tab:blue', label='OPS')
    
    ax2 = ax1.twinx()  
    ax2.set_ylabel('Throughput (MB/s)', color='tab:red')  
    ax2.plot(df['Relative_Time_s'], df['MBps'], color='tab:red', linestyle='--', label='MBps')
    
    fig.suptitle('Client Aggregate Throughput')
    fig.tight_layout()
    plt.savefig(os.path.join(output_dir, '1_client_throughput.png'))
    plt.close()

def plot_leader_throughput(df_stats, output_dir):
    leaders_df = df_stats[df_stats['Role'].str.lower() == 'leader'].copy()
    clusters = leaders_df['Cluster'].unique()
    
    fig, axes = plt.subplots(len(clusters), 2, figsize=(16, 4 * len(clusters)))
    if len(clusters) == 1: axes = [axes]
    
    for ax_row, cluster in zip(axes, clusters):
        df_cluster = leaders_df[leaders_df['Cluster'] == cluster]
        
        # Grafico OPS
        ax_row[0].plot(df_cluster['Relative_Time_s'], df_cluster['Write_OPS'], label='Write OPS', color='blue')
        ax_row[0].plot(df_cluster['Relative_Time_s'], df_cluster['Read_OPS'], label='Read OPS', color='green', alpha=0.7)
        ax_row[0].set_title(f'Leader OPS - Cluster {cluster}')
        ax_row[0].set_xlabel('Tempo (s)')
        ax_row[0].set_ylabel('Operazioni/sec')
        ax_row[0].legend()
        
        # Grafico MB/s
        ax_row[1].plot(df_cluster['Relative_Time_s'], df_cluster['Write_MBps'], label='Write MB/s', color='red')
        ax_row[1].plot(df_cluster['Relative_Time_s'], df_cluster['Read_MBps'], label='Read MB/s', color='orange', alpha=0.7)
        ax_row[1].set_title(f'Leader Bandwidth - Cluster {cluster}')
        ax_row[1].set_xlabel('Tempo (s)')
        ax_row[1].set_ylabel('MB/s')
        ax_row[1].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_leader_throughput.png'))
    plt.close()

def plot_leader_latency(df_stats, output_dir):
    leaders_df = df_stats[df_stats['Role'].str.lower() == 'leader'].copy()
    clusters = leaders_df['Cluster'].unique()
    
    fig, axes = plt.subplots(len(clusters), 1, figsize=(14, 4 * len(clusters)))
    if len(clusters) == 1: axes = [axes]
    
    for ax, cluster in zip(axes, clusters):
        df_cluster = leaders_df[leaders_df['Cluster'] == cluster]
        
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P50_Latency_ms'], label='P50', alpha=0.9)
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P95_Latency_ms'], label='P95', alpha=0.8)
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P99_Latency_ms'], label='P99', color='red', alpha=0.8)
        
        ax.set_title(f'Leader Latencies - Cluster {cluster}')
        ax.set_xlabel('Tempo (s)')
        ax.set_ylabel('Latenza (ms) - Log Scale')
        ax.set_yscale('log')
        ax.legend()
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '3_leader_latency.png'))
    plt.close()

def plot_raft_alignment(df_stats, output_dir):
    clusters = df_stats['Cluster'].unique()
    fig, axes = plt.subplots(len(clusters), 1, figsize=(14, 4 * len(clusters)))
    if len(clusters) == 1: axes = [axes]
    
    for ax, cluster in zip(axes, clusters):
        df_cluster = df_stats[df_stats['Cluster'] == cluster]
        
        for node in df_cluster['Node'].unique():
            df_node = df_cluster[df_cluster['Node'] == node]
            ax.plot(df_node['Relative_Time_s'], df_node['Raft_Idx_Local'], label=f'Node {node}')

        ax.set_title(f'Raft Log Alignment - Cluster {cluster}')
        ax.set_xlabel('Tempo (s)')
        ax.set_ylabel('Raft Local Index')
        ax.legend()
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '4_raft_alignment.png'))
    plt.close()

def plot_lsm_events(base_dir, output_dir):
    all_events = []
    for file_path in glob.glob(os.path.join(base_dir, '*', '*', 'storage_events_*.csv')):
        df = pd.read_csv(file_path, sep=CSV_SEP)
        # base_dir / Cluster / Node / storage_events_X.csv
        df['Cluster'] = file_path.split(os.sep)[-3]
        all_events.append(df)
        
    if not all_events:
        print(f"  -> Nessun file storage_events_*.csv trovato in {base_dir}, salto il grafico LSM.")
        return
        
    df_ev = pd.concat(all_events, ignore_index=True)
    df_ev['Relative_Time_s'] = (df_ev['timestamp'] - df_ev['timestamp'].min()) / 1000.0
    
    plt.figure(figsize=(14, 6))
    sns.scatterplot(
        data=df_ev, 
        x='Relative_Time_s', 
        y='duration_ms', 
        hue='event_type', 
        size='output_bytes',
        style='Cluster',
        alpha=0.7, 
        sizes=(20, 300)
    )
    
    plt.title('LSM-Tree Storage Events (Flush / Compactions)')
    plt.xlabel('Tempo (s)')
    plt.ylabel('Durata (ms)')
    plt.yscale('log')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '5_lsm_storage_events.png'))
    plt.close()

if __name__ == "__main__":
    args = parse_args()
    
    # Crea la cartella di output se non esiste
    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Cartella di output preparata: {args.output_dir}/")
    
    print("1/6 - Caricamento e analisi dei dati raw...")
    raw_df = load_all_stats(args.stats_dir)
    
    print("2/6 - Calcolo derivate (OPS, MBps) e gestione riavvii...")
    processed_df = calculate_rates(raw_df)
    
    print("3/6 - Generazione Client Throughput...")
    if os.path.exists(args.client_csv):
        plot_client_throughput(args.client_csv, args.output_dir)
    else:
        print(f"  -> File {args.client_csv} non trovato, salto.")
        
    print("4/6 - Generazione grafici Leader...")
    plot_leader_throughput(processed_df, args.output_dir)
    plot_leader_latency(processed_df, args.output_dir)
    
    print("5/6 - Generazione grafici Raft...")
    plot_raft_alignment(processed_df, args.output_dir)
    
    print("6/6 - Generazione grafici Storage (LSM)...")
    plot_lsm_events(args.stats_dir, args.output_dir)
    
    print(f"Finito! Tutti i grafici sono stati generati e salvati in: '{args.output_dir}/'")

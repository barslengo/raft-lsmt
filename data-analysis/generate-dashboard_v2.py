import os
import glob
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Impostazioni grafiche
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_context("notebook", font_scale=1.1)
CSV_SEP = ',' 

def parse_args():
    parser = argparse.ArgumentParser(description="Genera dashboard per DB distribuito Raft + LSMT")
    parser.add_argument('-s', '--stats-dir', type=str, required=True, help="Path della cartella stats")
    parser.add_argument('-c', '--client-csv', type=str, required=True, help="Path del file CSV del client")
    parser.add_argument('-o', '--output-dir', type=str, default='dashboards_output')
    return parser.parse_args()

def sanitize_timestamps(df, col='Timestamp_ms'):
    """Rimuove timestamp palesemente errati (es. 1970) per evitare OOM durante il resample."""
    min_valid_ms = 1577836800000 # 1 Gennaio 2020
    max_valid_ms = 2524608000000 # 1 Gennaio 2050
    
    initial_len = len(df)
    df = df[(df[col] > min_valid_ms) & (df[col] < max_valid_ms)].copy()
    
    dropped = initial_len - len(df)
    if dropped > 0:
        print(f"  [🛡️ SAFETY] Rimosse {dropped} righe con timestamp anomali/corrotti.")
    return df

def load_client_data(client_file):
    df = pd.read_csv(client_file, sep=CSV_SEP)
    df.columns = df.columns.str.strip()
    
    # Auto-detect: se il timestamp è piccolo, è in secondi. Altrimenti in millisecondi.
    if df['Timestamp'].mean() < 3000000000:
        df['Timestamp_ms'] = df['Timestamp'] * 1000
    else:
        df['Timestamp_ms'] = df['Timestamp']
        
    return sanitize_timestamps(df, 'Timestamp_ms')

def load_and_resample_server_data(base_dir):
    all_dfs = []
    pattern = os.path.join(base_dir, '*', '*', 'stats_*.csv')
    
    for file_path in glob.glob(pattern):
        parts = file_path.split(os.sep)
        cluster_id, node_id = parts[-3], parts[-2]
        
        df = pd.read_csv(file_path, sep=CSV_SEP)
        df.columns = df.columns.str.strip() 
        
        df['Cluster'] = cluster_id
        df['Node'] = node_id
        all_dfs.append(df)
        
    if not all_dfs:
        raise ValueError(f"Nessun file stats_*.csv trovato in {base_dir}!")
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=['Cluster', 'Node', 'Timestamp_ms'])
    
    # FORZATURA NUMERICA
    colonne_contatori = ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes', 'Total_Requests', 'Total_Bytes']
    for col in colonne_contatori:
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce')
    
    df_raw = sanitize_timestamps(df_raw, 'Timestamp_ms')
    
    timespan_s = (df_raw['Timestamp_ms'].max() - df_raw['Timestamp_ms'].min()) / 1000.0
    if timespan_s > 7200: 
        print(f"  [⚠️ WARNING] Dati sparsi su {timespan_s/3600:.2f} ore! Tronco a 1 ora per evitare Crash RAM.")
        df_raw = df_raw[df_raw['Timestamp_ms'] <= df_raw['Timestamp_ms'].min() + 3600000]

    df_raw['Datetime'] = pd.to_datetime(df_raw['Timestamp_ms'], unit='ms')
    resampled_nodes = []
    
    for (cluster, node), group in df_raw.groupby(['Cluster', 'Node']):
        group = group.set_index('Datetime').sort_index()
        
        # RESAMPLE
        res = group.resample('1s').last()
        
        # FORWARD FILL per non perdere il totale durante i crash
        for col in colonne_contatori:
            if col in res.columns:
                res[col] = res[col].ffill().fillna(0)
                
        for col in ['Role', 'Term', 'Raft_Idx_Local', 'Raft_Idx_Applied', 'Raft_Idx_Commit']:
            if col in res.columns:
                res[col] = res[col].ffill()
        
        # CALCOLO DEI DELTA
        req_col = 'Total_Write_Requests' if 'Total_Write_Requests' in res.columns else 'Total_Requests'
        byte_col = 'Total_Write_Bytes' if 'Total_Write_Bytes' in res.columns else 'Total_Bytes'
        
        if req_col in res.columns:
            raw_ops = res[req_col].diff().fillna(0)
            reset_mask = raw_ops < 0
            raw_ops.loc[reset_mask] = res.loc[reset_mask, req_col]
            
            res['Raw_Write_OPS'] = raw_ops.copy()
            res['Write_OPS'] = raw_ops.copy()
            res.loc[res['Write_OPS'] > 1500000, 'Write_OPS'] = np.nan
        else:
            res['Write_OPS'] = 0
            res['Raw_Write_OPS'] = 0
            
        if byte_col in res.columns:
            res['Write_MBps'] = res[byte_col].diff().fillna(0) / (1024 * 1024)
            reset_mask_mb = res['Write_MBps'] < 0
            res.loc[reset_mask_mb, 'Write_MBps'] = res.loc[reset_mask_mb, byte_col] / (1024 * 1024)
            res.loc[res['Write_MBps'] > 200.0, 'Write_MBps'] = np.nan
        else:
            res['Write_MBps'] = 0
        
        res['Cluster'] = cluster
        res['Node'] = node
        resampled_nodes.append(res.reset_index())
        
    return pd.concat(resampled_nodes, ignore_index=True)

def plot_chaos_engineering(df_server, output_dir):
    clusters = sorted(df_server['Cluster'].dropna().unique())
    fig, axes = plt.subplots(len(clusters), 1, figsize=(16, 5 * len(clusters)), sharex=True)
    if len(clusters) == 1: axes = [axes]
    
    for ax, cluster in zip(axes, clusters):
        df_cluster = df_server[df_server['Cluster'] == cluster]
        
        term_changes = df_cluster.groupby('Relative_Time_s')['Term'].max().diff()
        election_times = term_changes[term_changes > 0].index
        
        for et in election_times:
            ax.axvline(x=et, color='red', linestyle='--', alpha=0.6, linewidth=1.5)
            
        for node in sorted(df_cluster['Node'].dropna().unique()):
            df_node = df_cluster[df_cluster['Node'] == node]
            is_leader = df_node['Role'].str.upper() == 'LEADER'
            
            ax.plot(df_node.loc[is_leader, 'Relative_Time_s'], 
                    df_node.loc[is_leader, 'Write_OPS'], 
                    linewidth=2.5, label=f'Node {node} (LEADER)')
            ax.plot(df_node.loc[~is_leader, 'Relative_Time_s'], 
                    df_node.loc[~is_leader, 'Write_OPS'], 
                    linewidth=1, linestyle=':', alpha=0.5)

        ax.set_title(f'Chaos Engineering: Leader Elections & Throughput - Cluster {cluster}')
        ax.set_ylabel('Write OPS')
        ax.legend(loc='upper right')
        
    axes[-1].set_xlabel('Tempo dall\'inizio del test (s)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '1_chaos_engineering.png'))
    plt.close()

def plot_client_throughput(df_client, output_dir):
    fig, ax1 = plt.subplots(figsize=(14, 5))
    ax1.set_xlabel('Tempo dall\'inizio del test (s)')
    ax1.set_ylabel('OPS', color='tab:blue')
    # Supporta sia la colonna "OPS" che "QPS" per funzionare sia con write che read bench
    col_ops = 'OPS' if 'OPS' in df_client.columns else 'QPS'
    
    ax1.plot(df_client['Relative_Time_s'], df_client[col_ops], color='tab:blue', label=col_ops)
    
    ax2 = ax1.twinx()  
    ax2.set_ylabel('Throughput (MB/s)', color='tab:red')  
    ax2.plot(df_client['Relative_Time_s'], df_client['MBps'], color='tab:red', linestyle='--', label='MBps', alpha=0.7)
    
    fig.suptitle(f'Client Aggregate Throughput ({col_ops})')
    fig.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_client_throughput.png'))
    plt.close()

def plot_leader_latency(df_server, output_dir):
    if 'P50_Latency_ms' not in df_server.columns:
        print("  -> Latenze non trovate nel CSV, salto il grafico delle latenze.")
        return
        
    leaders_df = df_server[df_server['Role'].str.upper() == 'LEADER'].copy()
    clusters = sorted(leaders_df['Cluster'].unique())
    
    if not clusters: return
    
    fig, axes = plt.subplots(len(clusters), 1, figsize=(14, 4 * len(clusters)), sharex=True)
    if len(clusters) == 1: axes = [axes]
    
    for ax, cluster in zip(axes, clusters):
        df_cluster = leaders_df[leaders_df['Cluster'] == cluster]
        
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P50_Latency_ms'], label='P50', alpha=0.9)
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P95_Latency_ms'], label='P95', alpha=0.8)
        ax.plot(df_cluster['Relative_Time_s'], df_cluster['P99_Latency_ms'], label='P99', color='red', alpha=0.8)
        
        ax.set_title(f'Leader Latencies - Cluster {cluster}')
        ax.set_ylabel('Latenza (ms) - Log Scale')
        ax.set_yscale('log')
        ax.legend()
        
    axes[-1].set_xlabel('Tempo dall\'inizio del test (s)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '3_leader_latency.png'))
    plt.close()

def plot_raft_alignment(df_server, output_dir):
    clusters = sorted(df_server['Cluster'].dropna().unique())
    fig, axes = plt.subplots(len(clusters), 1, figsize=(14, 4 * len(clusters)), sharex=True)
    if len(clusters) == 1: axes = [axes]
    
    for ax, cluster in zip(axes, clusters):
        df_cluster = df_server[df_server['Cluster'] == cluster]
        
        for node in sorted(df_cluster['Node'].dropna().unique()):
            df_node = df_cluster[df_cluster['Node'] == node]
            ax.plot(df_node['Relative_Time_s'], df_node['Raft_Idx_Local'], label=f'Node {node}')

        ax.set_title(f'Raft Log Alignment - Cluster {cluster}')
        ax.set_ylabel('Raft Local Index')
        ax.legend(loc='lower right')
        
    axes[-1].set_xlabel('Tempo dall\'inizio del test (s)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '4_raft_alignment.png'))
    plt.close()

def plot_raft_recovery_dynamics(df_server, output_dir):
    """Genera una dashboard specifica per il Chaos Engineering: Replay dal disco e cambi Term."""
    clusters = sorted(df_server['Cluster'].dropna().unique())
    fig, axes = plt.subplots(len(clusters) * 3, 1, figsize=(14, 6 * len(clusters)), sharex=True)
    
    if len(clusters) == 1:
        axes = [axes[0], axes[1], axes[2]]
        
    for i, cluster in enumerate(clusters):
        df_cluster = df_server[df_server['Cluster'] == cluster]
        
        ax_ops = axes[i * 3]
        ax_idx = axes[i * 3 + 1]
        ax_term = axes[i * 3 + 2]
        
        # 1. GRAFICO DISK REPLAY (Scala Logaritmica)
        for node in sorted(df_cluster['Node'].dropna().unique()):
            df_node = df_cluster[df_cluster['Node'] == node]
            ax_ops.plot(df_node['Relative_Time_s'], df_node['Raw_Write_OPS'], label=f'Node {node}', alpha=0.8)
        
        ax_ops.set_title(f'Cluster {cluster} - Log Replay & Recovery Throughput (Log Scale)')
        ax_ops.set_ylabel('Raw OPS')
        ax_ops.set_yscale('symlog') 
        ax_ops.legend(loc='upper right')
        
        # 2. GRAFICO LOG ALIGNMENT
        for node in sorted(df_cluster['Node'].dropna().unique()):
            df_node = df_cluster[df_cluster['Node'] == node]
            ax_idx.plot(df_node['Relative_Time_s'], df_node['Raft_Idx_Local'], label=f'Node {node}', linewidth=2)
            
        ax_idx.set_title(f'Cluster {cluster} - Raft Log Index Alignment')
        ax_idx.set_ylabel('Raft Local Index')
        ax_idx.legend(loc='lower right')
        
        # 3. GRAFICO RAFT TERM (Elezioni)
        for node in sorted(df_cluster['Node'].dropna().unique()):
            df_node = df_cluster[df_cluster['Node'] == node]
            ax_term.step(df_node['Relative_Time_s'], df_node['Term'], label=f'Node {node}', where='post', alpha=0.7)
            
        ax_term.set_title(f'Cluster {cluster} - Raft Term (Elections)')
        ax_term.set_ylabel('Term Number')
        ax_term.legend(loc='lower right')

    axes[-1].set_xlabel('Tempo dall\'inizio del test (s)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '5_raft_recovery_dynamics.png'))
    plt.close()

def plot_lsm_events(base_dir, df_server, output_dir):
    all_events = []
    pattern = os.path.join(base_dir, '*', '*', 'storage_events_*.csv')
    
    for file_path in glob.glob(pattern):
        df = pd.read_csv(file_path, sep=CSV_SEP)
        df['Cluster'] = file_path.split(os.sep)[-3]
        all_events.append(df)
        
    if not all_events:
        print(f"  -> Nessun file storage_events_*.csv trovato in {base_dir}, salto il grafico LSM.")
        return
        
    df_ev = pd.concat(all_events, ignore_index=True)
    global_start_ms = df_server['Timestamp_ms'].min()
    df_ev['Relative_Time_s'] = (df_ev['timestamp'] - global_start_ms) / 1000.0
    
    # Filtra eventi fuori tempo per evitare sfasamenti
    df_ev = df_ev[df_ev['Relative_Time_s'] >= 0]
    
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
    plt.xlabel('Tempo dall\'inizio del test (s)')
    plt.ylabel('Durata (ms)')
    plt.yscale('log')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '6_lsm_storage_events.png'))
    plt.close()

if __name__ == "__main__":
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("1/6 - Caricamento e allineamento temporale dei dati...")
    df_server = load_and_resample_server_data(args.stats_dir)
    df_client = load_client_data(args.client_csv)
    
    # Sincronizzazione dell'asse X
    global_start_ms = min(df_server['Timestamp_ms'].min(), df_client['Timestamp_ms'].min())
    df_server['Relative_Time_s'] = (df_server['Timestamp_ms'] - global_start_ms) / 1000.0
    df_client['Relative_Time_s'] = (df_client['Timestamp_ms'] - global_start_ms) / 1000.0
    
    print("2/6 - Generazione grafici Chaos Engineering...")
    plot_chaos_engineering(df_server, args.output_dir)
    
    print("3/6 - Generazione grafici Client...")
    plot_client_throughput(df_client, args.output_dir)
    
    print("4/6 - Generazione grafici Latenze...")
    plot_leader_latency(df_server, args.output_dir)
    
    print("5/6 - Generazione grafici Raft e Sincronizzazione...")
    plot_raft_alignment(df_server, args.output_dir)
    plot_raft_recovery_dynamics(df_server, args.output_dir) 
    
    print("6/6 - Generazione grafici Storage (LSM)...")
    plot_lsm_events(args.stats_dir, df_server, args.output_dir)
    
    print(f"\nFinito! Dashboards salvate in: '{args.output_dir}/'")

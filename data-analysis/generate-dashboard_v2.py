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

def read_clean_csv(file_path):
    import io
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    if not lines:
        return pd.DataFrame()
    
    header = lines[0].strip().split(',')
    num_cols = len(header)
    
    clean_lines = [lines[0]]
    skipped_count = 0
    for line in lines[1:]:
        parts = line.strip().split(',')
        if len(parts) == num_cols:
            clean_lines.append(line)
        else:
            skipped_count += 1
            
    if skipped_count > 0:
        print(f"  [🛡️ SAFETY] Saltate {skipped_count} righe troncate/incomplete in {os.path.basename(file_path)}.")
        
    return pd.read_csv(io.StringIO("".join(clean_lines)), sep=CSV_SEP)

def calculate_rates_on_raw(df):
    # Ensure columns are numeric
    for col in ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes', 'Timestamp_ms']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.sort_values('Timestamp_ms').copy()
    
    # Calculate time diff in seconds
    time_diff = df['Timestamp_ms'].diff() / 1000.0
    time_diff = time_diff.replace(0, np.nan).fillna(1.0)
    
    # 1. Write OPS
    write_req_diff = df['Total_Write_Requests'].diff().fillna(0.0)
    non_zero_req = df[df['Total_Write_Requests'] > 0].index
    if len(non_zero_req) > 0:
        first_non_zero_idx = non_zero_req[0]
        write_req_diff.loc[:first_non_zero_idx] = 0.0
    df['Write_OPS'] = write_req_diff / time_diff
    df['Raw_Write_OPS'] = df['Write_OPS']
    
    # 2. Write MBps
    write_bytes_diff = df['Total_Write_Bytes'].diff().fillna(0.0)
    non_zero_bytes = df[df['Total_Write_Bytes'] > 0].index
    if len(non_zero_bytes) > 0:
        first_non_zero_idx = non_zero_bytes[0]
        write_bytes_diff.loc[:first_non_zero_idx] = 0.0
    df['Write_MBps'] = (write_bytes_diff / time_diff) / (1024 * 1024)
    
    # 3. Read OPS
    read_req_diff = df['Total_Read_Requests'].diff().fillna(0.0)
    non_zero_read_req = df[df['Total_Read_Requests'] > 0].index
    if len(non_zero_read_req) > 0:
        first_non_zero_idx = non_zero_read_req[0]
        read_req_diff.loc[:first_non_zero_idx] = 0.0
    df['Read_OPS'] = read_req_diff / time_diff
    
    # 4. Read MBps
    read_bytes_diff = df['Total_Read_Bytes'].diff().fillna(0.0)
    non_zero_read_bytes = df[df['Total_Read_Bytes'] > 0].index
    if len(non_zero_read_bytes) > 0:
        first_non_zero_idx = non_zero_read_bytes[0]
        read_bytes_diff.loc[:first_non_zero_idx] = 0.0
    df['Read_MBps'] = (read_bytes_diff / time_diff) / (1024 * 1024)
    
    return df

def load_and_resample_server_data(base_dir):
    all_dfs = []
    pattern = os.path.join(base_dir, '*', '*', 'stats_*.csv')
    
    for file_path in glob.glob(pattern):
        parts = file_path.split(os.sep)
        cluster_id, node_id = parts[-3], parts[-2]
        
        df = read_clean_csv(file_path)
        if df.empty:
            continue
        df.columns = df.columns.str.strip() 
        
        # Pre-calculate rates on the raw data
        df = calculate_rates_on_raw(df)
        
        df['Cluster'] = cluster_id
        df['Node'] = node_id
        all_dfs.append(df)
        
    if not all_dfs:
        raise ValueError(f"Nessun file stats_*.csv trovato in {base_dir}!")
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=['Cluster', 'Node', 'Timestamp_ms'])
    
    df_raw = sanitize_timestamps(df_raw, 'Timestamp_ms')
    
    timespan_s = (df_raw['Timestamp_ms'].max() - df_raw['Timestamp_ms'].min()) / 1000.0
    if timespan_s > 7200: 
        print(f"  [⚠️ WARNING] Dati sparsi su {timespan_s/3600:.2f} ore! Tronco a 1 ora per evitare Crash RAM.")
        df_raw = df_raw[df_raw['Timestamp_ms'] <= df_raw['Timestamp_ms'].min() + 3600000]

    df_raw['Datetime'] = pd.to_datetime(df_raw['Timestamp_ms'], unit='ms')
    global_min_dt = df_raw['Datetime'].min().floor('1s')
    global_max_dt = df_raw['Datetime'].max().ceil('1s')
    global_range = pd.date_range(start=global_min_dt, end=global_max_dt, freq='1s')
    resampled_nodes = []
    
    for (cluster, node), group in df_raw.groupby(['Cluster', 'Node']):
        group = group.set_index('Datetime').sort_index()
        
        # Save original timestamp to detect gaps
        group['Orig_Timestamp_ms'] = group['Timestamp_ms']
        
        # RESAMPLE and REINDEX to global range
        res = group.resample('1s').last()
        res = res.reindex(global_range)
        res.index.name = 'Datetime'
        
        # Forward fill original timestamps to detect gaps
        res['Last_Actual_Timestamp'] = res['Orig_Timestamp_ms'].ffill()
        res['Resampled_Timestamp_ms'] = res.index.values.astype('datetime64[ms]').astype(np.int64)
        
        # Detect gaps of > 3.0s and mark as offline
        res['Time_Since_Last_Log_ms'] = res['Resampled_Timestamp_ms'] - res['Last_Actual_Timestamp']
        is_offline = (res['Time_Since_Last_Log_ms'] > 3000) | res['Last_Actual_Timestamp'].isna()
        
        # Forward fill status columns
        for col in ['Role', 'Term', 'Raft_Idx_Local', 'Raft_Idx_Applied', 'Raft_Idx_Commit']:
            if col in res.columns:
                res[col] = res[col].ffill()
        
        # Forward fill cumulative counters (in case they are needed elsewhere)
        colonne_contatori = ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes', 'Total_Requests', 'Total_Bytes']
        for col in colonne_contatori:
            if col in res.columns:
                res[col] = res[col].ffill().fillna(0)
        
        # Fill rates with 0.0 or handle NaN
        rate_cols = ['Write_OPS', 'Write_MBps', 'Read_OPS', 'Read_MBps', 'Raw_Write_OPS']
        for col in rate_cols:
            if col in res.columns:
                res[col] = res[col].fillna(0.0)
        
        # Reset role and throughput counters for offline nodes
        res.loc[is_offline, 'Role'] = 'OFFLINE'
        for col in rate_cols:
            res.loc[is_offline, col] = 0.0
            
        res['Timestamp_ms'] = res['Resampled_Timestamp_ms']
        
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

def plot_cluster_leader_history(df_server, output_dir):
    clusters = sorted(df_server['Cluster'].dropna().unique())
    for cluster in clusters:
        df_cluster = df_server[df_server['Cluster'] == cluster]
        nodes = sorted(df_cluster['Node'].dropna().unique(), key=int)
        
        plt.figure(figsize=(12, len(nodes) * 1.5 + 2))
        node_to_y = {node: i + 1 for i, node in enumerate(nodes)}
        
        for node in nodes:
            df_node = df_cluster[df_cluster['Node'] == node].sort_values('Relative_Time_s')
            y_val = node_to_y[node]
            
            is_leader = df_node['Role'].str.upper() == 'LEADER'
            y_leader = np.where(is_leader, y_val, np.nan)
            
            is_follower = df_node['Role'].str.upper().isin(['FOLLOWER', 'CANDIDATE'])
            y_follower = np.where(is_follower, y_val, np.nan)
            
            plt.plot(df_node['Relative_Time_s'], y_follower, color=f'C{y_val-1}', linestyle=':', linewidth=2)
            plt.plot(df_node['Relative_Time_s'], y_leader, color=f'C{y_val-1}', linestyle='-', linewidth=4)
            
        plt.title(f'Leader History - Cluster {cluster}')
        plt.xlabel('Tempo dall\'inizio del test (s)')
        plt.ylabel('Nodi nel Cluster')
        plt.yticks(list(node_to_y.values()), [f'Node {n}' for n in nodes])
        plt.ylim(0.5, len(nodes) + 0.5)
        
        from matplotlib.lines import Line2D
        custom_lines = [
            Line2D([0], [0], color='black', linestyle='-', linewidth=4),
            Line2D([0], [0], color='black', linestyle=':', linewidth=2)
        ]
        plt.legend(custom_lines, ['LEADER (Solid Line)', 'FOLLOWER / CANDIDATE (Dotted Line)'], loc='upper right')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'cluster-{cluster}-leader-history.png'))
        plt.close()

def plot_cluster_throughput(df_server, output_dir):
    clusters = sorted(df_server['Cluster'].dropna().unique())
    for cluster in clusters:
        df_cluster = df_server[df_server['Cluster'] == cluster]
        
        leaders = df_cluster[df_cluster['Role'].str.upper() == 'LEADER']
        all_times = sorted(df_cluster['Relative_Time_s'].unique())
        cluster_throughput = leaders.groupby('Relative_Time_s')['Write_OPS'].max()
        cluster_throughput = cluster_throughput.reindex(all_times, fill_value=0.0)
        
        plt.figure(figsize=(12, 5))
        plt.plot(cluster_throughput.index, cluster_throughput.values, color='tab:blue', linewidth=2.5, label='Cluster Throughput (Leader Write OPS)')
        
        global_start_ms = df_server['Timestamp_ms'].min()
        test_end_time = df_cluster['Relative_Time_s'].max()
        
        for i, node in enumerate(sorted(df_cluster['Node'].dropna().unique(), key=int)):
            df_node = df_cluster[df_cluster['Node'] == node].sort_values('Relative_Time_s')
            is_offline = df_node['Role'] == 'OFFLINE'
            transitions = (~is_offline) & (is_offline.shift(-1) == True)
            stop_rows = df_node[transitions]
            
            for _, row in stop_rows.iterrows():
                stop_ts_ms = row['Last_Actual_Timestamp']
                stop_ts_s = (stop_ts_ms - global_start_ms) / 1000.0
                if stop_ts_s < test_end_time - 5.0:
                    plt.axvline(x=stop_ts_s, color=f'C{i}', linestyle='--', alpha=0.7, linewidth=1.5)
                    max_y = max(cluster_throughput.values) if len(cluster_throughput.values) > 0 else 1.0
                    if pd.isna(max_y) or max_y == 0: max_y = 1.0
                    plt.text(stop_ts_s, max_y * (0.8 - i * 0.1), f'Node {node} Stopped', color=f'C{i}', rotation=90, va='top', ha='right', fontsize=9)
                    
                    nearest_t_idx = np.abs(cluster_throughput.index - stop_ts_s).argmin()
                    nearest_t = cluster_throughput.index[nearest_t_idx]
                    y_val = cluster_throughput.loc[nearest_t]
                    plt.scatter(stop_ts_s, y_val, color=f'C{i}', marker='X', zorder=5, s=150, label=f'Node {node} Stop Event' if i == 0 else "")
                    
        plt.title(f'Cluster {cluster} Throughput & Crash Events')
        plt.xlabel('Tempo dall\'inizio del test (s)')
        plt.ylabel('Write OPS')
        plt.legend(loc='upper right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'cluster-{cluster}-throughput.png'))
        plt.close()

def plot_cluster_raft_alignment(df_server, output_dir):
    clusters = sorted(df_server['Cluster'].dropna().unique())
    for cluster in clusters:
        df_cluster = df_server[df_server['Cluster'] == cluster]
        
        plt.figure(figsize=(12, 5))
        for node in sorted(df_cluster['Node'].dropna().unique(), key=int):
            df_node = df_cluster[df_cluster['Node'] == node].sort_values('Relative_Time_s')
            plt.plot(df_node['Relative_Time_s'], df_node['Raft_Idx_Local'], label=f'Node {node}')
            
        plt.title(f'Raft Log Alignment - Cluster {cluster}')
        plt.xlabel('Tempo dall\'inizio del test (s)')
        plt.ylabel('Raft Local Index')
        plt.legend(loc='lower right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'cluster-{cluster}-raft-alignment.png'))
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
    
    print("Generazione grafici TODO.md (leader history, cluster throughput, raft alignment)...")
    plot_cluster_leader_history(df_server, args.output_dir)
    plot_cluster_throughput(df_server, args.output_dir)
    plot_cluster_raft_alignment(df_server, args.output_dir)
    
    print(f"\nFinito! Dashboards salvate in: '{args.output_dir}/'")

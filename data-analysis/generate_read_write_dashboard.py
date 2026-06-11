import os
import glob
import argparse
import io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Graphic theme
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_context("notebook", font_scale=1.1)
CSV_SEP = ','

def parse_args():
    parser = argparse.ArgumentParser(description="Dedicated Dashboard for Read-Heavy & Mixed Read-Write Performance Analysis")
    parser.add_argument('-s', '--stats-dir', type=str, required=True, help="Path to aggregated server stats folder")
    parser.add_argument('-cr', '--client-read-csv', type=str, required=True, help="Path to client read throughput CSV")
    parser.add_argument('-cw', '--client-write-csv', type=str, required=False, help="Path to client write throughput CSV")
    parser.add_argument('-o', '--output-dir', type=str, default='read_write_output', help="Output folder for plots")
    return parser.parse_args()

def sanitize_timestamps(df, col='Timestamp_ms'):
    min_valid_ms = 1577836800000  # 2020-01-01
    max_valid_ms = 2524608000000  # 2050-01-01
    initial_len = len(df)
    df = df[(df[col] > min_valid_ms) & (df[col] < max_valid_ms)].copy()
    dropped = initial_len - len(df)
    if dropped > 0:
        print(f"  [🛡️ SAFETY] Removed {dropped} rows with abnormal timestamps.")
    return df

def load_client_csv(file_path, op_type='read'):
    if not file_path or not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path, sep=CSV_SEP)
    df.columns = df.columns.str.strip()
    
    # Check timestamp unit (seconds vs milliseconds)
    if df['Timestamp'].mean() < 3000000000:
        df['Timestamp_ms'] = df['Timestamp'] * 1000
    else:
        df['Timestamp_ms'] = df['Timestamp']
        
    df = df.sort_values('Timestamp_ms').copy()
    df = sanitize_timestamps(df, 'Timestamp_ms')
    
    # Calculate time diff in seconds
    time_diff = df['Timestamp_ms'].diff() / 1000.0
    time_diff = time_diff.replace(0, np.nan).fillna(1.0)
    
    col_ops = 'QPS' if 'QPS' in df.columns else ('OPS' if 'OPS' in df.columns else 'QPS')
    col_mbps = 'MBps' if 'MBps' in df.columns else 'MBps'
    
    # Detect if the column is cumulative.
    is_cumulative = False
    if len(df) > 5:
        diffs = df[col_ops].diff().dropna()
        if (diffs >= 0).sum() / len(diffs) > 0.9:  # 90% of diffs are non-negative
            initial_val = df[col_ops].iloc[0]
            final_val = df[col_ops].iloc[-1]
            if final_val > initial_val * 3 and initial_val > 0:
                is_cumulative = True
                
    if is_cumulative:
        print(f"  [ℹ️ DETECTED] Column '{col_ops}' in {os.path.basename(file_path)} is CUMULATIVE. Converting to instantaneous rates.")
        df['OPS'] = (df[col_ops].diff().fillna(df[col_ops].iloc[0])) / time_diff
        df['MBps'] = (df[col_mbps].diff().fillna(df[col_mbps].iloc[0])) / time_diff
    else:
        print(f"  [ℹ️ DETECTED] Column '{col_ops}' in {os.path.basename(file_path)} is INSTANTANEOUS.")
        df['OPS'] = df[col_ops]
        df['MBps'] = df[col_mbps]
        
    return df


def read_clean_csv(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    if not lines:
        return pd.DataFrame()
    
    header = lines[0].strip().split(',')
    num_cols = len(header)
    
    clean_lines = [lines[0]]
    skipped = 0
    for line in lines[1:]:
        parts = line.strip().split(',')
        if len(parts) == num_cols:
            clean_lines.append(line)
        else:
            skipped += 1
    if skipped > 0:
        print(f"  [🛡️ SAFETY] Skipped {skipped} truncated/incomplete rows in {os.path.basename(file_path)}.")
    return pd.read_csv(io.StringIO("".join(clean_lines)), sep=CSV_SEP)

def calculate_rates_on_raw(df):
    for col in ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes', 'Timestamp_ms']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.sort_values('Timestamp_ms').copy()
    time_diff = df['Timestamp_ms'].diff() / 1000.0
    time_diff = time_diff.replace(0, np.nan).fillna(1.0)
    
    # Write OPS & MB/s
    write_req_diff = df['Total_Write_Requests'].diff().fillna(0.0)
    non_zero_write = df[df['Total_Write_Requests'] > 0].index
    if len(non_zero_write) > 0:
        write_req_diff.loc[:non_zero_write[0]] = 0.0
    df['Write_OPS'] = write_req_diff / time_diff
    
    write_bytes_diff = df['Total_Write_Bytes'].diff().fillna(0.0)
    if len(non_zero_write) > 0:
        write_bytes_diff.loc[:non_zero_write[0]] = 0.0
    df['Write_MBps'] = (write_bytes_diff / time_diff) / (1024 * 1024)
    
    # Read OPS & MB/s
    read_req_diff = df['Total_Read_Requests'].diff().fillna(0.0)
    non_zero_read = df[df['Total_Read_Requests'] > 0].index
    if len(non_zero_read) > 0:
        read_req_diff.loc[:non_zero_read[0]] = 0.0
    df['Read_OPS'] = read_req_diff / time_diff
    
    read_bytes_diff = df['Total_Read_Bytes'].diff().fillna(0.0)
    if len(non_zero_read) > 0:
        read_bytes_diff.loc[:non_zero_read[0]] = 0.0
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
        df = calculate_rates_on_raw(df)
        df['Cluster'] = cluster_id
        df['Node'] = node_id
        all_dfs.append(df)
        
    if not all_dfs:
        raise ValueError(f"No stats_*.csv files found in {base_dir}!")
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=['Cluster', 'Node', 'Timestamp_ms'])
    df_raw = sanitize_timestamps(df_raw, 'Timestamp_ms')
    
    # Reindex & Resample to 1 second
    df_raw['Datetime'] = pd.to_datetime(df_raw['Timestamp_ms'], unit='ms')
    global_min_dt = df_raw['Datetime'].min().floor('1s')
    global_max_dt = df_raw['Datetime'].max().ceil('1s')
    global_range = pd.date_range(start=global_min_dt, end=global_max_dt, freq='1s')
    
    resampled_nodes = []
    for (cluster, node), group in df_raw.groupby(['Cluster', 'Node']):
        group = group.set_index('Datetime').sort_index()
        group['Orig_Timestamp_ms'] = group['Timestamp_ms']
        
        res = group.resample('1s').last()
        res = res.reindex(global_range)
        res.index.name = 'Datetime'
        
        res['Last_Actual_Timestamp'] = res['Orig_Timestamp_ms'].ffill()
        res['Resampled_Timestamp_ms'] = res.index.values.astype('datetime64[ms]').astype(np.int64)
        
        is_offline = ((res['Resampled_Timestamp_ms'] - res['Last_Actual_Timestamp']) > 3000) | res['Last_Actual_Timestamp'].isna()
        
        # Forward fill statuses and counters
        cols_to_fill = ['Role', 'Term', 'Raft_Idx_Local', 'Raft_Idx_Applied', 'Raft_Idx_Commit',
                        'Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes',
                        'P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms', 'Avg_Latency_ms']
        for col in cols_to_fill:
            if col in res.columns:
                res[col] = res[col].ffill().fillna(0)
                
        rate_cols = ['Write_OPS', 'Write_MBps', 'Read_OPS', 'Read_MBps']
        for col in rate_cols:
            if col in res.columns:
                res[col] = res[col].fillna(0.0)
                
        res.loc[is_offline, 'Role'] = 'OFFLINE'
        for col in rate_cols:
            res.loc[is_offline, col] = 0.0
            
        res['Timestamp_ms'] = res['Resampled_Timestamp_ms']
        res['Cluster'] = cluster
        res['Node'] = node
        resampled_nodes.append(res.reset_index())
        
    return pd.concat(resampled_nodes, ignore_index=True)

def load_all_storage_events(stats_dir, global_start_ms):
    all_events = []
    pattern = os.path.join(stats_dir, '*', '*', 'storage_events_*.csv')
    
    for file_path in glob.glob(pattern):
        try:
            df = pd.read_csv(file_path, sep=CSV_SEP)
            if df.empty:
                continue
            df.columns = df.columns.str.strip()
            df['Cluster'] = file_path.split(os.sep)[-3]
            df['Node'] = file_path.split(os.sep)[-2]
            all_events.append(df)
        except Exception as e:
            print(f"  Error loading storage events {file_path}: {e}")
            
    if not all_events:
        return pd.DataFrame()
        
    df_ev = pd.concat(all_events, ignore_index=True)
    df_ev['Relative_Time_s'] = (df_ev['timestamp'] - global_start_ms) / 1000.0
    return df_ev

def generate_plots(df_server, df_read, df_write, df_events, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Plot: Client Read & Write Throughput Over Time
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(df_read['Relative_Time_s'], df_read['OPS'], color='tab:blue', linewidth=2.5, label='Client Read QPS')
    ax1.set_xlabel('Time from test start (s)')
    ax1.set_ylabel('Read QPS', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    
    if df_write is not None and not df_write.empty:
        ax2 = ax1.twinx()
        ax2.plot(df_write['Relative_Time_s'], df_write['OPS'], color='tab:red', linewidth=2.0, linestyle='--', label='Client Write OPS')
        ax2.set_ylabel('Write OPS', color='tab:red')
        ax2.tick_params(axis='y', labelcolor='tab:red')
        
        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    else:
        ax1.legend(loc='upper left')
        
    plt.title('Client Read and Write Throughput (Mixed Workload)')
    fig.tight_layout()
    plt.savefig(os.path.join(output_dir, 'client_read_write_throughput.png'), dpi=150)
    plt.close()
    
    # 2. Plot: Server-Side Aggregated Read vs Write OPS
    # Sum across all nodes at each timestamp
    server_agg = df_server.groupby('Relative_Time_s')[['Read_OPS', 'Write_OPS']].sum().reset_index()
    
    plt.figure(figsize=(14, 6))
    plt.plot(server_agg['Relative_Time_s'], server_agg['Read_OPS'], color='dodgerblue', linewidth=2.5, label='Server Aggregate Read OPS')
    plt.plot(server_agg['Relative_Time_s'], server_agg['Write_OPS'], color='crimson', linewidth=2.0, label='Server Aggregate Write OPS')
    plt.title('Database Server-Side Processed Read vs Write OPS')
    plt.xlabel('Time from test start (s)')
    plt.ylabel('OPS')
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'server_read_write_ops.png'), dpi=150)
    plt.close()
    
    # 3. Plot: Server Latency Profiles during Read vs Mixed Phases
    leaders_df = df_server[df_server['Role'].str.upper() == 'LEADER']
    if not leaders_df.empty:
        latency_agg = leaders_df.groupby('Relative_Time_s')[['P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms']].mean().reset_index()
    else:
        latency_agg = df_server.groupby('Relative_Time_s')[['P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms']].mean().reset_index()
        
    plt.figure(figsize=(14, 6))
    plt.plot(latency_agg['Relative_Time_s'], latency_agg['P50_Latency_ms'], color='green', linewidth=1.8, label='P50 Latency')
    plt.plot(latency_agg['Relative_Time_s'], latency_agg['P95_Latency_ms'], color='orange', linewidth=1.8, label='P95 Latency')
    plt.plot(latency_agg['Relative_Time_s'], latency_agg['P99_Latency_ms'], color='red', linewidth=1.8, label='P99 Latency')
    plt.yscale('log')
    plt.title('Database Latency Profile (P50, P95, P99) - Log Scale')
    plt.xlabel('Time from test start (s)')
    plt.ylabel('Latency (ms)')
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'server_latency_impact.png'), dpi=150)
    plt.close()
    
    # 4. Plot: LSM Activity and Client Read Performance Correlation
    fig, (ax_qps, ax_lat) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    
    # Client QPS on top panel
    ax_qps.plot(df_read['Relative_Time_s'], df_read['OPS'], color='tab:blue', linewidth=2.5, label='Read QPS')
    ax_qps.set_ylabel('Read QPS')
    ax_qps.set_title('Impact of LSM-Tree Storage Events on Read Performance')
    ax_qps.legend(loc='upper right')
    
    # Server P99 Latency on bottom panel
    ax_lat.plot(latency_agg['Relative_Time_s'], latency_agg['P99_Latency_ms'], color='crimson', linewidth=2.0, label='P99 Latency (ms)')
    ax_lat.set_ylabel('P99 Latency (ms) - Log Scale')
    ax_lat.set_xlabel('Time from test start (s)')
    ax_lat.legend(loc='upper right')
    
    # Overlay storage events as vertical lines/spans
    if df_events is not None and not df_events.empty:
        flushes = df_events[df_events['event_type'] == 'memtable_flush']
        compactions = df_events[df_events['event_type'] == 'compaction']
        
        first_flush = True
        for _, row in flushes.iterrows():
            t = row['Relative_Time_s']
            ax_qps.axvline(x=t, color='skyblue', linestyle=':', alpha=0.6, linewidth=1.2, 
                           label='Memtable Flush' if first_flush else '')
            ax_lat.axvline(x=t, color='skyblue', linestyle=':', alpha=0.6, linewidth=1.2)
            first_flush = False
            
        first_comp = True
        for _, row in compactions.iterrows():
            t_start = row['Relative_Time_s']
            t_end = t_start + (row['duration_ms'] / 1000.0)
            ax_qps.axvspan(t_start, t_end, color='lightcoral', alpha=0.25, 
                           label='LSM Compaction' if first_comp else '')
            ax_lat.axvspan(t_start, t_end, color='lightcoral', alpha=0.25)
            first_comp = False
            
        ax_qps.legend(loc='upper right')
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'lsm_impact_on_reads.png'), dpi=150)
    plt.close()

if __name__ == "__main__":
    args = parse_args()
    
    print("Loading datasets...")
    df_server = load_and_resample_server_data(args.stats_dir)
    df_read = load_client_csv(args.client_read_csv, op_type='read')
    df_write = load_client_csv(args.client_write_csv, op_type='write')
    
    # Sync X axis to global minimum timestamp
    t_min = df_server['Timestamp_ms'].min()
    if df_read is not None:
        t_min = min(t_min, df_read['Timestamp_ms'].min())
    if df_write is not None:
        t_min = min(t_min, df_write['Timestamp_ms'].min())
        
    df_server['Relative_Time_s'] = (df_server['Timestamp_ms'] - t_min) / 1000.0
    if df_read is not None:
        df_read['Relative_Time_s'] = (df_read['Timestamp_ms'] - t_min) / 1000.0
    if df_write is not None:
        df_write['Relative_Time_s'] = (df_write['Timestamp_ms'] - t_min) / 1000.0
        
    print("Loading storage events...")
    df_events = load_all_storage_events(args.stats_dir, t_min)
    
    print("Generating dedicated read-write performance plots...")
    generate_plots(df_server, df_read, df_write, df_events, args.output_dir)
    print(f"Finished! Custom plots saved to: {args.output_dir}")

import os
import re
import glob
import argparse
import io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Visual Setup
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except Exception:
    try:
        plt.style.use('seaborn-darkgrid')
    except Exception:
        sns.set_theme(style='darkgrid')
sns.set_context("notebook", font_scale=1.1)
CSV_SEP = ','

def parse_args():
    parser = argparse.ArgumentParser(description="Focused Performance Dashboard & HTML Report Generator")
    parser.add_argument('-s', '--stats-dir', type=str, required=True, help="Path to aggregated server stats folder")
    parser.add_argument('-c', '--client-csv', type=str, required=True, help="Path to client CSV (or dataset directory)")
    parser.add_argument('-o', '--output-dir', type=str, required=True, help="Output folder for plots")
    parser.add_argument('-r', '--report-file', type=str, required=True, help="Output path for the HTML report")
    parser.add_argument('-t', '--type', type=str, choices=['recovery', 'write', 'read'], required=True, help="Analysis type: recovery, write, read")
    return parser.parse_args()

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
        print(f"  [🛡️ SAFETY] Skipped {skipped} rows in {os.path.basename(file_path)}.")
    return pd.read_csv(io.StringIO("".join(clean_lines)), sep=CSV_SEP)

def sanitize_timestamps(df, col='Timestamp_ms'):
    min_valid_ms = 1577836800000  # 2020-01-01
    max_valid_ms = 2524608000000  # 2050-01-01
    initial_len = len(df)
    df = df[(df[col] > min_valid_ms) & (df[col] < max_valid_ms)].copy()
    dropped = initial_len - len(df)
    if dropped > 0:
        print(f"  [🛡️ SAFETY] Removed {dropped} rows with abnormal timestamps in {col}.")
    return df

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
    files = glob.glob(pattern)
    if not files:
        # Try recursive fallback
        pattern = os.path.join(base_dir, '**', 'stats_*.csv')
        files = glob.glob(pattern, recursive=True)
        
    for file_path in files:
        parts = file_path.split(os.sep)
        cluster_id, node_id = parts[-3], parts[-2]
        
        df = read_clean_csv(file_path)
        if df.empty:
            continue
        df.columns = df.columns.str.strip()
        df = calculate_rates_on_raw(df)
        df['Cluster'] = cluster_id
        df['Node'] = node_id
        df['File_Path'] = file_path
        all_dfs.append(df)
        
    if not all_dfs:
        raise ValueError(f"No stats_*.csv files found in {base_dir}!")
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=['Cluster', 'Node', 'Timestamp_ms'])
    df_raw = sanitize_timestamps(df_raw, 'Timestamp_ms')
    
    df_raw['Datetime'] = pd.to_datetime(df_raw['Timestamp_ms'], unit='ms')
    global_min_dt = df_raw['Datetime'].min().floor('1s')
    global_max_dt = df_raw['Datetime'].max().ceil('1s')
    global_range = pd.date_range(start=global_min_dt, end=global_max_dt, freq='1s')
    
    resampled_nodes = []
    for (cluster, node), group in df_raw.groupby(['Cluster', 'Node']):
        node_files = group['File_Path'].dropna().unique() if 'File_Path' in group.columns else []
        file_ranges = []
        for fp in node_files:
            file_df = group[group['File_Path'] == fp]
            if not file_df.empty:
                f_min = file_df['Timestamp_ms'].min()
                f_max = file_df['Timestamp_ms'].max()
                file_ranges.append((f_min, f_max))

        group = group.set_index('Datetime').sort_index()
        group['Orig_Timestamp_ms'] = group['Timestamp_ms']
        
        res = group.resample('1s').last()
        res = res.reindex(global_range)
        res.index.name = 'Datetime'
        
        res['Last_Actual_Timestamp'] = res['Orig_Timestamp_ms'].ffill()
        res['Resampled_Timestamp_ms'] = res.index.values.astype('datetime64[ms]').astype(np.int64)
        
        ts_series = res['Resampled_Timestamp_ms']
        is_offline = pd.Series(True, index=res.index)
        for f_min, f_max in file_ranges:
            is_offline = is_offline & ~((ts_series >= f_min) & (ts_series <= f_max))
        
        cols_to_fill = ['Role', 'Term', 'Raft_Idx_Local', 'Raft_Idx_Applied', 'Raft_Idx_Commit',
                        'Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes',
                        'P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms', 'Avg_Latency_ms',
                        'PendingRequests', 'PendingBytes', 'Backlog']
        for col in cols_to_fill:
            if col in res.columns:
                res[col] = res[col].ffill().fillna(0)
                
        # Ensure Raft indices are monotonically ascending
        for col in ['Raft_Idx_Local', 'Raft_Idx_Applied', 'Raft_Idx_Commit']:
            if col in res.columns:
                res[col] = res[col].cummax()

                
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
    global_start_ms = df_raw['Timestamp_ms'].min()
    return pd.concat(resampled_nodes, ignore_index=True), global_start_ms



def load_client_csv(file_path, op_type='write'):
    if not file_path or not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path, sep=CSV_SEP)
    df.columns = df.columns.str.strip()
    
    if df['Timestamp'].mean() < 3000000000:
        df['Timestamp_ms'] = df['Timestamp'] * 1000
    else:
        df['Timestamp_ms'] = df['Timestamp']
        
    df = df.sort_values('Timestamp_ms').copy()
    df = sanitize_timestamps(df, 'Timestamp_ms')
    
    time_diff = df['Timestamp_ms'].diff() / 1000.0
    time_diff = time_diff.replace(0, np.nan).fillna(1.0)
    
    col_ops = 'OPS' if 'OPS' in df.columns else ('QPS' if 'QPS' in df.columns else 'OPS')
    col_mbps = 'MBps' if 'MBps' in df.columns else 'MBps'
    
    # Detect if cumulative
    is_cumulative = False
    if len(df) > 5:
        diffs = df[col_ops].diff().dropna()
        if (diffs >= 0).sum() / len(diffs) > 0.8:
            non_zero = df[df[col_ops] > 0][col_ops]
            if not non_zero.empty and non_zero.iloc[-1] > non_zero.iloc[0] * 1.5:
                is_cumulative = True
                
    if is_cumulative:
        df['Instantaneous_OPS'] = (df[col_ops].diff().fillna(df[col_ops].iloc[0])) / time_diff
        df['Instantaneous_MBps'] = (df[col_mbps].diff().fillna(df[col_mbps].iloc[0])) / time_diff
    else:
        df['Instantaneous_OPS'] = df[col_ops]
        df['Instantaneous_MBps'] = df[col_mbps]
        
    return df

def group_files_into_sessions(file_paths, max_gap_s=15):
    if not file_paths:
        return []
    
    file_starts = []
    for f in file_paths:
        try:
            df = pd.read_csv(f, sep=CSV_SEP, nrows=5)
            if df.empty:
                continue
            df.columns = df.columns.str.strip()
            t_col = 'Timestamp' if 'Timestamp' in df.columns else 'Timestamp_ms'
            if t_col in df.columns:
                mean_t = df[t_col].iloc[0]
                if mean_t < 3000000000:
                    start_ms = mean_t * 1000
                else:
                    start_ms = mean_t
                file_starts.append((start_ms, f))
        except Exception as e:
            print(f"  [⚠️ WARNING] Error reading start timestamp of {f}: {e}")
            
    if not file_starts:
        return []
        
    file_starts.sort(key=lambda x: x[0])
    
    sessions = []
    current_session = [file_starts[0][1]]
    current_start = file_starts[0][0]
    
    for start_ms, f in file_starts[1:]:
        if (start_ms - current_start) / 1000.0 <= max_gap_s:
            current_session.append(f)
        else:
            sessions.append(current_session)
            current_session = [f]
            current_start = start_ms
    sessions.append(current_session)
    return sessions

def load_and_aggregate_client_files(file_list, op_type='write'):
    if not file_list:
        return None
        
    loaded_dfs = []
    for f in file_list:
        df = load_client_csv(f, op_type)
        if df is not None and not df.empty:
            loaded_dfs.append(df)
            
    if not loaded_dfs:
        return None
        
    if len(loaded_dfs) == 1:
        return loaded_dfs[0]
        
    # Align to 1-second ticks
    min_ts = min(df['Timestamp_ms'].min() for df in loaded_dfs)
    max_ts = max(df['Timestamp_ms'].max() for df in loaded_dfs)
    
    global_range = pd.date_range(
        start=pd.to_datetime(min_ts, unit='ms').floor('1s'),
        end=pd.to_datetime(max_ts, unit='ms').ceil('1s'),
        freq='1s'
    )
    
    resampled_dfs = []
    for df in loaded_dfs:
        df = df.copy()
        df['Datetime'] = pd.to_datetime(df['Timestamp_ms'], unit='ms')
        df = df.set_index('Datetime').sort_index()
        
        # Resample to 1-second ticks
        res = df.resample('1s').mean()
        res = res.reindex(global_range)
        res.index.name = 'Datetime'
        
        res['Instantaneous_OPS'] = res['Instantaneous_OPS'].fillna(0.0)
        res['Instantaneous_MBps'] = res['Instantaneous_MBps'].fillna(0.0)
        
        for col in ['Avg_Latency_ms', 'P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms']:
            if col in res.columns:
                res[col] = res[col].ffill().bfill().fillna(0.0)
                
        res['Timestamp_ms'] = res.index.values.astype('datetime64[ms]').astype(np.int64)
        resampled_dfs.append(res)
        
    # Aggregate resampled DataFrames
    agg_df = pd.DataFrame(index=global_range)
    agg_df.index.name = 'Datetime'
    agg_df['Timestamp_ms'] = agg_df.index.values.astype('datetime64[ms]').astype(np.int64)
    
    agg_df['Instantaneous_OPS'] = sum(df['Instantaneous_OPS'] for df in resampled_dfs)
    agg_df['Instantaneous_MBps'] = sum(df['Instantaneous_MBps'] for df in resampled_dfs)
    
    for col in ['Avg_Latency_ms', 'P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms']:
        cols_present = [df[col] for df in resampled_dfs if col in df.columns]
        if cols_present:
            agg_df[col] = pd.concat(cols_present, axis=1).mean(axis=1).fillna(0.0)
            
    for col in ['Total_ACKed_Records', 'Total_ACKed_Bytes']:
        cols_present = [df[col] for df in resampled_dfs if col in df.columns]
        if cols_present:
            agg_df[col] = pd.concat(cols_present, axis=1).sum(axis=1)
            
    return agg_df.reset_index()


def load_storage_events(stats_dir):
    all_events = []
    pattern = os.path.join(stats_dir, '*', '*', 'storage_events_*.csv')
    files = glob.glob(pattern)
    if not files:
        pattern = os.path.join(stats_dir, '**', 'storage_events_*.csv')
        files = glob.glob(pattern, recursive=True)
        
    for file_path in files:
        try:
            df = pd.read_csv(file_path, sep=CSV_SEP)
            if df.empty:
                continue
            df.columns = df.columns.str.strip()
            df['Cluster'] = file_path.split(os.sep)[-3]
            df['Node'] = file_path.split(os.sep)[-2]
            all_events.append(df)
        except Exception as e:
            print(f"  [⚠️ WARNING] Error loading storage events {file_path}: {e}")
            
    if not all_events:
        return pd.DataFrame()
    return pd.concat(all_events, ignore_index=True)

def apply_theme(fig, ax, theme='dark'):
    if theme == 'dark':
        fig.patch.set_facecolor('#0d1117')
        ax.set_facecolor('#161b22')
        ax.spines['bottom'].set_color('#30363d')
        ax.spines['top'].set_color('#30363d')
        ax.spines['left'].set_color('#30363d')
        ax.spines['right'].set_color('#30363d')
        ax.tick_params(colors='#c9d1d9', which='both')
        ax.yaxis.label.set_color('#c9d1d9')
        ax.xaxis.label.set_color('#c9d1d9')
        ax.title.set_color('#58a6ff')
        ax.grid(True, color='#30363d', linestyle='--', alpha=0.3)
    else:
        fig.patch.set_facecolor('#ffffff')
        ax.set_facecolor('#f6f8fa')
        ax.spines['bottom'].set_color('#d0d7de')
        ax.spines['top'].set_color('#d0d7de')
        ax.spines['left'].set_color('#d0d7de')
        ax.spines['right'].set_color('#d0d7de')
        ax.tick_params(colors='#24292f', which='both')
        ax.yaxis.label.set_color('#24292f')
        ax.xaxis.label.set_color('#24292f')
        ax.title.set_color('#0969da')
        ax.grid(True, color='#d0d7de', linestyle='--', alpha=0.5)

def calculate_node_load(df_server, t_start, t_end, op_type='write'):
    col_req = 'Total_Write_Requests' if op_type == 'write' else 'Total_Read_Requests'
    col_bytes = 'Total_Write_Bytes' if op_type == 'write' else 'Total_Read_Bytes'
    
    results = []
    df_session = df_server[(df_server['Timestamp_ms'] >= t_start) & (df_server['Timestamp_ms'] <= t_end)]
    if df_session.empty:
        return pd.DataFrame()
        
    for (cluster, node), group in df_session.groupby(['Cluster', 'Node']):
        group = group.sort_values('Timestamp_ms')
        req_start = group[col_req].iloc[0] if len(group) > 0 else 0
        req_end = group[col_req].iloc[-1] if len(group) > 0 else 0
        req_processed = max(0, req_end - req_start)
        
        bytes_start = group[col_bytes].iloc[0] if len(group) > 0 else 0
        bytes_end = group[col_bytes].iloc[-1] if len(group) > 0 else 0
        bytes_processed = max(0, bytes_end - bytes_start)
        
        role_counts = group['Role'].value_counts()
        avg_role = role_counts.index[0] if not role_counts.empty else 'UNKNOWN'
        
        avg_backlog = group['Backlog'].mean() if 'Backlog' in group.columns else 0.0
        avg_pending = group['PendingRequests'].mean() if 'PendingRequests' in group.columns else 0.0
        avg_latency = group['Avg_Latency_ms'].mean() if 'Avg_Latency_ms' in group.columns else 0.0
        p95_latency = group['P95_Latency_ms'].mean() if 'P95_Latency_ms' in group.columns else 0.0
        
        results.append({
            'Cluster': cluster,
            'Node': node,
            'Role': avg_role,
            'Requests_Processed': req_processed,
            'Bytes_Processed_MB': bytes_processed / (1024 * 1024),
            'Avg_Backlog': avg_backlog,
            'Avg_Pending_Reqs': avg_pending,
            'Avg_Latency_ms': avg_latency,
            'P95_Latency_ms': p95_latency
        })
    return pd.DataFrame(results)

# HTML Style Template Helper
def get_html_header(title, subtype, stats_dir, client_csv):
    return f"""<!DOCTYPE html>
<html data-theme="dark">
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-color: #f6f8fa;
            --container-bg: #ffffff;
            --card-bg: #ffffff;
            --text-color: #24292f;
            --text-muted: #57606a;
            --border-color: #d0d7de;
            --primary-color: #0969da;
            --primary-gradient: linear-gradient(135deg, #0969da, #1f6feb);
            --header-color: #0969da;
            --table-header-bg: #f6f8fa;
            --table-hover-bg: #eaeef2;
            --badge-leader-bg: rgba(45, 164, 78, 0.15);
            --badge-leader-text: #1a7f37;
            --badge-follower-bg: rgba(240, 136, 62, 0.15);
            --badge-follower-text: #bc4c00;
            --badge-offline-bg: rgba(207, 34, 46, 0.15);
            --badge-offline-text: #cf222e;
            --card-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
        }}

        [data-theme="dark"] {{
            --bg-color: #0d1117;
            --container-bg: #161b22;
            --card-bg: #1e2530;
            --text-color: #c9d1d9;
            --text-muted: #8b949e;
            --border-color: #30363d;
            --primary-color: #58a6ff;
            --primary-gradient: linear-gradient(135deg, #1f6feb, #388bfd);
            --header-color: #58a6ff;
            --table-header-bg: #21262d;
            --table-hover-bg: #28303d;
            --badge-leader-bg: rgba(63, 185, 80, 0.15);
            --badge-leader-text: #3fb950;
            --badge-follower-bg: rgba(240, 136, 62, 0.15);
            --badge-follower-text: #f0883e;
            --badge-offline-bg: rgba(248, 81, 73, 0.15);
            --badge-offline-text: #f85149;
            --card-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            margin: 0;
            padding: 30px;
            transition: background-color 0.3s ease, color 0.3s ease;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: var(--container-bg);
            padding: 30px;
            border-radius: 16px;
            box-shadow: var(--card-shadow);
            border: 1px solid var(--border-color);
            transition: background 0.3s ease, border-color 0.3s ease, box-shadow 0.3s ease;
        }}
        header {{
            position: relative;
            border-bottom: 2px solid var(--border-color);
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        h1 {{
            font-family: 'Outfit', sans-serif;
            font-weight: 700;
            font-size: 2.2em;
            background: var(--primary-gradient);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0 0 10px 0;
        }}
        h2 {{
            font-family: 'Outfit', sans-serif;
            font-weight: 600;
            color: var(--header-color);
            margin-top: 45px;
            margin-bottom: 15px;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 8px;
        }}
        .subtitle {{
            color: var(--text-muted);
            font-size: 1.1em;
            margin: 0;
            font-weight: 500;
        }}
        .meta-box {{
            margin-top: 15px;
            font-size: 0.9em;
            color: var(--text-muted);
            background: var(--table-header-bg);
            padding: 10px 15px;
            border-radius: 8px;
            border: 1px solid var(--border-color);
            display: inline-block;
        }}
        .btn-theme {{
            position: absolute;
            top: 5px;
            right: 0;
            padding: 8px 16px;
            border-radius: 8px;
            border: 1px solid var(--border-color);
            background-color: var(--card-bg);
            color: var(--text-color);
            cursor: pointer;
            font-weight: 600;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            transition: all 0.2s ease;
        }}
        .btn-theme:hover {{
            background-color: var(--table-hover-bg);
            border-color: var(--text-muted);
        }}
        
        /* Dashboard Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 35px;
        }}
        .stat-card {{
            background: var(--card-bg);
            padding: 20px;
            border-radius: 12px;
            border: 1px solid var(--border-color);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.02);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .stat-card:hover {{
            transform: translateY(-2px);
            box-shadow: var(--card-shadow);
        }}
        .stat-label {{
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 8px;
            font-weight: 600;
        }}
        .stat-value {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.8em;
            font-weight: 700;
            color: var(--text-color);
        }}
        .stat-desc {{
            font-size: 0.8em;
            color: var(--text-muted);
            margin-top: 5px;
        }}

        /* Table Styles */
        table {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            margin: 20px 0;
            font-size: 14px;
            border-radius: 10px;
            overflow: hidden;
            border: 1px solid var(--border-color);
        }}
        th, td {{
            padding: 14px 18px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{
            background-color: var(--table-header-bg);
            color: var(--primary-color);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }}
        tr:last-child td {{
            border-bottom: none;
        }}
        tr:hover td {{
            background-color: var(--table-hover-bg);
        }}

        /* Badges */
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .badge-leader {{
            background-color: var(--badge-leader-bg);
            color: var(--badge-leader-text);
        }}
        .badge-follower {{
            background-color: var(--badge-follower-bg);
            color: var(--badge-follower-text);
        }}
        .badge-offline {{
            background-color: var(--badge-offline-bg);
            color: var(--badge-offline-text);
        }}

        /* Plot Panels */
        .plot-panel {{
            background: var(--card-bg);
            padding: 24px;
            border-radius: 12px;
            border: 1px solid var(--border-color);
            margin-bottom: 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.02);
        }}
        .plot-panel h3 {{
            margin-top: 0;
            margin-bottom: 15px;
            font-family: 'Outfit', sans-serif;
            color: var(--text-color);
            font-size: 1.25em;
        }}
        .plot-container {{
            text-align: center;
            margin: 15px 0;
        }}
        .plot-image {{
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            border: 1px solid var(--border-color);
        }}
        .plot-description {{
            font-size: 0.95em;
            color: var(--text-muted);
            margin-top: 15px;
            line-height: 1.5;
        }}

        .insight-box {{
            background-color: rgba(9, 105, 218, 0.05);
            border-left: 4px solid var(--primary-color);
            padding: 16px 20px;
            border-radius: 0 12px 12px 0;
            margin: 25px 0;
            line-height: 1.6;
            font-size: 0.95em;
        }}
        [data-theme="dark"] .insight-box {{
            background-color: rgba(56, 139, 253, 0.08);
        }}

        /* Cluster highlight card */
        .cluster-banner {{
            display: flex;
            align-items: center;
            background: var(--table-header-bg);
            padding: 15px 20px;
            border-radius: 10px;
            border: 1px solid var(--border-color);
            margin-bottom: 25px;
        }}
        .cluster-badge {{
            background: var(--primary-gradient);
            color: white;
            font-weight: 700;
            padding: 6px 12px;
            border-radius: 6px;
            margin-right: 15px;
            font-family: 'Outfit', sans-serif;
        }}
        .cluster-info {{
            font-size: 0.95em;
            color: var(--text-color);
        }}

        .timeline-container {{
            position: relative;
            margin: 20px 0;
            padding: 10px;
            background: var(--table-header-bg);
            border-radius: 8px;
            border: 1px solid var(--border-color);
            overflow-x: auto;
        }}

        /* Helper Styles */
        .text-right {{ text-align: right; }}
        .text-center {{ text-align: center; }}
        .semibold {{ font-weight: 600; }}
        .success-text {{ color: var(--badge-leader-text); }}
        .danger-text {{ color: var(--badge-offline-text); }}
        .warning-text {{ color: var(--badge-follower-text); }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{title}</h1>
            <p class="subtitle">{subtype}</p>
            <div class="meta-box">
                <strong>Server Stats:</strong> <code>{os.path.basename(stats_dir)}</code> &nbsp;|&nbsp; 
                <strong>Client Source:</strong> <code>{os.path.basename(client_csv)}</code>
            </div>
            <button class="btn-theme" onclick="toggleTheme()">🌓 Light/Dark</button>
        </header>
"""

def get_html_footer():
    return """
        <footer>
            <div style="text-align: center; margin-top: 50px; font-size: 0.85em; color: var(--text-muted); border-top: 1px solid var(--border-color); padding-top: 20px;">
                Generated by Database Performance Dashboard Service &copy; 2026. All rights reserved.
            </div>
        </footer>
    </div>

    <script>
        function toggleTheme() {
            const htmlDoc = document.documentElement;
            const currentTheme = htmlDoc.getAttribute('data-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            htmlDoc.setAttribute('data-theme', newTheme);
            localStorage.setItem('dashboard-theme', newTheme);
            updatePlotImages(newTheme);
        }

        function updatePlotImages(theme) {
            const images = document.querySelectorAll('.plot-image');
            images.forEach(img => {
                const src = img.getAttribute('src');
                if (!src) return;
                if (theme === 'dark') {
                    img.setAttribute('src', src.replace('_light.png', '_dark.png'));
                } else {
                    img.setAttribute('src', src.replace('_dark.png', '_light.png'));
                }
            });
        }

        // Initialize theme on load
        window.addEventListener('DOMContentLoaded', () => {
            const savedTheme = localStorage.getItem('dashboard-theme') || 'dark';
            document.documentElement.setAttribute('data-theme', savedTheme);
            updatePlotImages(savedTheme);
        });
    </script>
</body>
</html>
"""

# =====================================================================
# 1. RECOVERY ANALYSIS MODE
# =====================================================================
def run_recovery_mode(args):
    print("=== Phase: Recovery Analysis ===")
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load and resample server data
    try:
        df_server, global_start_ms = load_and_resample_server_data(args.stats_dir)
    except ValueError as e:
        print(f"  [⚠️ WARNING] Server stats not found: {e}. Skipping Recovery Analysis.")
        return
    df_server['Relative_Time_s'] = (df_server['Timestamp_ms'] - global_start_ms) / 1000.0

    client_dir = args.client_csv if os.path.isdir(args.client_csv) else os.path.dirname(args.client_csv)
    client_files = sorted(glob.glob(os.path.join(client_dir, 'client_throughput_*.csv')))
    if not client_files and os.path.isfile(args.client_csv):
        client_files = [args.client_csv]
    df_client = load_and_aggregate_client_files(client_files, op_type='write')

    
    if df_client is not None:
        df_client['Relative_Time_s'] = (df_client['Timestamp_ms'] - global_start_ms) / 1000.0
    
    # 1. Extract crash events
    crash_events = []
    global_max_ts = df_server['Timestamp_ms'].max()
    stats_dir_lower = args.stats_dir.lower()
    is_performance_test = 'writes' in stats_dir_lower or 'reads' in stats_dir_lower

    for (cluster, node), group in df_server.groupby(['Cluster', 'Node']):
        group = group.sort_values('Relative_Time_s')
        non_offline_group = group[group['Role'] != 'OFFLINE']
        if non_offline_group.empty:
            continue
        last_file_max_ts = non_offline_group['Timestamp_ms'].max()
        
        role_prev = group['Role'].shift(1)
        is_transition = (group['Role'] == 'OFFLINE') & (role_prev != 'OFFLINE') & (role_prev.notna())
        transitions = group[is_transition]
        for idx, row in transitions.iterrows():
            prev_role = role_prev.loc[idx]
            t_crash_ms = row['Timestamp_ms']
            is_end_of_last_file = t_crash_ms >= last_file_max_ts
            if is_end_of_last_file:
                if is_performance_test:
                    continue
                if (global_max_ts - t_crash_ms) <= 15000:
                    continue
                    
            crash_events.append({
                'Cluster': cluster,
                'Node': node,
                'Time_s': row['Relative_Time_s'],
                'Role_Before': prev_role,
                'Timestamp_ms': row['Timestamp_ms']
            })
    df_crashes = pd.DataFrame(crash_events)
    
    # 2. Calculate failovers
    failover_events = []
    if not df_crashes.empty:
        leaders_df = df_server[df_server['Role'] == 'LEADER'].copy()
        leader_crashes = df_crashes[df_crashes['Role_Before'] == 'LEADER'].copy()
        for _, crash in leader_crashes.iterrows():
            cluster = crash['Cluster']
            node = crash['Node']
            t_crash = crash['Time_s']
            
            post_crash = leaders_df[(leaders_df['Cluster'] == cluster) & (leaders_df['Relative_Time_s'] > t_crash)]
            if not post_crash.empty:
                next_leader_row = post_crash.sort_values('Relative_Time_s').iloc[0]
                t_recovery = next_leader_row['Relative_Time_s']
                new_leader_node = next_leader_row['Node']
                failover_duration = t_recovery - t_crash
            else:
                t_recovery = np.nan
                new_leader_node = "None"
                failover_duration = np.nan
                
            failover_events.append({
                'Cluster': cluster,
                'Crashed_Leader': node,
                'Crash_Time_s': t_crash,
                'New_Leader': new_leader_node,
                'Election_Time_s': t_recovery,
                'Failover_Duration_s': failover_duration
            })
    df_failovers = pd.DataFrame(failover_events)
    
    # 3. Analyze restart & log replay spikes
    restart_events = []
    pattern = os.path.join(args.stats_dir, '*', '*', 'stats_*.csv')
    files = glob.glob(pattern)
    if not files:
        files = glob.glob(os.path.join(args.stats_dir, '**', 'stats_*.csv'), recursive=True)
        
    files_by_node = {}
    for file_path in files:
        parts = file_path.split(os.sep)
        cluster, node = parts[-3], parts[-2]
        files_by_node.setdefault((cluster, node), []).append(file_path)
        
    for (cluster, node), paths in files_by_node.items():
        file_details = []
        for p in paths:
            df = read_clean_csv(p)
            if df.empty: continue
            df.columns = df.columns.str.strip()
            df = df.sort_values('Timestamp_ms')
            file_details.append((df['Timestamp_ms'].min(), p, df))
        file_details.sort(key=lambda x: x[0])
        
        for idx, (start_ts, path, df) in enumerate(file_details):
            is_restart = idx > 0
            df = calculate_rates_on_raw(df)
            
            max_row = df.loc[df['Write_OPS'].idxmax()] if not df['Write_OPS'].isna().all() else None
            peak_ops = max_row['Write_OPS'] if max_row is not None else 0.0
            peak_ts = max_row['Timestamp_ms'] if max_row is not None else start_ts
            
            # Detect log replay spike phase (Write OPS > 200,000)
            spike_rows = df[df['Write_OPS'] > 200000]
            if not spike_rows.empty:
                replay_duration = (spike_rows['Timestamp_ms'].max() - spike_rows['Timestamp_ms'].min()) / 1000.0
                if replay_duration == 0: replay_duration = 1.0
                total_replayed_writes = df.loc[spike_rows.index.max(), 'Total_Write_Requests'] - df.loc[spike_rows.index.min(), 'Total_Write_Requests']
            else:
                replay_duration = 0.0
                total_replayed_writes = 0
                
            restart_events.append({
                'Cluster': cluster,
                'Node': node,
                'File': os.path.basename(path),
                'Is_Restart': is_restart,
                'Start_Timestamp_ms': start_ts,
                'Peak_OPS': peak_ops,
                'Peak_Timestamp_ms': peak_ts,
                'Replay_Duration_s': replay_duration,
                'Replayed_Writes': total_replayed_writes
            })
    df_restarts = pd.DataFrame(restart_events)
    
    # 4. Plots Generation (Light & Dark)
    for theme in ['light', 'dark']:
        suffix = f"_{theme}"
        bg_col = '#0d1117' if theme == 'dark' else '#ffffff'
        text_col = '#c9d1d9' if theme == 'dark' else '#24292f'
        
        # Plot 1: Node State Timeline
        clusters = sorted(df_server['Cluster'].dropna().unique())
        fig, axes = plt.subplots(len(clusters), 1, figsize=(13, 2.5 * len(clusters) + 1.5), sharex=True)
        if len(clusters) == 1: axes = [axes]
        
        for ax, cluster in zip(axes, clusters):
            apply_theme(fig, ax, theme)
            df_cluster = df_server[df_server['Cluster'] == cluster]
            nodes = sorted(df_cluster['Node'].dropna().unique())
            
            for y_idx, node in enumerate(nodes):
                df_node = df_cluster[df_cluster['Node'] == node].sort_values('Relative_Time_s')
                
                # Draw segments based on roles
                for i in range(len(df_node) - 1):
                    t1 = df_node['Relative_Time_s'].iloc[i]
                    t2 = df_node['Relative_Time_s'].iloc[i+1]
                    role = df_node['Role'].iloc[i]
                    
                    if role == 'LEADER': color = '#3fb950' if theme == 'dark' else '#2da44e'
                    elif role == 'FOLLOWER': color = '#f0883e' if theme == 'dark' else '#bc4c00'
                    else: color = '#f85149' if theme == 'dark' else '#cf222e' # OFFLINE
                    
                    ax.plot([t1, t2], [y_idx, y_idx], color=color, linewidth=8, solid_capstyle='butt')
            
            ax.set_yticks(range(len(nodes)))
            ax.set_yticklabels([f"Node {n}" for n in nodes], fontweight='bold')
            ax.set_title(f"Cluster {cluster} Role State Timeline", fontsize=12, fontweight='bold')
            
        plt.xlabel('Time from test start (s)', fontweight='bold')
        fig.tight_layout()
        plt.savefig(os.path.join(args.output_dir, f"recovery_node_roles{suffix}.png"), dpi=150, facecolor=bg_col)
        plt.close()
        
        # Plot 2: Client Write OPS & Crash Overlay
        if df_client is not None and not df_client.empty:
            fig, ax = plt.subplots(figsize=(13, 5))
            apply_theme(fig, ax, theme)
            
            ax.plot(df_client['Relative_Time_s'], df_client['Instantaneous_OPS'], color='#58a6ff' if theme == 'dark' else '#0969da', linewidth=2.0, label='Client Write OPS')
            
            # Overlay crashes
            if not df_crashes.empty:
                for _, crash in df_crashes[df_crashes['Role_Before'] == 'LEADER'].iterrows():
                    ax.axvline(x=crash['Time_s'], color='#f85149' if theme == 'dark' else '#cf222e', linestyle='--', alpha=0.8)
                    ax.text(crash['Time_s'] + 1, ax.get_ylim()[1] * 0.85, f"Crash Cluster {crash['Cluster']} Leader", color='#f85149' if theme == 'dark' else '#cf222e', fontsize=9, fontweight='bold')
            
            ax.set_title("Client Write Throughput & Leader Crashes", fontsize=13, fontweight='bold')
            ax.set_xlabel("Time from test start (s)")
            ax.set_ylabel("OPS")
            ax.legend()
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"recovery_client_ops{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
        # Plot 3: Raft Index Alignment
        fig, axes = plt.subplots(len(clusters), 1, figsize=(13, 3 * len(clusters) + 1), sharex=True)
        if len(clusters) == 1: axes = [axes]
        for ax, cluster in zip(axes, clusters):
            apply_theme(fig, ax, theme)
            df_cluster = df_server[df_server['Cluster'] == cluster]
            
            for node in sorted(df_cluster['Node'].dropna().unique()):
                df_node = df_cluster[df_cluster['Node'] == node].sort_values('Relative_Time_s')
                ax.plot(df_node['Relative_Time_s'], df_node['Raft_Idx_Applied'], label=f"Node {node} Applied Index", alpha=0.8, linewidth=1.8)
                
            ax.set_title(f"Cluster {cluster} Raft Index Alignment", fontsize=12, fontweight='bold')
            ax.set_ylabel("Raft Index")
            ax.legend(loc='lower right', fontsize=9)
        plt.xlabel("Time from test start (s)")
        fig.tight_layout()
        plt.savefig(os.path.join(args.output_dir, f"recovery_raft_lag{suffix}.png"), dpi=150, facecolor=bg_col)
        plt.close()

    # 5. HTML Report Generation
    print("Generating HTML report...")
    html_content = get_html_header("Failure Recovery Analysis Dashboard", "Raft + LSMT Failover & Log Replay Evaluation", args.stats_dir, args.client_csv)
    
    # 5.1 Summary cards
    num_crashes = len(df_crashes)
    avg_failover = df_failovers['Failover_Duration_s'].mean() if not df_failovers.empty else 0.0
    max_failover = df_failovers['Failover_Duration_s'].max() if not df_failovers.empty else 0.0
    peak_replay = df_restarts['Peak_OPS'].max() if not df_restarts.empty else 0.0
    
    # Format peak replay
    peak_replay_str = f"{peak_replay/1e6:.2f}M OPS" if peak_replay >= 1e6 else f"{peak_replay/1e3:.1f}k OPS"
    
    html_content += f"""
        <!-- Summary Dashboard -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Crash Events</div>
                <div class="stat-value danger-text">{num_crashes}</div>
                <div class="stat-desc">Server nodes going offline</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Failover Time</div>
                <div class="stat-value warning-text">{avg_failover:.2f} s</div>
                <div class="stat-desc">Time to elect a new Leader</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Max Failover Time</div>
                <div class="stat-value danger-text">{max_failover:.2f} s</div>
                <div class="stat-desc">Worst case reelection latency</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Peak Log Replay Rate</div>
                <div class="stat-value success-text">{peak_replay_str}</div>
                <div class="stat-desc">Max rate replaying WAL logs</div>
            </div>
        </div>
    """
    
    # Failovers Table
    html_content += """
        <h2>Leader Failover Events</h2>
        <table>
            <thead>
                <tr>
                    <th>Cluster</th>
                    <th>Crashed Leader</th>
                    <th>Crash Time (s)</th>
                    <th>New Leader Elected</th>
                    <th>Election Complete (s)</th>
                    <th>Reelection Duration</th>
                </tr>
            </thead>
            <tbody>
    """
    if df_failovers.empty:
        html_content += "<tr><td colspan='6' class='text-center'>No leader crashes detected during the test.</td></tr>"
    else:
        for _, row in df_failovers.iterrows():
            dur_str = f"{row['Failover_Duration_s']:.2f} s" if not pd.isna(row['Failover_Duration_s']) else "N/A"
            elec_str = f"{row['Election_Time_s']:.2f} s" if not pd.isna(row['Election_Time_s']) else "N/A"
            html_content += f"""
                <tr>
                    <td class="semibold">Cluster {row['Cluster']}</td>
                    <td>Node {row['Crashed_Leader']}</td>
                    <td>{row['Crash_Time_s']:.1f} s</td>
                    <td><span class="badge badge-leader">Node {row['New_Leader']}</span></td>
                    <td>{elec_str}</td>
                    <td class="semibold danger-text">{dur_str}</td>
                </tr>
            """
    html_content += "</tbody></table>"
    
    # Restarts Table
    html_content += """
        <h2>Node Reboot & Log Replay Spikes</h2>
        <table>
            <thead>
                <tr>
                    <th>Cluster</th>
                    <th>Node ID</th>
                    <th>Boot Type</th>
                    <th>Trigger Time (s)</th>
                    <th>Peak Replay Speed</th>
                    <th>Replayed Writes</th>
                    <th>Replay Duration</th>
                </tr>
            </thead>
            <tbody>
    """
    if df_restarts.empty:
        html_content += "<tr><td colspan='7' class='text-center'>No node reboot events recorded.</td></tr>"
    else:
        for _, row in df_restarts.sort_values(['Cluster', 'Node', 'Start_Timestamp_ms']).iterrows():
            g_start = df_restarts['Start_Timestamp_ms'].min()
            rel_start_s = (row['Start_Timestamp_ms'] - g_start) / 1000.0
            boot_type = "RESTART / RECOVERY" if row['Is_Restart'] else "INITIAL BOOT"
            boot_badge = "badge-follower" if row['Is_Restart'] else "badge-leader"
            ops_str = f"{row['Peak_OPS']/1e6:.2f}M OPS" if row['Peak_OPS'] >= 1e6 else f"{row['Peak_OPS']/1e3:.1f}k OPS"
            dur_str = f"{row['Replay_Duration_s']:.2f} s" if row['Replay_Duration_s'] > 0 else "Instantaneous"
            
            html_content += f"""
                <tr>
                    <td class="semibold">Cluster {row['Cluster']}</td>
                    <td>Node {row['Node']}</td>
                    <td><span class="badge {boot_badge}">{boot_type}</span></td>
                    <td>{rel_start_s:.1f} s</td>
                    <td class="semibold success-text">{ops_str}</td>
                    <td>{row['Replayed_Writes']:,} requests</td>
                    <td>{dur_str}</td>
                </tr>
            """
    html_content += "</tbody></table>"
    
    # Embedded Plots Panel
    html_content += """
        <h2>Recovery Visualization</h2>
        <div class="plot-panel">
            <h3>Raft Cluster States & Roles Over Time</h3>
            <div class="plot-container">
                <img class="plot-image" src="plots/recovery_node_roles_dark.png" alt="Node Roles State Timeline">
            </div>
            <p class="plot-description">
                Timeline visualization of the roles held by each node in the cluster. Green segments indicate Leader active status, orange represents Follower status, and red segments depict OFFLINE/crashed duration. Useful for identifying re-election intervals and cluster-wide alignment.
            </p>
        </div>
    """
    
    if df_client is not None and not df_client.empty:
        html_content += """
            <div class="plot-panel">
                <h3>Client Write Throughput & Crash Impact</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/recovery_client_ops_dark.png" alt="Client Write Throughput">
                </div>
                <p class="plot-description">
                    Overlaid client-side write OPS with leader crash markers. Demonstrates the impact of leader failures on write throughput, showing the drop in throughput during failover and the recovery to stable levels after a new leader is elected.
                </p>
            </div>
        """
        
    html_content += """
        <div class="plot-panel">
            <h3>Raft Log Replication Alignment</h3>
            <div class="plot-container">
                <img class="plot-image" src="plots/recovery_raft_lag_dark.png" alt="Raft Index Alignment">
            </div>
            <p class="plot-description">
                Comparison of the local Applied Index across all nodes. Displays replication progress and confirms whether followers sync up rapidly with the leader after their recovery/boot.
            </p>
        </div>
    """
    
    html_content += get_html_footer()
    
    # Write report
    with open(args.report_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"[✓] HTML Recovery report successfully generated: {args.report_file}")

# =====================================================================
# 2. WRITE ANALYSIS MODE
# =====================================================================
def run_write_mode(args):
    print("=== Phase: Write Performance Analysis ===")
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load server stats
    try:
        df_server, global_start_ms = load_and_resample_server_data(args.stats_dir)
    except ValueError as e:
        print(f"  [⚠️ WARNING] Server stats not found: {e}. Skipping Write Performance Analysis.")
        return

    client_dir = args.client_csv if os.path.isdir(args.client_csv) else os.path.dirname(args.client_csv)
    client_files = sorted(glob.glob(os.path.join(client_dir, 'client_throughput_*.csv')))
    if not client_files and os.path.isfile(args.client_csv):
        client_files = [args.client_csv]
    df_client = load_and_aggregate_client_files(client_files, op_type='write')
    df_events = load_storage_events(args.stats_dir)

    
    # Sync time
    df_server['Relative_Time_s'] = (df_server['Timestamp_ms'] - global_start_ms) / 1000.0
    if df_client is not None:
        df_client['Relative_Time_s'] = (df_client['Timestamp_ms'] - global_start_ms) / 1000.0
    if not df_events.empty:
        df_events['Relative_Time_s'] = (df_events['timestamp'] - global_start_ms) / 1000.0
        
    t_min = df_server['Timestamp_ms'].min()
    t_max = df_server['Timestamp_ms'].max()
    
    # Get node load
    df_node_load = calculate_node_load(df_server, t_min, t_max, op_type='write')
    
    # LSM Activity count
    num_flushes = 0
    num_compactions = 0
    compaction_duration_s = 0.0
    if not df_events.empty:
        num_flushes = len(df_events[df_events['event_type'] == 'memtable_flush'])
        compactions = df_events[df_events['event_type'] == 'compaction']
        num_compactions = len(compactions)
        compaction_duration_s = compactions['duration_ms'].sum() / 1000.0
        
    # Generate Plots (Light & Dark)
    for theme in ['light', 'dark']:
        suffix = f"_{theme}"
        bg_col = '#0d1117' if theme == 'dark' else '#ffffff'
        
        # Plot 1: Client Write Throughput (OPS & MB/s)
        if df_client is not None and not df_client.empty:
            fig, ax1 = plt.subplots(figsize=(13, 5))
            apply_theme(fig, ax1, theme)
            
            color = '#58a6ff' if theme == 'dark' else '#0969da'
            ax1.plot(df_client['Relative_Time_s'], df_client['Instantaneous_OPS'], color=color, linewidth=2.5, label='Client Write OPS')
            ax1.set_xlabel('Time from test start (s)', fontweight='bold')
            ax1.set_ylabel('Write OPS', color=color, fontweight='bold')
            ax1.tick_params(axis='y', labelcolor=color)
            
            ax2 = ax1.twinx()
            color = '#3fb950' if theme == 'dark' else '#1a7f37'
            ax2.plot(df_client['Relative_Time_s'], df_client['Instantaneous_MBps'], color=color, linewidth=1.8, linestyle='--', label='Client Write Bandwidth')
            ax2.set_ylabel('Write Throughput (MB/s)', color=color, fontweight='bold')
            ax2.tick_params(axis='y', labelcolor=color)
            
            plt.title('Client Write Throughput & Bandwidth Over Time', fontsize=13, fontweight='bold')
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"write_throughput{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
        # Plot 2: Leader Write Latency Profile
        fig, ax = plt.subplots(figsize=(13, 5))
        apply_theme(fig, ax, theme)
        
        # Select Leader latency
        leaders_df = df_server[df_server['Role'] == 'LEADER']
        if leaders_df.empty:
            leaders_df = df_server  # Fallback to aggregate
            
        latency_agg = leaders_df.groupby('Relative_Time_s')[['P50_Latency_ms', 'P95_Latency_ms', 'P99_Latency_ms']].mean().reset_index()
        
        ax.plot(latency_agg['Relative_Time_s'], latency_agg['P50_Latency_ms'], color='#3fb950' if theme == 'dark' else '#2da44e', linewidth=1.8, label='P50 Latency')
        ax.plot(latency_agg['Relative_Time_s'], latency_agg['P95_Latency_ms'], color='#f0883e' if theme == 'dark' else '#bc4c00', linewidth=1.8, label='P95 Latency')
        ax.plot(latency_agg['Relative_Time_s'], latency_agg['P99_Latency_ms'], color='#f85149' if theme == 'dark' else '#cf222e', linewidth=1.8, label='P99 Latency')
        
        ax.set_yscale('log')
        ax.set_title('Write Latency Profiles (Leader) - Log Scale', fontsize=13, fontweight='bold')
        ax.set_xlabel('Time from test start (s)')
        ax.set_ylabel('Latency (ms)')
        ax.legend()
        fig.tight_layout()
        plt.savefig(os.path.join(args.output_dir, f"write_latency{suffix}.png"), dpi=150, facecolor=bg_col)
        plt.close()
        
        # Plot 3: LSM Storage Events Overlay
        if df_client is not None and not df_client.empty:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
            apply_theme(fig, ax1, theme)
            apply_theme(fig, ax2, theme)
            
            # Top panel: client Write OPS
            ax1.plot(df_client['Relative_Time_s'], df_client['Instantaneous_OPS'], color='#58a6ff' if theme == 'dark' else '#0969da', linewidth=2.0)
            ax1.set_ylabel('Client Write OPS', fontweight='bold')
            ax1.set_title('Impact of LSM-Tree Compactions & Flushes on Write Performance', fontsize=13, fontweight='bold')
            
            # Bottom panel: P99 Latency
            ax2.plot(latency_agg['Relative_Time_s'], latency_agg['P99_Latency_ms'], color='#f85149' if theme == 'dark' else '#cf222e', linewidth=1.8)
            ax2.set_ylabel('P99 Latency (ms) [Log]', fontweight='bold')
            ax2.set_yscale('log')
            ax2.set_xlabel('Time from test start (s)', fontweight='bold')
            
            # Shading storage events
            if not df_events.empty:
                flushes = df_events[df_events['event_type'] == 'memtable_flush']
                compactions = df_events[df_events['event_type'] == 'compaction']
                
                # Plot flushes as lines
                for _, row in flushes.iterrows():
                    ax1.axvline(x=row['Relative_Time_s'], color='#1f6feb', linestyle=':', alpha=0.5, linewidth=1.2)
                    ax2.axvline(x=row['Relative_Time_s'], color='#1f6feb', linestyle=':', alpha=0.5, linewidth=1.2)
                    
                # Plot compactions as spans
                first_comp = True
                for _, row in compactions.iterrows():
                    t_start = row['Relative_Time_s']
                    t_end = t_start + (row['duration_ms'] / 1000.0)
                    lbl = 'LSM Compaction' if first_comp else ''
                    ax1.axvspan(t_start, t_end, color='#f85149' if theme == 'dark' else '#cf222e', alpha=0.2, label=lbl)
                    ax2.axvspan(t_start, t_end, color='#f85149' if theme == 'dark' else '#cf222e', alpha=0.2)
                    first_comp = False
                
                ax1.legend(loc='upper right')
                
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"write_lsm_impact{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
        # Plot 4: Backlog Queues
        fig, ax = plt.subplots(figsize=(13, 5))
        apply_theme(fig, ax, theme)
        for node in sorted(df_server['Node'].dropna().unique()):
            df_node = df_server[df_server['Node'] == node].sort_values('Relative_Time_s')
            ax.plot(df_node['Relative_Time_s'], df_node['Backlog'], label=f"Node {node} Backlog Queue", linewidth=1.5, alpha=0.8)
        ax.set_title('Write Request Backlog Queue Size', fontsize=13, fontweight='bold')
        ax.set_xlabel('Time from test start (s)')
        ax.set_ylabel('Backlog Queue Length')
        ax.legend()
        fig.tight_layout()
        plt.savefig(os.path.join(args.output_dir, f"write_queues{suffix}.png"), dpi=150, facecolor=bg_col)
        plt.close()
        
    # 5. HTML Report Generation
    print("Generating HTML report...")
    html_content = get_html_header("Write Performance Analysis Dashboard", "Raft + LSM-Tree Write Heavy Workload Profile", args.stats_dir, args.client_csv)
    
    # Summary Cards
    avg_ops = df_client['Instantaneous_OPS'].mean() if df_client is not None else 0.0
    peak_ops = df_client['Instantaneous_OPS'].max() if df_client is not None else 0.0
    avg_mbps = df_client['Instantaneous_MBps'].mean() if df_client is not None else 0.0
    p95_lat = latency_agg['P95_Latency_ms'].mean() if not latency_agg.empty else 0.0
    
    ops_fmt = f"{avg_ops/1e6:.2f}M OPS" if avg_ops >= 1e6 else f"{avg_ops/1e3:.1f}k OPS"
    peak_ops_fmt = f"{peak_ops/1e6:.2f}M OPS" if peak_ops >= 1e6 else f"{peak_ops/1e3:.1f}k OPS"
    
    html_content += f"""
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Avg Client QPS</div>
                <div class="stat-value var(--text-color)">{ops_fmt}</div>
                <div class="stat-desc">Mean client-side write speed</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Peak Client QPS</div>
                <div class="stat-value success-text">{peak_ops_fmt}</div>
                <div class="stat-desc">Max client-side burst write speed</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Bandwidth</div>
                <div class="stat-value success-text">{avg_mbps:.1f} MB/s</div>
                <div class="stat-desc">Mean bytes written per second</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">P95 Write Latency</div>
                <div class="stat-value warning-text">{p95_lat:.2f} ms</div>
                <div class="stat-desc">Leader average 95th percentile</div>
            </div>
        </div>
    """
    
    # Leader vs Follower load distribution table
    html_content += """
        <h2>Leader vs Follower Load Distribution</h2>
        <div class="insight-box">
            <strong>Raft Cluster Configuration Summary:</strong> Each cluster is configured with 3 nodes (Node 1, 2, and 3). One node acts as the <strong>LEADER</strong> executing writes and committing them via Raft log replication, while the other two nodes act as <strong>FOLLOWERS</strong> replication targets.
        </div>
        <table>
            <thead>
                <tr>
                    <th>Cluster</th>
                    <th>Node ID</th>
                    <th>Raft Role</th>
                    <th>Writes Processed</th>
                    <th>Data Written (MB)</th>
                    <th>Avg Backlog Queue</th>
                    <th>Avg Pending requests</th>
                    <th>Avg Latency</th>
                    <th>P95 Latency</th>
                </tr>
            </thead>
            <tbody>
    """
    for _, row in df_node_load.sort_values(['Cluster', 'Node']).iterrows():
        role_class = "badge-leader" if row['Role'] == "LEADER" else ("badge-follower" if row['Role'] == "FOLLOWER" else "badge-offline")
        html_content += f"""
            <tr>
                <td class="semibold">Cluster {row['Cluster']}</td>
                <td>Node {row['Node']}</td>
                <td><span class="badge {role_class}">{row['Role']}</span></td>
                <td class="semibold">{row['Requests_Processed']:,}</td>
                <td>{row['Bytes_Processed_MB']:.1f} MB</td>
                <td>{row['Avg_Backlog']:.1f}</td>
                <td>{row['Avg_Pending_Reqs']:.1f}</td>
                <td>{row['Avg_Latency_ms']:.2f} ms</td>
                <td>{row['P95_Latency_ms']:.2f} ms</td>
            </tr>
        """
    html_content += "</tbody></table>"
    
    # Compaction details
    html_content += f"""
        <h2>LSM-Tree Storage Engine Events</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Memtable Flushes</div>
                <div class="stat-value">{num_flushes}</div>
                <div class="stat-desc">Total memtable dumps to SSTables</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">LSM Compactions</div>
                <div class="stat-value danger-text">{num_compactions}</div>
                <div class="stat-desc">SSTable merges and garbage collections</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Compaction Time</div>
                <div class="stat-value warning-text">{compaction_duration_s:.1f} s</div>
                <div class="stat-desc">Duration background threads were active</div>
            </div>
        </div>
    """
    
    if not df_events.empty:
        html_content += """
            <table>
                <thead>
                    <tr>
                        <th>Cluster</th>
                        <th>Node</th>
                        <th>Event Type</th>
                        <th>Duration</th>
                        <th>Bytes Flushed</th>
                        <th>Input Data</th>
                        <th>Output Data</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in df_events.sort_values('timestamp').head(15).iterrows():
            rel_t = (row['timestamp'] - global_start_ms)/1000.0
            type_class = "semibold danger-text" if row['event_type'] == "compaction" else "semibold success-text"
            flushed_str = f"{row['bytes_flushed']/(1024*1024):.1f} MB" if not pd.isna(row['bytes_flushed']) and row['bytes_flushed'] > 0 else "0"
            input_str = f"{row['input_bytes']/(1024*1024):.1f} MB" if not pd.isna(row['input_bytes']) and row['input_bytes'] > 0 else "N/A"
            output_str = f"{row['output_bytes']/(1024*1024):.1f} MB" if not pd.isna(row['output_bytes']) and row['output_bytes'] > 0 else "N/A"
            
            html_content += f"""
                <tr>
                    <td class="semibold">Cluster {row['Cluster']}</td>
                    <td>Node {row['Node']}</td>
                    <td><span class="{type_class}">{row['event_type'].upper()}</span></td>
                    <td>{row['duration_ms']/1000.0:.2f} s</td>
                    <td>{flushed_str}</td>
                    <td>{input_str}</td>
                    <td>{output_str}</td>
                </tr>
            """
        html_content += "</tbody></table>"
        
    # Embedded Plots Panel
    html_content += """
        <h2>Write Analysis Charts</h2>
        <div class="plot-panel">
            <h3>Client Write Throughput & Bandwidth</h3>
            <div class="plot-container">
                <img class="plot-image" src="plots/write_throughput_dark.png" alt="Client Write Throughput">
            </div>
            <p class="plot-description">
                Time-series graph showing client-side write OPS (operations per second) on the left blue axis, and overall bandwidth (MB/s) on the right green axis. Helps evaluate overall test throughput stability.
            </p>
        </div>
        
        <div class="plot-panel">
            <h3>Leader Write Latency Profile</h3>
            <div class="plot-container">
                <img class="plot-image" src="plots/write_latency_dark.png" alt="Leader Write Latency Profile">
            </div>
            <p class="plot-description">
                Displays the 50th, 95th, and 99th percentile write latency on the leader node(s). The log-scale Y-axis emphasizes latency spikes or stalls.
            </p>
        </div>
    """
    
    if df_client is not None and not df_client.empty:
        html_content += """
            <div class="plot-panel">
                <h3>LSM compaction and flush performance drops</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/write_lsm_impact_dark.png" alt="LSM Impact on Writes">
                </div>
                <p class="plot-description">
                    Correlates client Write OPS (top panel) and P99 latency (bottom panel) with background LSM activity. Red vertical shaded spans show compactions and dashed blue vertical lines show memtable flushes. This clearly illustrates write stalls caused by disk compaction activities.
                </p>
            </div>
        """
        
    html_content += """
        <div class="plot-panel">
            <h3>Server Backlog Queues</h3>
            <div class="plot-container">
                <img class="plot-image" src="plots/write_queues_dark.png" alt="Server Backlog Queues">
            </div>
            <p class="plot-description">
                Illustrates the size of pending queues and backlog across nodes. High queues on followers suggest slow log replication receipt, while high queues on leaders represent engine write saturation.
            </p>
        </div>
    """
    
    html_content += get_html_footer()
    
    with open(args.report_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"[✓] HTML Write report successfully generated: {args.report_file}")


# =====================================================================
# 3. READ ANALYSIS MODE
# =====================================================================
def parse_config_range_sizes(data_dir):
    config_path = os.path.join(data_dir, 'config.txt')
    range_sizes = []
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            for line in f:
                if 'read_benchmark.py' in line:
                    match = re.search(r'--range-size\s+(\d+)', line)
                    if match:
                        range_sizes.append(int(match.group(1)))
    return range_sizes

def run_read_mode(args):
    print("=== Phase: Read Performance Analysis ===")
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 1. Determine read benchmark files
    # Check if multiple read throughput files exist in the client CSV parent directory or source folder
    data_dir = os.path.dirname(args.client_csv) if os.path.isfile(args.client_csv) else args.client_csv
    
    client_files = sorted(glob.glob(os.path.join(data_dir, 'read_throughput_*.csv')))
    
    if not client_files:
        # Fallback to single read file matching client_csv
        client_files = [args.client_csv] if os.path.isfile(args.client_csv) else []
        
    if not client_files:
        print("  [⚠️ WARNING] No read_throughput_*.csv or client CSV files found. Skipping Read Performance Analysis.")
        return
        
    print(f"Loading server stats...")
    try:
        df_server, global_start_ms = load_and_resample_server_data(args.stats_dir)
    except ValueError as e:
        print(f"  [⚠️ WARNING] Server stats not found: {e}. Skipping Read Performance Analysis.")
        return

    df_server['Relative_Time_s'] = (df_server['Timestamp_ms'] - global_start_ms) / 1000.0
    
    df_events = load_storage_events(args.stats_dir)
    if not df_events.empty:
        df_events['Relative_Time_s'] = (df_events['timestamp'] - global_start_ms) / 1000.0
        
    # Group client files into sessions based on start time
    sessions_grouped = group_files_into_sessions(client_files)
    print(f"Grouped client read files into {len(sessions_grouped)} test sessions.")
    
    is_multi = len(sessions_grouped) > 1
    
    if is_multi:
        print(f"Detected multi-session read benchmarks. Parsing config and matching range sizes.")
        range_sizes = parse_config_range_sizes(data_dir)
        print(f"Parsed range sizes from config: {range_sizes}")
        
        if len(range_sizes) != len(sessions_grouped):
            print(f"  [⚠️ WARNING] Mismatch in range sizes and sessions. Synthesizing labels.")
            if len(range_sizes) < len(sessions_grouped):
                range_sizes += [9999] * (len(sessions_grouped) - len(range_sizes))
            else:
                range_sizes = range_sizes[:len(sessions_grouped)]
                
        # Load all sessions
        sessions = []
        summary_data = []
        
        for session_files, rs in zip(sessions_grouped, range_sizes):
            df_client = load_and_aggregate_client_files(session_files, op_type='read')
            if df_client is None or df_client.empty:
                continue
            sessions.append((rs, df_client))
            
            t_start = df_client['Timestamp_ms'].min()
            t_end = df_client['Timestamp_ms'].max()
            duration_s = (t_end - t_start) / 1000.0
            
            # Client metrics
            avg_qps = df_client['Instantaneous_OPS'].mean()
            max_qps = df_client['Instantaneous_OPS'].max()
            avg_mbps = df_client['Instantaneous_MBps'].mean()
            avg_lat = df_client['Avg_Latency_ms'].mean()
            p50_lat = df_client['P50_Latency_ms'].mean()
            p95_lat = df_client['P95_Latency_ms'].mean()
            
            # Server read OPS in session window
            df_server_session = df_server[(df_server['Timestamp_ms'] >= t_start) & (df_server['Timestamp_ms'] <= t_end)]
            avg_server_read_ops = 0.0
            if not df_server_session.empty:
                server_agg = df_server_session.groupby('Timestamp_ms')['Read_OPS'].sum()
                avg_server_read_ops = server_agg.mean() if not server_agg.empty else 0.0
                
            # LSM Events
            flushes_count = 0
            compactions_count = 0
            if not df_events.empty:
                df_events_session = df_events[(df_events['timestamp'] >= t_start) & (df_events['timestamp'] <= t_end)]
                flushes_count = len(df_events_session[df_events_session['event_type'] == 'memtable_flush'])
                compactions_count = len(df_events_session[df_events_session['event_type'] == 'compaction'])
                
            summary_data.append({
                'Range_Size': rs,
                'Duration_s': duration_s,
                'Avg_Client_QPS': avg_qps,
                'Max_Client_QPS': max_qps,
                'Avg_Client_MBps': avg_mbps,
                'Avg_Client_Latency_ms': avg_lat,
                'P50_Client_Latency_ms': p50_lat,
                'P95_Client_Latency_ms': p95_lat,
                'Avg_Server_Read_OPS': avg_server_read_ops,
                'Memtable_Flushes': flushes_count,
                'LSM_Compactions': compactions_count,
                'T_Start': t_start,
                'T_End': t_end
            })
            
        summary_df = pd.DataFrame(summary_data)
        
        # Plot: Multi-session Comparison Charts
        for theme in ['light', 'dark']:
            suffix = f"_{theme}"
            bg_col = '#0d1117' if theme == 'dark' else '#ffffff'
            
            # 1. QPS & Bandwidth vs Range Size
            fig, ax1 = plt.subplots(figsize=(11, 5.5))
            apply_theme(fig, ax1, theme)
            
            color = '#58a6ff' if theme == 'dark' else '#0969da'
            x_labels = [str(rs) for rs in summary_df['Range_Size']]
            x_indices = np.arange(len(summary_df))
            
            ax1.plot(x_indices, summary_df['Avg_Client_QPS'], color=color, marker='o', linewidth=2.5, label='Avg Read QPS')
            ax1.set_xlabel('Pagination Scan Range Size', fontweight='bold')
            ax1.set_ylabel('Read QPS', color=color, fontweight='bold')
            ax1.tick_params(axis='y', labelcolor=color)
            ax1.set_xticks(x_indices)
            ax1.set_xticklabels(x_labels)
            
            ax2 = ax1.twinx()
            color = '#3fb950' if theme == 'dark' else '#1a7f37'
            ax2.plot(x_indices, summary_df['Avg_Client_MBps'], color=color, marker='s', linewidth=2.0, linestyle='--', label='Avg MB/s')
            ax2.set_ylabel('Bandwidth (MB/s)', color=color, fontweight='bold')
            ax2.tick_params(axis='y', labelcolor=color)
            
            plt.title('Throughput & Bandwidth vs Pagination Scan Range Size', fontsize=13, fontweight='bold')
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"read_throughput_vs_range_size{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
            # 2. Latency curves vs Range Size
            fig, ax = plt.subplots(figsize=(11, 5.5))
            apply_theme(fig, ax, theme)
            ax.plot(x_indices, summary_df['Avg_Client_Latency_ms'], marker='o', linewidth=2.0, color='#58a6ff' if theme == 'dark' else '#0969da', label='Average Latency')
            ax.plot(x_indices, summary_df['P50_Client_Latency_ms'], marker='v', linewidth=2.0, color='#3fb950' if theme == 'dark' else '#2da44e', label='P50 Latency')
            ax.plot(x_indices, summary_df['P95_Client_Latency_ms'], marker='^', linewidth=2.0, color='#f85149' if theme == 'dark' else '#cf222e', label='P95 Latency')
            ax.set_yscale('log')
            ax.set_xticks(x_indices)
            ax.set_xticklabels(x_labels)
            ax.set_xlabel('Pagination Scan Range Size', fontweight='bold')
            ax.set_ylabel('Latency (ms) [Log]', fontweight='bold')
            ax.set_title('Read Latency Curve vs Range Size', fontsize=13, fontweight='bold')
            ax.legend()
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"read_latency_vs_range_size{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
            # 3. Overlaid QPS over Time
            fig, (ax_qps, ax_lat) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
            apply_theme(fig, ax_qps, theme)
            apply_theme(fig, ax_lat, theme)
            
            colors = sns.color_palette("rocket_r" if theme == 'dark' else "viridis", len(sessions))
            for idx, (rs, df_s) in enumerate(sessions):
                df_s = df_s.copy()
                df_s['Relative_Time_s'] = (df_s['Timestamp_ms'] - df_s['Timestamp_ms'].min()) / 1000.0
                lbl = f"Range Size: {rs}"
                ax_qps.plot(df_s['Relative_Time_s'], df_s['Instantaneous_OPS'], color=colors[idx], label=lbl, linewidth=1.8)
                ax_lat.plot(df_s['Relative_Time_s'], df_s['Avg_Latency_ms'], color=colors[idx], label=lbl, linewidth=1.8)
                
            ax_qps.set_ylabel('Read QPS', fontweight='bold')
            ax_qps.set_title('Read QPS Overlaid Comparison Over Time', fontsize=12, fontweight='semibold')
            ax_qps.legend(loc='upper right', fontsize=8)
            
            ax_lat.set_ylabel('Avg Latency (ms) [Log]', fontweight='bold')
            ax_lat.set_yscale('log')
            ax_lat.set_xlabel('Time from session start (s)', fontweight='bold')
            ax_lat.set_title('Average Client Latency Overlaid Comparison Over Time', fontsize=12, fontweight='semibold')
            
            fig.suptitle('Read Sessions Performance Profiles (Overlaid)', fontsize=14, fontweight='bold', y=0.98)
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"read_overlaid_qps{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
        # HTML Multi report
        html_content = get_html_header("Read Pagination Comparison Dashboard", "Distributed Read-Heavy Scan Range Evaluation", args.stats_dir, args.client_csv)
        
        # Summary table
        html_content += """
            <h2>Read Performance Comparison Matrix</h2>
            <table>
                <thead>
                    <tr>
                        <th>Range Size</th>
                        <th>Duration</th>
                        <th>Avg Client QPS</th>
                        <th>Max Client QPS</th>
                        <th>Avg Bandwidth</th>
                        <th>Avg Latency</th>
                        <th>P50 Latency</th>
                        <th>P95 Latency</th>
                        <th>Server Read OPS</th>
                        <th>LSM Flushes</th>
                        <th>Compactions</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in summary_df.iterrows():
            html_content += f"""
                <tr>
                    <td class="semibold" style="color:var(--primary-color);">Range Size {int(row['Range_Size'])}</td>
                    <td>{row['Duration_s']:.1f} s</td>
                    <td class="semibold">{row['Avg_Client_QPS']:.1f}</td>
                    <td>{row['Max_Client_QPS']:.1f}</td>
                    <td class="semibold success-text">{row['Avg_Client_MBps']:.1f} MB/s</td>
                    <td>{row['Avg_Client_Latency_ms']:.2f} ms</td>
                    <td>{row['P50_Client_Latency_ms']:.2f} ms</td>
                    <td class="danger-text semibold">{row['P95_Client_Latency_ms']:.2f} ms</td>
                    <td>{row['Avg_Server_Read_OPS']:.1f}</td>
                    <td>{int(row['Memtable_Flushes'])}</td>
                    <td>{int(row['LSM_Compactions'])}</td>
                </tr>
            """
        html_content += "</tbody></table>"
        
        # Load balancing table for the last (or largest) read session
        last_sess = summary_df.iloc[-1]
        df_node_load = calculate_node_load(df_server, last_sess['T_Start'], last_sess['T_End'], op_type='read')
        
        html_content += f"""
            <h2>Read Load Distribution (Range Size: {int(last_sess['Range_Size'])})</h2>
            <div class="insight-box">
                <strong>Raft Routing Strategy Analysis:</strong> 
                This table shows the division of read requests across leader and follower nodes during the largest scan test. 
                If the routing strategy is routing reads to followers (e.g. followers have high requests processed), we can verify load balancing. 
                If followers have 0 requests, it means reads are solely executed on the Leader.
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Cluster</th>
                        <th>Node ID</th>
                        <th>Role</th>
                        <th>Reads Processed</th>
                        <th>Data Read (MB)</th>
                        <th>Avg Backlog Queue</th>
                        <th>Avg Pending requests</th>
                        <th>Avg Read Latency</th>
                        <th>P95 Read Latency</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in df_node_load.sort_values(['Cluster', 'Node']).iterrows():
            role_class = "badge-leader" if row['Role'] == "LEADER" else ("badge-follower" if row['Role'] == "FOLLOWER" else "badge-offline")
            html_content += f"""
                <tr>
                    <td class="semibold">Cluster {row['Cluster']}</td>
                    <td>Node {row['Node']}</td>
                    <td><span class="badge {role_class}">{row['Role']}</span></td>
                    <td class="semibold">{row['Requests_Processed']:,}</td>
                    <td>{row['Bytes_Processed_MB']:.1f} MB</td>
                    <td>{row['Avg_Backlog']:.1f}</td>
                    <td>{row['Avg_Pending_Reqs']:.1f}</td>
                    <td>{row['Avg_Latency_ms']:.2f} ms</td>
                    <td>{row['P95_Latency_ms']:.2f} ms</td>
                </tr>
            """
        html_content += "</tbody></table>"
        
        # Embedded plots
        html_content += """
            <h2>Pagination Benchmark Charts</h2>
            <div class="plot-panel">
                <h3>Read Throughput and Bandwidth vs Range Size</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_throughput_vs_range_size_dark.png" alt="Throughput vs Range Size">
                </div>
                <p class="plot-description">
                    Shows how the client-side Read QPS (left axis, blue) and physical scan bandwidth (right axis, green) change with range sizes. Typically, as range size increases, QPS decreases but overall bandwidth increases.
                </p>
            </div>
            
            <div class="plot-panel">
                <h3>Read Latency Curves vs Range Size</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_latency_vs_range_size_dark.png" alt="Latency vs Range Size">
                </div>
                <p class="plot-description">
                    Illustrates the growth of client-side latency (mean, P50, and P95 percentiles) as the range size increases. The log-scale highlights how pagination query size directly affects server processing latency.
                </p>
            </div>
            
            <div class="plot-panel">
                <h3>Read Session Profiles Comparison (Overlaid)</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_overlaid_qps_dark.png" alt="Overlaid Performance profiles">
                </div>
                <p class="plot-description">
                    Overlaid QPS (top) and average latency (bottom) time-series profile for each individual benchmark session. Highlights performance degradation patterns and stalls over the run durations.
                </p>
            </div>
        """
        
        html_content += get_html_footer()
        with open(args.report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"[✓] HTML Multi-read comparison report generated: {args.report_file}")
        
    else:
        # Single Session Read report
        print(f"Detected single-session read benchmark. Generating single session analysis.")
        session_files = sessions_grouped[0] if sessions_grouped else client_files
        df_client = load_and_aggregate_client_files(session_files, op_type='read')
        if df_client is not None:
            df_client['Relative_Time_s'] = (df_client['Timestamp_ms'] - global_start_ms)/1000.0
            
        t_min = df_server['Timestamp_ms'].min()
        t_max = df_server['Timestamp_ms'].max()
        df_node_load = calculate_node_load(df_server, t_min, t_max, op_type='read')
        
        # Plots Generation
        for theme in ['light', 'dark']:
            suffix = f"_{theme}"
            bg_col = '#0d1117' if theme == 'dark' else '#ffffff'
            
            # 1. Throughput & Bandwidth
            if df_client is not None and not df_client.empty:
                fig, ax1 = plt.subplots(figsize=(13, 5))
                apply_theme(fig, ax1, theme)
                color = '#58a6ff' if theme == 'dark' else '#0969da'
                ax1.plot(df_client['Relative_Time_s'], df_client['Instantaneous_OPS'], color=color, linewidth=2.5, label='Client Read QPS')
                ax1.set_xlabel('Time from test start (s)', fontweight='bold')
                ax1.set_ylabel('Read QPS', color=color, fontweight='bold')
                ax1.tick_params(axis='y', labelcolor=color)
                
                ax2 = ax1.twinx()
                color = '#3fb950' if theme == 'dark' else '#1a7f37'
                ax2.plot(df_client['Relative_Time_s'], df_client['Instantaneous_MBps'], color=color, linewidth=1.8, linestyle='--', label='Client Bandwidth')
                ax2.set_ylabel('Read MB/s', color=color, fontweight='bold')
                ax2.tick_params(axis='y', labelcolor=color)
                
                plt.title('Client Read Throughput & Bandwidth Over Time', fontsize=13, fontweight='bold')
                fig.tight_layout()
                plt.savefig(os.path.join(args.output_dir, f"read_throughput{suffix}.png"), dpi=150, facecolor=bg_col)
                plt.close()
                
                # 2. Client Latency profile
                fig, ax = plt.subplots(figsize=(13, 5))
                apply_theme(fig, ax, theme)
                ax.plot(df_client['Relative_Time_s'], df_client['Avg_Latency_ms'], color='#58a6ff' if theme == 'dark' else '#0969da', linewidth=2.0, label='Avg Latency')
                ax.plot(df_client['Relative_Time_s'], df_client['P50_Latency_ms'], color='#3fb950' if theme == 'dark' else '#2da44e', linewidth=1.8, label='P50 Latency')
                ax.plot(df_client['Relative_Time_s'], df_client['P95_Latency_ms'], color='#f85149' if theme == 'dark' else '#cf222e', linewidth=1.8, label='P95 Latency')
                ax.set_yscale('log')
                ax.set_title('Client Read Latency Profiles - Log Scale', fontsize=13, fontweight='bold')
                ax.set_xlabel('Time from test start (s)')
                ax.set_ylabel('Latency (ms)')
                ax.legend()
                fig.tight_layout()
                plt.savefig(os.path.join(args.output_dir, f"read_latency{suffix}.png"), dpi=150, facecolor=bg_col)
                plt.close()
                
            # 3. Read Load Distribution
            fig, ax = plt.subplots(figsize=(13, 5))
            apply_theme(fig, ax, theme)
            for node in sorted(df_server['Node'].dropna().unique()):
                df_node = df_server[df_server['Node'] == node].sort_values('Relative_Time_s')
                ax.plot(df_node['Relative_Time_s'], df_node['Read_OPS'], label=f"Node {node} Read OPS", linewidth=1.8, alpha=0.8)
            ax.set_title('Database Server-Side Read OPS Load Distribution', fontsize=13, fontweight='bold')
            ax.set_xlabel('Time from test start (s)')
            ax.set_ylabel('Server Processed Read OPS')
            ax.legend()
            fig.tight_layout()
            plt.savefig(os.path.join(args.output_dir, f"read_load_distribution{suffix}.png"), dpi=150, facecolor=bg_col)
            plt.close()
            
            # 4. LSM impact on Reads
            if df_client is not None and not df_client.empty:
                fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
                apply_theme(fig, ax1, theme)
                apply_theme(fig, ax2, theme)
                
                ax1.plot(df_client['Relative_Time_s'], df_client['Instantaneous_OPS'], color='#58a6ff' if theme == 'dark' else '#0969da', linewidth=2.0)
                ax1.set_ylabel('Client QPS', fontweight='bold')
                ax1.set_title('Impact of LSM-Tree Compactions & Flushes on Read Performance', fontsize=13, fontweight='bold')
                
                ax2.plot(df_client['Relative_Time_s'], df_client['P95_Latency_ms'], color='#f85149' if theme == 'dark' else '#cf222e', linewidth=1.8)
                ax2.set_ylabel('P95 Latency (ms) [Log]', fontweight='bold')
                ax2.set_yscale('log')
                ax2.set_xlabel('Time from test start (s)', fontweight='bold')
                
                if not df_events.empty:
                    flushes = df_events[df_events['event_type'] == 'memtable_flush']
                    compactions = df_events[df_events['event_type'] == 'compaction']
                    for _, row in flushes.iterrows():
                        ax1.axvline(x=row['Relative_Time_s'], color='#1f6feb', linestyle=':', alpha=0.5, linewidth=1.2)
                        ax2.axvline(x=row['Relative_Time_s'], color='#1f6feb', linestyle=':', alpha=0.5, linewidth=1.2)
                    first_comp = True
                    for _, row in compactions.iterrows():
                        t_start = row['Relative_Time_s']
                        t_end = t_start + (row['duration_ms'] / 1000.0)
                        lbl = 'LSM Compaction' if first_comp else ''
                        ax1.axvspan(t_start, t_end, color='#f85149' if theme == 'dark' else '#cf222e', alpha=0.2, label=lbl)
                        ax2.axvspan(t_start, t_end, color='#f85149' if theme == 'dark' else '#cf222e', alpha=0.2)
                        first_comp = False
                    ax1.legend(loc='upper right')
                fig.tight_layout()
                plt.savefig(os.path.join(args.output_dir, f"read_lsm_impact{suffix}.png"), dpi=150, facecolor=bg_col)
                plt.close()
                
        # HTML Report
        html_content = get_html_header("Read Performance Dashboard", "Distributed Read-Only Benchmark Evaluation", args.stats_dir, args.client_csv)
        
        # Summary Cards
        avg_qps = df_client['Instantaneous_OPS'].mean() if df_client is not None else 0.0
        peak_qps = df_client['Instantaneous_OPS'].max() if df_client is not None else 0.0
        avg_mbps = df_client['Instantaneous_MBps'].mean() if df_client is not None else 0.0
        avg_lat = df_client['Avg_Latency_ms'].mean() if df_client is not None else 0.0
        
        qps_fmt = f"{avg_qps/1e6:.2f}M OPS" if avg_qps >= 1e6 else f"{avg_qps/1e3:.1f}k OPS"
        peak_qps_fmt = f"{peak_qps/1e6:.2f}M OPS" if peak_qps >= 1e6 else f"{peak_qps/1e3:.1f}k OPS"
        
        html_content += f"""
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Avg Client Read QPS</div>
                    <div class="stat-value">{qps_fmt}</div>
                    <div class="stat-desc">Mean client-side read speed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Peak Client Read QPS</div>
                    <div class="stat-value success-text">{peak_qps_fmt}</div>
                    <div class="stat-desc">Max client-side burst read speed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Avg Bandwidth</div>
                    <div class="stat-value success-text">{avg_mbps:.1f} MB/s</div>
                    <div class="stat-desc">Mean bytes read per second</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Avg Client Latency</div>
                    <div class="stat-value warning-text">{avg_lat:.2f} ms</div>
                    <div class="stat-desc">Mean read request roundtrip duration</div>
                </div>
            </div>
        """
        
        # Load balancing table
        html_content += """
            <h2>Server-Side Read Load Balancing</h2>
            <div class="insight-box">
                <strong>Read Routing Strategy Analysis:</strong> 
                This table shows the division of read requests across leader and follower nodes. 
                If the routing strategy is routing reads to followers (e.g. followers have high requests processed), we can verify load balancing. 
                If followers have 0 requests, it means reads are solely executed on the Leader.
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Cluster</th>
                        <th>Node ID</th>
                        <th>Role</th>
                        <th>Reads Processed</th>
                        <th>Data Read (MB)</th>
                        <th>Avg Backlog Queue</th>
                        <th>Avg Pending requests</th>
                        <th>Avg Latency</th>
                        <th>P95 Latency</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in df_node_load.sort_values(['Cluster', 'Node']).iterrows():
            role_class = "badge-leader" if row['Role'] == "LEADER" else ("badge-follower" if row['Role'] == "FOLLOWER" else "badge-offline")
            html_content += f"""
                <tr>
                    <td class="semibold">Cluster {row['Cluster']}</td>
                    <td>Node {row['Node']}</td>
                    <td><span class="badge {role_class}">{row['Role']}</span></td>
                    <td class="semibold">{row['Requests_Processed']:,}</td>
                    <td>{row['Bytes_Processed_MB']:.1f} MB</td>
                    <td>{row['Avg_Backlog']:.1f}</td>
                    <td>{row['Avg_Pending_Reqs']:.1f}</td>
                    <td>{row['Avg_Latency_ms']:.2f} ms</td>
                    <td>{row['P95_Latency_ms']:.2f} ms</td>
                </tr>
            """
        html_content += "</tbody></table>"
        
        # Embedded plots
        html_content += """
            <h2>Read Performance Charts</h2>
            <div class="plot-panel">
                <h3>Client Read Throughput & Bandwidth</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_throughput_dark.png" alt="Read Throughput">
                </div>
                <p class="plot-description">
                    Client QPS (left axis, blue) and read scan bandwidth (right axis, green) over the duration of the test. Shows general read stability.
                </p>
            </div>
            
            <div class="plot-panel">
                <h3>Client Read Latency Profile</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_latency_dark.png" alt="Read Latency Profile">
                </div>
                <p class="plot-description">
                    Time-series profile of 50th, 95th, and average client-side read latency. Logarithmic scale helps identify read bottlenecks and tail latencies.
                </p>
            </div>
            
            <div class="plot-panel">
                <h3>Server-Side Read load balancing</h3>
                <div class="plot-container">
                    <img class="plot-image" src="plots/read_load_distribution_dark.png" alt="Server Read load distribution">
                </div>
                <p class="plot-description">
                    Displays Server-side Read OPS processed by each node. Confirms if follower reads are active or if all read load falls on the leader.
                </p>
            </div>
        """
        
        if df_client is not None and not df_client.empty:
            html_content += """
                <div class="plot-panel">
                    <h3>Impact of LSM Compactions & Flushes on Reads</h3>
                    <div class="plot-container">
                        <img class="plot-image" src="plots/read_lsm_impact_dark.png" alt="LSM impact on Reads">
                    </div>
                    <p class="plot-description">
                        Correlates background compaction/flush activities with client-side read performance. Shaded red areas represent compaction events, which can degrade read speed due to read amplification or disk resource contention.
                    </p>
                </div>
            """
            
        html_content += get_html_footer()
        with open(args.report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"[✓] HTML single-read report successfully generated: {args.report_file}")

# =====================================================================
# MAIN ENTRYPOINT
# =====================================================================
def main():
    args = parse_args()
    print("==========================================")
    print("Database performance Focused Report Tool")
    print(f"Stats Directory: {args.stats_dir}")
    print(f"Client CSV:      {args.client_csv}")
    print(f"Plots Output:    {args.output_dir}")
    print(f"HTML Report:     {args.report_file}")
    print(f"Analysis Type:   {args.type.upper()}")
    print("==========================================")
    
    if args.type == 'recovery':
        run_recovery_mode(args)
    elif args.type == 'write':
        run_write_mode(args)
    elif args.type == 'read':
        run_read_mode(args)
    else:
        print(f"Error: Unknown type {args.type}")
        
    print("==========================================")
    print("Analysis Completed Successfully!")
    print("==========================================")

if __name__ == '__main__':
    main()

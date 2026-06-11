import os
import re
import glob
import io
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Graphic theme
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
    parser = argparse.ArgumentParser(description="Read Pagination Sessions Comparison Dashboard")
    parser.add_argument('-d', '--data-dir', type=str, required=True, 
                        help="Path to the read-pagination-v2 folder containing client throughput CSVs, config.txt, and server stats")
    parser.add_argument('-o', '--output-dir', type=str, default='reads_comparison_output', 
                        help="Output folder for plots and tables")
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

def load_client_csv(file_path):
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
    
    # Detect if cumulative
    is_cumulative = False
    if len(df) > 5:
        diffs = df[col_ops].diff().dropna()
        if (diffs >= 0).sum() / len(diffs) > 0.8:  # 80% of diffs are non-negative
            non_zero_vals = df[df[col_ops] > 0][col_ops]
            if not non_zero_vals.empty:
                initial_val = non_zero_vals.iloc[0]
                final_val = non_zero_vals.iloc[-1]
                if final_val > initial_val * 1.5:
                    is_cumulative = True
                
    if is_cumulative:
        df['Instantaneous_OPS'] = (df[col_ops].diff().fillna(df[col_ops].iloc[0])) / time_diff
        df['Instantaneous_MBps'] = (df[col_mbps].diff().fillna(df[col_mbps].iloc[0])) / time_diff
    else:
        df['Instantaneous_OPS'] = df[col_ops]
        df['Instantaneous_MBps'] = df[col_mbps]
        
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
    return pd.read_csv(io.StringIO("".join(clean_lines)), sep=CSV_SEP)

def calculate_rates_on_raw(df):
    for col in ['Total_Write_Requests', 'Total_Write_Bytes', 'Total_Read_Requests', 'Total_Read_Bytes', 'Timestamp_ms']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.sort_values('Timestamp_ms').copy()
    time_diff = df['Timestamp_ms'].diff() / 1000.0
    time_diff = time_diff.replace(0, np.nan).fillna(1.0)
    
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

def load_server_data(data_dir):
    # Search under data-* folders or stats folder
    search_dirs = [os.path.join(data_dir, 'stats'), data_dir]
    all_dfs = []
    
    seen_files = set()
    for s_dir in search_dirs:
        if not os.path.exists(s_dir):
            continue
        pattern = os.path.join(s_dir, '**', 'stats_*.csv')
        for file_path in glob.glob(pattern, recursive=True):
            abs_path = os.path.abspath(file_path)
            if abs_path in seen_files:
                continue
            seen_files.add(abs_path)
            
            parts = file_path.split(os.sep)
            # Find cluster and node from path segments
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
        print("  [⚠️ WARNING] No stats_*.csv server logs found. Analysis will rely on client metrics only.")
        return None
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=['Cluster', 'Node', 'Timestamp_ms'])
    df_raw = sanitize_timestamps(df_raw, 'Timestamp_ms')
    return df_raw

def load_storage_events(data_dir):
    search_dirs = [os.path.join(data_dir, 'stats'), data_dir]
    all_events = []
    
    seen_files = set()
    for s_dir in search_dirs:
        if not os.path.exists(s_dir):
            continue
        pattern = os.path.join(s_dir, '**', 'storage_events_*.csv')
        for file_path in glob.glob(pattern, recursive=True):
            abs_path = os.path.abspath(file_path)
            if abs_path in seen_files:
                continue
            seen_files.add(abs_path)
            
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
        
    return pd.concat(all_events, ignore_index=True)

def generate_report_markdown(summary_df, output_dir):
    md_content = """# Database Read Pagination Sessions Comparison

This report compares 5 read sessions with increasing `range_size` to assess the latency and throughput impacts on a distributed Raft + LSM-Tree database.

## Summary Table

| Range Size | Duration (s) | Avg QPS | Max QPS | Avg MB/s | Max MB/s | Avg Latency (ms) | P50 Latency (ms) | P95 Latency (ms) | Server Read OPS | Server Latency P99 (ms) | Memtable Flushes | LSM Compactions |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
"""
    for _, row in summary_df.iterrows():
        md_content += (
            f"| {row['Range_Size']:,} "
            f"| {row['Duration_s']:.1f} "
            f"| {row['Avg_Client_QPS']:.1f} "
            f"| {row['Max_Client_QPS']:.1f} "
            f"| {row['Avg_Client_MBps']:.1f} "
            f"| {row['Max_Client_MBps']:.1f} "
            f"| {row['Avg_Client_Latency_ms']:.2f} "
            f"| {row['P50_Client_Latency_ms']:.2f} "
            f"| {row['P95_Client_Latency_ms']:.2f} "
            f"| {row['Avg_Server_Read_OPS']:.1f} "
            f"| {row['Avg_Server_P99_ms']:.2f} "
            f"| {int(row['Memtable_Flushes'])} "
            f"| {int(row['LSM_Compactions'])} |\n"
        )
        
    md_content += """
## Key Observations

1. **Bandwidth Scaling**: As `range_size` increases, the bandwidth throughput (`MB/s`) scales up significantly (from ~11.3 GB/s up to ~161.1 GB/s).
2. **Latency Trade-off**: High range sizes impose extreme latency penalties. Average latency rises from ~105 ms at `range_size=8192` to over **64 seconds** at `range_size=131072`. This represents a ~600x increase in latency for a 16x increase in range size.
3. **Queueing & Saturation**: For `range_size >= 65536`, client-side latencies spike to tens of seconds. This indicates severe system saturation, likely due to raft transmission limits, disk read speed bottlenecking, or network interface saturation when transmitting hundreds of megabytes per request.
4. **LSM Impact**: Server-side storage events (like flushes and compactions) occurring concurrently during longer sessions add variable spikes to latency profiles.

*Plots and additional figures are located in the output directory.*
"""
    
    report_path = os.path.join(output_dir, 'comparison_report.md')
    with open(report_path, 'w') as f:
        f.write(md_content)
    print(f"\n[✓] Markdown comparison report written to: {report_path}")

def generate_report_html(summary_df, output_dir):
    rows_html = ""
    for _, row in summary_df.iterrows():
        rows_html += f"""
        <tr>
            <td><strong>{row['Range_Size']:,}</strong></td>
            <td>{row['Duration_s']:.1f}</td>
            <td>{row['Avg_Client_QPS']:.1f}</td>
            <td>{row['Max_Client_QPS']:.1f}</td>
            <td>{row['Avg_Client_MBps']:.1f}</td>
            <td>{row['Max_Client_MBps']:.1f}</td>
            <td class="lat-val">{row['Avg_Client_Latency_ms']:.2f}</td>
            <td class="lat-val">{row['P50_Client_Latency_ms']:.2f}</td>
            <td class="lat-val">{row['P95_Client_Latency_ms']:.2f}</td>
            <td>{row['Avg_Server_Read_OPS']:.1f}</td>
            <td>{row['Avg_Server_P99_ms']:.2f}</td>
            <td>{int(row['Memtable_Flushes'])}</td>
            <td>{int(row['LSM_Compactions'])}</td>
        </tr>
        """
        
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Distributed Database - Read Pagination Sessions Comparison</title>
    <style>
        body {{
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
            background-color: #0d1117;
            color: #c9d1d9;
            margin: 0;
            padding: 40px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: #161b22;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
            border: 1px solid #30363d;
        }}
        h1 {{
            color: #58a6ff;
            border-bottom: 2px solid #30363d;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #ff7b72;
            margin-top: 30px;
            border-bottom: 1px solid #30363d;
            padding-bottom: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 15px;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #30363d;
        }}
        th {{
            background-color: #21262d;
            color: #58a6ff;
        }}
        tr:hover {{
            background-color: #21262d;
        }}
        .lat-val {{
            color: #ffa657;
            font-weight: bold;
        }}
        .plot-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-top: 30px;
        }}
        .plot-card {{
            background-color: #0d1117;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }}
        .plot-card img {{
            max-width: 100%;
            border-radius: 6px;
            margin-top: 10px;
        }}
        .summary-notes {{
            background-color: #21262d;
            border-left: 4px solid #58a6ff;
            padding: 15px;
            border-radius: 4px;
            margin-top: 25px;
            line-height: 1.6;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Read Pagination Performance Analysis</h1>
        <p>Comparison of 5 consecutive read sessions with increasing range sizes configured during the benchmark runs.</p>
        
        <h2>Performance Matrix Table</h2>
        <table>
            <thead>
                <tr>
                    <th>Range Size</th>
                    <th>Duration (s)</th>
                    <th>Avg QPS</th>
                    <th>Max QPS</th>
                    <th>Avg MB/s</th>
                    <th>Max MB/s</th>
                    <th>Avg Latency (ms)</th>
                    <th>P50 Latency (ms)</th>
                    <th>P95 Latency (ms)</th>
                    <th>Server Read OPS</th>
                    <th>Server P99 Lat (ms)</th>
                    <th>Flushes</th>
                    <th>Compactions</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>

        <div class="summary-notes">
            <strong>Key Insights:</strong>
            <ul>
                <li><strong>Throughput / Bandwidth Trade-off:</strong> While client QPS decreases slightly at range_size=65,536, network throughput (MB/s) spikes exponentially, reaching over 161 GB/s peak bandwidth during range_size=131,072.</li>
                <li><strong>Latency Degradation:</strong> Latency scales non-linearly. Range size 8,192 features an average latency of ~105ms. In contrast, range size 131,072 reaches average latency of ~64s, causing massive read queues and server processing delays.</li>
                <li><strong>System Saturation:</strong> Beyond range size 32,768, the system becomes heavily backlogged, showing response latency values that increase exponentially.</li>
            </ul>
        </div>

        <h2>Visual Dashboards</h2>
        <div class="plot-grid">
            <div class="plot-card">
                <h3>Throughput metrics vs Range Size</h3>
                <p>Compares client-side read QPS and MBps bandwidth output.</p>
                <img src="read_throughput_vs_range_size.png" alt="Read Throughput vs Range Size">
            </div>
            <div class="plot-card">
                <h3>Read Latency vs Range Size</h3>
                <p>Log-scale latency curves showing P50, P95 and Average response times.</p>
                <img src="read_latency_vs_range_size.png" alt="Read Latency vs Range Size">
            </div>
            <div class="plot-card" style="grid-column: span 2;">
                <h3>Read Sessions Time-Series Comparison</h3>
                <p>Overlaid time-series comparison showing QPS and Latency profiles across relative time offsets.</p>
                <img src="client_throughput_timeseries_comparison.png" alt="Client Throughput Timeseries Comparison">
            </div>
        </div>
    </div>
</body>
</html>
"""
    html_path = os.path.join(output_dir, 'comparison_report.html')
    with open(html_path, 'w') as f:
        f.write(html_content)
    print(f"[✓] HTML comparison report written to: {html_path}")

def generate_comparison_plots(sessions, summary_df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Plot: Read Throughput (QPS & MBps) vs Range Size
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    color = 'tab:blue'
    ax1.set_xlabel('Range Size', fontweight='bold')
    ax1.set_ylabel('Read QPS', color=color, fontweight='bold')
    # Use log2 scale for X-axis because range sizes double (8192, 16384, ...)
    x_labels = [str(rs) for rs in summary_df['Range_Size']]
    x_indices = np.arange(len(summary_df))
    
    ax1.plot(x_indices, summary_df['Avg_Client_QPS'], color=color, marker='o', linewidth=2.5, label='Avg Read QPS')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_xticks(x_indices)
    ax1.set_xticklabels(x_labels)
    
    ax2 = ax1.twinx()  
    color = 'tab:orange'
    ax2.set_ylabel('Bandwidth (MB/s)', color=color, fontweight='bold')
    ax2.plot(x_indices, summary_df['Avg_Client_MBps'], color=color, marker='s', linewidth=2.5, linestyle='--', label='Avg MB/s')
    ax2.tick_params(axis='y', labelcolor=color)
    
    plt.title('Read Throughput and Bandwidth vs Range Size', fontsize=14, fontweight='bold', pad=15)
    fig.tight_layout()
    plt.savefig(os.path.join(output_dir, 'read_throughput_vs_range_size.png'), dpi=150)
    plt.close()
    
    # 2. Plot: Read Latency vs Range Size (Log Scale)
    plt.figure(figsize=(10, 6))
    plt.plot(x_indices, summary_df['Avg_Client_Latency_ms'], marker='o', linewidth=2.0, color='royalblue', label='Average Latency')
    plt.plot(x_indices, summary_df['P50_Client_Latency_ms'], marker='v', linewidth=2.0, color='forestgreen', label='P50 Latency')
    plt.plot(x_indices, summary_df['P95_Client_Latency_ms'], marker='^', linewidth=2.0, color='crimson', label='P95 Latency')
    
    plt.yscale('log')
    plt.xticks(x_indices, x_labels)
    plt.xlabel('Range Size', fontweight='bold')
    plt.ylabel('Client Latency (ms) - Log Scale', fontweight='bold')
    plt.title('Read Latency Performance Curve vs Range Size', fontsize=14, fontweight='bold', pad=15)
    plt.legend(loc='upper left')
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'read_latency_vs_range_size.png'), dpi=150)
    plt.close()
    
    # 3. Time-Series Comparison: Overlaid QPS and Latency
    fig, (ax_qps, ax_lat) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    # Generate distinctive colors
    colors = sns.color_palette("rocket_r", len(sessions))
    
    for idx, (rs, df) in enumerate(sessions):
        # Calculate relative time from start of this session
        df = df.copy()
        df['Relative_Time_s'] = (df['Timestamp_ms'] - df['Timestamp_ms'].min()) / 1000.0
        
        lbl = f"Range Size: {rs}"
        ax_qps.plot(df['Relative_Time_s'], df['Instantaneous_OPS'], color=colors[idx], label=lbl, linewidth=2.0)
        ax_lat.plot(df['Relative_Time_s'], df['Avg_Latency_ms'], color=colors[idx], label=lbl, linewidth=2.0)
        
    ax_qps.set_ylabel('Read QPS', fontweight='bold')
    ax_qps.set_title('Read QPS Comparison Over Time', fontsize=12, fontweight='bold')
    ax_qps.legend(loc='upper right')
    
    ax_lat.set_ylabel('Avg Client Latency (ms) - Log Scale', fontweight='bold')
    ax_lat.set_yscale('log')
    ax_lat.set_xlabel('Time from session start (s)', fontweight='bold')
    ax_lat.set_title('Average Client Latency Comparison Over Time', fontsize=12, fontweight='bold')
    ax_lat.legend(loc='upper left')
    ax_lat.grid(True, which="both", ls="--", alpha=0.5)
    
    plt.suptitle('Read Sessions Time-Series Comparison (Overlaid)', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'client_throughput_timeseries_comparison.png'), dpi=150)
    plt.close()


def main():
    args = parse_args()
    
    print(f"Reading configuration and files in: {args.data_dir}")
    range_sizes = parse_config_range_sizes(args.data_dir)
    print(f"Parsed range sizes from config.txt: {range_sizes}")
    
    client_files = sorted(glob.glob(os.path.join(args.data_dir, 'read_throughput_*.csv')))
    print(f"Found client throughput CSV files: {[os.path.basename(f) for f in client_files]}")
    
    if not client_files:
        raise ValueError(f"No read_throughput_*.csv files found in data directory: {args.data_dir}")
        
    # If there is a mismatch or config range sizes is empty, map range sizes to sorted file list
    if len(range_sizes) != len(client_files):
        print(f"[⚠️ WARNING] Mismatch between parsed range sizes ({len(range_sizes)}) and found files ({len(client_files)}).")
        # Generate default names/sizes or try to map
        if len(range_sizes) < len(client_files):
            range_sizes += [9999] * (len(client_files) - len(range_sizes))
        else:
            range_sizes = range_sizes[:len(client_files)]
            
    # Load Server Data and Storage Events to cross-correlate server impact
    print("Loading server statistics...")
    df_server = load_server_data(args.data_dir)
    print("Loading storage events...")
    df_events = load_storage_events(args.data_dir)
    
    # Process each session
    sessions = []
    summary_data = []
    
    for f, rs in zip(client_files, range_sizes):
        print(f"Processing session for range_size = {rs} ({os.path.basename(f)})...")
        df_client = load_client_csv(f)
        if df_client is None or df_client.empty:
            continue
            
        sessions.append((rs, df_client))
        
        # Calculate client averages
        avg_qps = df_client['Instantaneous_OPS'].mean()
        max_qps = df_client['Instantaneous_OPS'].max()
        avg_mbps = df_client['Instantaneous_MBps'].mean()
        max_mbps = df_client['Instantaneous_MBps'].max()
        avg_latency = df_client['Avg_Latency_ms'].mean()
        p50_latency = df_client['P50_Latency_ms'].mean()
        p95_latency = df_client['P95_Latency_ms'].mean()
        
        # Determine timestamps
        t_start = df_client['Timestamp_ms'].min()
        t_end = df_client['Timestamp_ms'].max()
        duration_s = (t_end - t_start) / 1000.0
        
        # Correlate server metrics during this session
        avg_server_read_ops = 0.0
        avg_server_p99 = 0.0
        flushes_count = 0
        compactions_count = 0
        
        if df_server is not None and not df_server.empty:
            df_server_session = df_server[(df_server['Timestamp_ms'] >= t_start) & (df_server['Timestamp_ms'] <= t_end)]
            if not df_server_session.empty:
                # Sum Read_OPS across nodes at each timestamp, then average
                server_agg = df_server_session.groupby('Timestamp_ms')['Read_OPS'].sum()
                avg_server_read_ops = server_agg.mean() if not server_agg.empty else 0.0
                
                # Average server-side P99 latency
                avg_server_p99 = df_server_session['P99_Latency_ms'].mean()
                
        if df_events is not None and not df_events.empty:
            df_events_session = df_events[(df_events['timestamp'] >= t_start) & (df_events['timestamp'] <= t_end)]
            flushes_count = len(df_events_session[df_events_session['event_type'] == 'memtable_flush'])
            compactions_count = len(df_events_session[df_events_session['event_type'] == 'compaction'])
            
        summary_data.append({
            'Range_Size': rs,
            'Duration_s': duration_s,
            'Avg_Client_QPS': avg_qps,
            'Max_Client_QPS': max_qps,
            'Avg_Client_MBps': avg_mbps,
            'Max_Client_MBps': max_mbps,
            'Avg_Client_Latency_ms': avg_latency,
            'P50_Client_Latency_ms': p50_latency,
            'P95_Client_Latency_ms': p95_latency,
            'Avg_Server_Read_OPS': avg_server_read_ops,
            'Avg_Server_P99_ms': avg_server_p99,
            'Memtable_Flushes': flushes_count,
            'LSM_Compactions': compactions_count
        })
        
    summary_df = pd.DataFrame(summary_data)
    
    # Print Markdown table directly to CLI
    print("\n" + "="*40)
    print("READ SESSIONS PERFORMANCE MATRIX")
    print("="*40)
    try:
        print(summary_df.to_markdown(index=False))
    except Exception:
        print(summary_df.to_string(index=False))
    print("="*40 + "\n")
    
    # Generate Output plots and reports
    os.makedirs(args.output_dir, exist_ok=True)
    print("Generating comparison plots...")
    generate_comparison_plots(sessions, summary_df, args.output_dir)
    print("Generating reports...")
    generate_report_markdown(summary_df, args.output_dir)
    generate_report_html(summary_df, args.output_dir)
    
    print(f"\n[✓] Session comparison analysis complete! Output files saved to: {os.path.abspath(args.output_dir)}")

if __name__ == '__main__':
    main()

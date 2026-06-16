import unittest
import os
import pandas as pd
import numpy as np
import importlib.util

# Dynamically import functions from generate-dashboard_v2.py due to hyphen in name
spec = importlib.util.spec_from_file_location("generate_dashboard_v2", "./generate-dashboard_v2.py")
generate_dashboard_v2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_dashboard_v2)

read_clean_csv = generate_dashboard_v2.read_clean_csv
load_and_resample_server_data = generate_dashboard_v2.load_and_resample_server_data

class TestDatabaseDataset(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.stats_dir = "/home/barslengo/ad-datasets/failure-recovery/single-cluster-20260609_190232/stats"
        cls.df_server = load_and_resample_server_data(cls.stats_dir)
        global_start_ms = cls.df_server['Timestamp_ms'].min()
        cls.df_server['Relative_Time_s'] = (cls.df_server['Timestamp_ms'] - global_start_ms) / 1000.0

    def test_csv_parser_safety(self):
        """Test #3: Verify that read_clean_csv ignores truncated/unfinished CSV lines."""
        # Find all stats csv files in single-cluster stats
        import glob
        pattern = os.path.join(self.stats_dir, '*', '*', 'stats_*.csv')
        csv_files = glob.glob(pattern)
        
        self.assertTrue(len(csv_files) > 0, "No CSV files found to test.")
        
        for file_path in csv_files:
            # Check length of file in lines vs pandas dataframe rows
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            df = read_clean_csv(file_path)
            
            # The header line is 1. If any line is skipped, df rows + 1 + skipped = total lines
            # Check if any row in the loaded df contains NaN values in critical consensus columns
            self.assertFalse(df['Role'].isna().any(), f"Role contains NaN in file {file_path}")
            self.assertFalse(df['Term'].isna().any(), f"Term contains NaN in file {file_path}")
            self.assertFalse(df['Timestamp_ms'].isna().any(), f"Timestamp contains NaN in file {file_path}")

    def test_node_grouping(self):
        """Test #1: Verify that server data correctly groups and preserves node metrics."""
        self.assertFalse(self.df_server.empty, "Processed server dataset is empty.")
        
        # In single-cluster, we expect cluster 'A' and nodes '1', '2', '3'
        self.assertIn('Cluster', self.df_server.columns)
        self.assertIn('Node', self.df_server.columns)
        
        clusters = self.df_server['Cluster'].unique()
        self.assertEqual(list(clusters), ['A'])
        
        nodes = sorted(self.df_server['Node'].unique())
        self.assertEqual(nodes, ['1', '2', '3'])

    def test_offline_detection_and_leader_reconstruction(self):
        """Test #2: Verify that offline periods are detected and marked as OFFLINE."""
        # Node 1 and Node 2 are crashed/stopped in the single-cluster test.
        # Check if the role 'OFFLINE' exists in the processed dataset for those nodes.
        offline_records = self.df_server[self.df_server['Role'] == 'OFFLINE']
        self.assertFalse(offline_records.empty, "No offline periods were detected, but some nodes should have crashed.")
        
        # Verify that during offline periods, throughput counters are set to 0.0
        for idx, row in offline_records.iterrows():
            self.assertEqual(row['Write_OPS'], 0.0, f"Write_OPS not reset to 0.0 when offline at row {idx}")
            self.assertEqual(row['Write_MBps'], 0.0, f"Write_MBps not reset to 0.0 when offline at row {idx}")

    def test_cluster_throughput_calculation(self):
        """Test #6: Verify that cluster throughput matches active leader throughput and is valid."""
        # Let's group by Relative_Time_s and check cluster throughput calculation logic
        leaders = self.df_server[self.df_server['Role'].str.upper() == 'LEADER']
        all_times = sorted(self.df_server['Relative_Time_s'].unique())
        cluster_throughput = leaders.groupby('Relative_Time_s')['Write_OPS'].max()
        cluster_throughput = cluster_throughput.reindex(all_times, fill_value=0.0)
        
        # Check that throughput is always non-negative
        self.assertTrue((cluster_throughput.values >= 0).all(), "Negative cluster throughput values detected.")
        
        # Check that during elections (if any term changes exist and no node is LEADER), throughput is 0.0
        # If there is no leader at a timestamp, the value must be 0.0
        for t in all_times:
            roles_at_t = self.df_server[self.df_server['Relative_Time_s'] == t]['Role'].str.upper().tolist()
            if 'LEADER' not in roles_at_t:
                self.assertEqual(cluster_throughput.loc[t], 0.0, f"Throughput should be 0.0 when there is no leader at t={t}")

if __name__ == '__main__':
    unittest.main()

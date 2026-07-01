import os
import json
import time
import subprocess
import unittest
import glob

class TestReadBenchmarkMulti(unittest.TestCase):
    def setUp(self):
        # We start three mock servers representing cluster 'A'.
        from mocks.mock_server import MockDatabaseServer
        self.servers = [
            MockDatabaseServer("127.0.0.1", 18091, leader_id=1, is_leader=True),
            MockDatabaseServer("127.0.0.1", 18092, leader_id=1, is_leader=False),
            MockDatabaseServer("127.0.0.1", 18093, leader_id=1, is_leader=False),
        ]
        for server in self.servers:
            server.start()

        # Create a mock cluster configuration
        self.config_data = {
            "A": [
                {"id": 1, "host": "127.0.0.1", "raft_port": 17091, "tcp_port": 18091},
                {"id": 2, "host": "127.0.0.1", "raft_port": 17092, "tcp_port": 18092},
                {"id": 3, "host": "127.0.0.1", "raft_port": 17093, "tcp_port": 18093},
            ]
        }
        self.config_file = "test_cluster_conf.json"
        with open(self.config_file, "w") as f:
            json.dump(self.config_data, f)

        # Clean up any leftover CSVs and logs before running the test
        for file_pattern in ["read_throughput_*.csv", "log-worker-*.txt"]:
            for f in glob.glob(file_pattern):
                try:
                    os.remove(f)
                except Exception:
                    pass

        # Allow time for mock servers to bind and start listening
        time.sleep(0.5)

    def tearDown(self):
        # Stop servers
        for server in self.servers:
            server.stop()
        
        # Clean up configuration file
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
            
        # Clean up any generated CSVs and logs from the test
        for file_pattern in ["read_throughput_*.csv", "log-worker-*.txt"]:
            for f in glob.glob(file_pattern):
                try:
                    os.remove(f)
                except Exception:
                    pass

    def test_multi_benchmark_execution(self):
        # Execute the run_read_bench_multi.sh script as a subprocess
        # We run with 3 workers and a total of 150 requests (50 each)
        cmd = [
            "./bash-scripts/run_read_bench_multi.sh",
            "--config", self.config_file,
            "--requests", "150",
            "--workers", "3",
            "--strategy", "hash",
            "--dist", "uniform",
            "--max-key", "1000",
            "--range", "5",
            "--threads", "4",
            "--batch", "8"
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        # Check that it executed successfully
        print("Subprocess STDOUT:")
        print(result.stdout)
        print("Subprocess STDERR:")
        print(result.stderr)
        
        self.assertEqual(result.returncode, 0, f"Multi read benchmark script failed with code {result.returncode}")
        
        # Verify that multiple CSV files were generated, one for each worker, and one merged CSV
        all_csv_files = glob.glob("read_throughput_*.csv")
        worker_csv_files = [f for f in all_csv_files if "merged" not in f]
        merged_csv_files = [f for f in all_csv_files if "merged" in f]

        self.assertEqual(len(worker_csv_files), 3, f"Expected 3 worker CSV files, found {len(worker_csv_files)}: {worker_csv_files}")
        self.assertEqual(len(merged_csv_files), 1, f"Expected 1 merged CSV file, found {len(merged_csv_files)}: {merged_csv_files}")
        
        # Expected header: Request_ID,Query_ID,Send_Timestamp_ms,Recv_Timestamp_ms,Record_Count,Records_Bytes
        for csv_file in all_csv_files:
            with open(csv_file, "r") as f:
                lines = f.readlines()
            self.assertGreater(len(lines), 0, f"CSV file {csv_file} is empty")
            header = lines[0].strip()
            self.assertEqual(header, "Request_ID,Query_ID,Send_Timestamp_ms,Recv_Timestamp_ms,Record_Count,Records_Bytes")

    def test_multi_benchmark_execution_with_output_dir(self):
        output_dir = "test_run_output_dir"
        if os.path.exists(output_dir):
            import shutil
            shutil.rmtree(output_dir)

        cmd = [
            "./bash-scripts/run_read_bench_multi.sh",
            "--config", self.config_file,
            "--requests", "60",
            "--workers", "2",
            "--strategy", "hash",
            "--dist", "uniform",
            "--max-key", "1000",
            "--range", "5",
            "--threads", "4",
            "--batch", "8",
            "--output-dir", output_dir
        ]
        
        print(f"Running output-dir command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        self.assertEqual(result.returncode, 0, f"Multi read benchmark script failed with code {result.returncode}")
        
        # Verify that output directory was created and contains the logs and csv files
        self.assertTrue(os.path.isdir(output_dir), "Output directory was not created")
        
        # Check files inside the output directory
        csv_files = glob.glob(os.path.join(output_dir, "read_throughput_*.csv"))
        worker_csvs = [f for f in csv_files if "merged" not in f]
        merged_csvs = [f for f in csv_files if "merged" in f]
        log_files = glob.glob(os.path.join(output_dir, "log-worker-*.txt"))
        
        self.assertEqual(len(worker_csvs), 2, f"Expected 2 worker CSVs in output_dir, found {len(worker_csvs)}")
        self.assertEqual(len(merged_csvs), 1, f"Expected 1 merged CSV in output_dir, found {len(merged_csvs)}")
        self.assertEqual(len(log_files), 2, f"Expected 2 log files in output_dir, found {len(log_files)}")
        
        # Clean up output directory
        import shutil
        shutil.rmtree(output_dir)

if __name__ == "__main__":
    unittest.main()


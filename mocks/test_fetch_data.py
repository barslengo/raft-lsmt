import os
import subprocess
import unittest
import glob
import shutil

class TestFetchData(unittest.TestCase):
    def setUp(self):
        self.created_dirs = []

    def tearDown(self):
        # Clean up any created directories
        for d in self.created_dirs:
            if os.path.exists(d):
                shutil.rmtree(d)
        
        # Additional glob cleanup just in case
        for d in glob.glob("stats-folder-*") + glob.glob("my-custom-prefix-*"):
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_default_prefix(self):
        cmd = ["./bash-scripts/fetch-data.sh"]
        # Timeout quickly since scp attempts will fail (which is fine, it handles them gracefully)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        # The script should exit with 0 even if scp fails due to the || echo handler
        self.assertEqual(result.returncode, 0)
        
        # Check that a stats-folder-<timestamp> was created
        dirs = glob.glob("stats-folder-*")
        self.assertGreater(len(dirs), 0, "Default stats-folder was not created")
        self.created_dirs.extend(dirs)

    def test_custom_prefix(self):
        cmd = ["./bash-scripts/fetch-data.sh", "-p", "my-custom-prefix"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        self.assertEqual(result.returncode, 0)
        
        # Check that my-custom-prefix-<timestamp> was created
        dirs = glob.glob("my-custom-prefix-*")
        self.assertGreater(len(dirs), 0, "Custom prefix folder was not created")
        self.created_dirs.extend(dirs)

    def test_move_csv_files(self):
        # Create dummy CSV files in current directory
        dummy_write_csv = "client_throughput_test.csv"
        dummy_read_csv = "read_throughput_test.csv"
        
        with open(dummy_write_csv, "w") as f:
            f.write("dummy write metrics\n")
        with open(dummy_read_csv, "w") as f:
            f.write("dummy read metrics\n")
            
        try:
            cmd = ["./bash-scripts/fetch-data.sh", "-p", "test-move", "-m", "5"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            self.assertEqual(result.returncode, 0)
            
            # Check that test-move-<timestamp> was created
            dirs = glob.glob("test-move-*")
            self.assertGreater(len(dirs), 0, "Test folder was not created")
            self.created_dirs.extend(dirs)
            
            target_dir = dirs[0]
            
            # Verify that the CSV files are no longer in current directory but are in target_dir
            self.assertFalse(os.path.exists(dummy_write_csv), "Dummy write CSV was not moved from current dir")
            self.assertFalse(os.path.exists(dummy_read_csv), "Dummy read CSV was not moved from current dir")
            
            self.assertTrue(os.path.exists(os.path.join(target_dir, dummy_write_csv)), "Dummy write CSV was not found in target dir")
            self.assertTrue(os.path.exists(os.path.join(target_dir, dummy_read_csv)), "Dummy read CSV was not found in target dir")
        finally:
            # Cleanup in case of failures
            if os.path.exists(dummy_write_csv):
                os.remove(dummy_write_csv)
            if os.path.exists(dummy_read_csv):
                os.remove(dummy_read_csv)

if __name__ == "__main__":
    unittest.main()

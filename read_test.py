import socket
import struct
import time
import argparse
import random
import sys
from collections import deque

# --- Configuration based on C Code ---
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)]
REQUEST_FMT = "<Q Q Q Q Q" # Little Endian: ReqID, StartID, StartTS, EndID, EndTS
REQUEST_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_KEY (16)] [MAX_KEY (16)]
# Note: C code does manual memcpy, so it is tightly packed (no padding)
RESP_HEADER_FMT = "<Q Q B Q Q Q Q" 
RESP_HEADER_SIZE = 8 + 8 + 1 + 16 + 16 # 49 bytes

# Body Header: [BODY_SIZE (8)] [COUNT (4)]
RESP_BODY_HEAD_FMT = "<Q I"
RESP_BODY_HEAD_SIZE = 12

# Record Header: [KEY (16)] [TYPE (1)] [LEN (4)]
RECORD_HEAD_FMT = "<Q Q B I"
RECORD_HEAD_SIZE = 21

class RaftReadClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.connect()

    def connect(self):
        if self.sock:
            try: self.sock.close()
            except: pass
        
        print(f"[Client] Connecting to {self.host}:{self.port}...")
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(30.0)
            self.sock.connect((self.host, self.port))
            print("[Client] Connected.")
        except Exception as e:
            print(f"[Client] Connection failed: {e}")
            sys.exit(1)

    def _read_exact(self, n):
        data = bytearray()
        while len(data) < n:
            try:
                chunk = self.sock.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("Socket closed by server")
                data.extend(chunk)
            except socket.timeout:
                raise ConnectionError("Socket timeout during read")
        return bytes(data)

    def send_query(self, req_id, start_key, end_key):
        """
        Sends a single query packet and parses the response.
        start_key/end_key are tuples (id, timestamp).
        Returns: (records_list, limit_reached, max_key_tuple)
        """
        # 1. Pack Request
        # Key format: (id, ts)
        req_data = struct.pack(REQUEST_FMT, 
                               req_id, 
                               start_key[0], start_key[1], 
                               end_key[0], end_key[1])
        
        try:
            self.sock.sendall(req_data)

            # 2. Read Response Header
            header_bytes = self._read_exact(RESP_HEADER_SIZE)
            (total_msg_size, res_req_id, limit_flag, 
             min_id, min_ts, max_id, max_ts) = struct.unpack(RESP_HEADER_FMT, header_bytes)

            limit_reached = bool(limit_flag)
            min_key = (min_id, min_ts)
            max_key = (max_id, max_ts)

            # 3. Read Body Header
            body_head_bytes = self._read_exact(RESP_BODY_HEAD_SIZE)
            body_size, count = struct.unpack(RESP_BODY_HEAD_FMT, body_head_bytes)

            # 4. Read Records
            records = []
            for _ in range(count):
                # Read Record Header
                rec_head = self._read_exact(RECORD_HEAD_SIZE)
                k_id, k_ts, d_type, d_len = struct.unpack(RECORD_HEAD_FMT, rec_head)
                
                # Read Record Data
                data = self._read_exact(d_len)
                
                # Convert primitive (assuming int for benchmark consistency)
                val = int.from_bytes(data, byteorder='little')
                records.append({
                    'key': (k_id, k_ts),
                    'val': val
                })

            return records, limit_reached, min_key, max_key

        except (socket.error, ConnectionError) as e:
            print(f"[Client] Error during query: {e}")
            self.connect() # Reconnect for next attempt
            #return [], False, (0,0), (0,0)
            return None

def get_next_key(key_tuple):
    """ 
    Calculates key + 1 for pagination.
    Logic: Increment timestamp. If overflow, increment ID.
    """
    k_id, k_ts = key_tuple
    if k_ts < (2**64 - 1):
        return (k_id, k_ts + 1)
    else:
        return (k_id + 1, 0)

def get_prev_key(key_tuple):
    """ 
    Calculates key - 1. 
    Logic: Decrement timestamp. If underflow, decrement ID.
    Returns None if key is (0,0).
    """
    k_id, k_ts = key_tuple
    if k_ts > 0:
        return (k_id, k_ts - 1)
    else:
        if k_id > 0:
            return (k_id - 1, 2**64 - 1)
        else:
            return None # Cannot go below 0

def run_paged_query_loop(client, full_start_key, full_end_key):
    """
    Performs a full range scan using a Work Queue to handle gaps.
    """
    # Queue stores tuples: (start_key, end_key)
    work_queue = deque([(full_start_key, full_end_key)])
    
    total_records = 0
    page_num = 0
    start_time = time.time()

    print(f"\n>>> Starting Queue Scan: {full_start_key} -> {full_end_key}")

    while work_queue:
        # Pop the next range to process
        curr_start, curr_end = work_queue.popleft()
        
        # Optimization: If start > end due to key math, skip
        if curr_start > curr_end:
            continue

        req_id = random.randint(1, 1000000)
        
        # Call Server
        result = client.send_query(req_id, curr_start, curr_end)
        
        if result is None:
            # Network failure: Push back to queue to retry later
            print("Network failure... pushing back to queue")
            work_queue.appendleft((curr_start, curr_end))
            time.sleep(0.5)
            continue

        records, limit_reached, batch_min_key, batch_max_key = result
        
        count = len(records)
        total_records += count
        page_num += 1

        if page_num % 100 == 0:
            print(f"    Page {page_num}: Range [{curr_start[0]}..{curr_end[0]}] -> [{batch_min_key[0]}..{batch_max_key[0]}] -> Fetched {count}. Limit? {limit_reached}")

        if count > 0:
            if not limit_reached:
                continue

            if batch_min_key > batch_max_key:
                print("empty?")
                continue
            
            # If the server returned keys ending at 800, but we asked for 1000...
            # We need to query [801... 1000]  
            if batch_max_key < curr_end:
                tmp = get_next_key(batch_max_key)
                if tmp and tmp <= curr_end:
                    work_queue.append((tmp, curr_end))

    duration = time.time() - start_time
    print(f"<<< Scan Complete. Pages: {page_num} Total: {total_records} records in {duration:.4f}s")

def main():
    parser = argparse.ArgumentParser(description="Raft Read Client with Paging")
    parser.add_argument("--host", default="127.0.0.1", help="Server Host")
    parser.add_argument("--port", type=int, default=11001, help="Read Port (Base+4000)")
    parser.add_argument("--loops", type=int, default=10, help="Number of full scans to run")
    args = parser.parse_args()

    client = RaftReadClient(args.host, args.port)

    # Example: Scan the entire possible range (0 to Max)
    # Or a smaller random range if you prefer
    
    for i in range(args.loops):
        # Pick a random range to keep it interesting
        # Assuming keys were generated with random ID and current timestamp
        
        # Wide range to force paging
        start_id = 1
        end_id = 2**64 - 1 
        
        start_key = (start_id, 0)
        end_key = (end_id, 2**64 - 1)

        run_paged_query_loop(client, start_key, end_key)
        
        time.sleep(0.5)

if __name__ == "__main__":
    main()

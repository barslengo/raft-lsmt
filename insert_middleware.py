import socket
import struct
import threading
import time
import json
import argparse
import random
import sys

# --- Configuration ---
LSMT_TYPE_INT = 1
# Protocol: [MsgLen(4)] [KeyID(8)] [KeyTS(8)] [InnerType(1)] [InnerLen(4)] [Val(8)]
# The provided snippet calculates this manually, which is correct.
PIPELINE_DEPTH = 4096 

# --- Global Shared State ---

# 1. Pre-computed source data: List of (binary_packet, metadata_dict)
ALL_REQUESTS = []

# 2. History of what has actually been written to the DB (for Reads/Verification)
COMMITTED_HISTORY = [] 
history_lock = threading.Lock()

# 3. Statistics
stats_lock = threading.Lock()
stop_event = threading.Event()

# Counters
total_committed_count = 0  # Writes (Requests)
total_batches_sent = 0     # Network Batches

def create_insert_request(seq_id):
    """ 
    Creates a valid, serialized insert request. 
    Format matches server expectation: [Len][KeyID][KeyTS][ValType][ValLen][Val]
    """
    real_content = seq_id # Use seq_id to ensure uniqueness for this test
    real_content_size = 8 

    # Inner Payload: [Type(1)][Len(4)][Data(8)]
    inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, real_content_size, real_content)
    inner_payload_size = len(inner_payload)

    # Key generation
    key_id = real_content 
    key_timestamp = int(time.time() * 1000)

    # Outer Payload: [KeyID][KeyTS][InnerPayload]
    # INSERT_CMD_FORMAT_PREFIX = "<QQ" defined implicitly here
    outer_payload_format = f"<QQ{inner_payload_size}s"
    outer_payload = struct.pack(
            outer_payload_format,
            key_id,
            key_timestamp,
            inner_payload 
            )

    # Prefix with Total Message Length (4 bytes)
    # The server expects: [TotalLen] [Payload]
    # TotalLen = sizeof(TotalLen) + sizeof(Payload) ? 
    # Usually in your C code: msg_size is read, then msg_size-4 is read.
    # Standard: Total size of the frame.
    packet_len = 4 + len(outer_payload)
    message_length_prefix = struct.pack("<I", packet_len)
    binary_packet = message_length_prefix + outer_payload

    metadata = {
            'id': key_id,
            'ts': key_timestamp,
            'val': real_content
            }

    return binary_packet, metadata 

def read_exact(sock, n):
    """Reads exactly n bytes from socket, handling short reads."""
    data = bytearray()
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
            if not chunk: raise Exception("Socket closed")
            data.extend(chunk)
        except socket.timeout:
            continue 
    return bytes(data)

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)["A"]

# -------------------------------------------------------------------------
# RAFT CLIENT (Network Layer)
# -------------------------------------------------------------------------
class RaftClient:
    def __init__(self, cluster_conf):
        self.nodes = cluster_conf
        self.sock = None
        self.current_node_idx = random.randint(0, len(self.nodes) - 1)
        
    def connect_to_leader(self):
        """
        Round-robin attempts to connect to nodes. 
        The server closes connection if not leader, so we try next on failure.
        """
        while True:
            if self.sock:
                try: self.sock.close()
                except: pass
            
            node = self.nodes[self.current_node_idx]
            
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # socket.sendall blocks if buffer full, allowing backpressure
                self.sock.connect((node['host'], node['port']))
                return
            except Exception:
                # Connection refused or timeout
                self.current_node_idx = (self.current_node_idx + 1) % len(self.nodes)
                time.sleep(0.1)

    def send_batch_reliable(self, packets):
        """
        Concatenates packets and sends them.
        Waits for exactly 1 byte of ACK per packet from the server.
        Retries infinitely until success (Strong consistency).
        """
        batch_data = b''.join(packets)
        expected_ack_bytes = len(packets)
        
        while True:
            try:
                if self.sock is None:
                    self.connect_to_leader()

                # 1. Send Data
                self.sock.sendall(batch_data)
                
                # 2. Wait for ACKs
                # The server sends 1 byte for every processed command.
                # We must read exactly that many bytes to confirm persistence.
                _ = read_exact(self.sock, expected_ack_bytes)
                
                # If we get here, success
                return

            except Exception as e:
                # print(f"Batch failed ({e}), finding new leader...")
                self.sock = None # Force reconnect
                self.current_node_idx = (self.current_node_idx + 1) % len(self.nodes)
                # Loop continues and retries the SAME batch

# -------------------------------------------------------------------------
# WRITE WORKER
# -------------------------------------------------------------------------
def write_worker(thread_id, cluster_conf, start_idx, end_idx):
    """
    Takes a slice of ALL_REQUESTS and sends them via RaftClient.
    """
    # print(f"[Writer-{thread_id}] Started. Processing items {start_idx} to {end_idx}.")
    
    client = RaftClient(cluster_conf)
    
    current_batch_pkts = []
    current_batch_meta = []
    
    local_committed = 0
    
    for i in range(start_idx, end_idx):
        packet, meta = ALL_REQUESTS[i]
        
        current_batch_pkts.append(packet)
        current_batch_meta.append(meta)

        # Flush Batch
        if len(current_batch_pkts) >= PIPELINE_DEPTH:
            # 1. Send to Raft (Blocking until ACK)
            client.send_batch_reliable(current_batch_pkts)
            
            # 2. Update Stats & History
            count = len(current_batch_pkts)
            with stats_lock:
                global total_committed_count, total_batches_sent
                total_committed_count += count
                total_batches_sent += 1
            
            with history_lock:
                COMMITTED_HISTORY.extend(current_batch_meta)
            
            local_committed += count
            
            # Reset
            current_batch_pkts = []
            current_batch_meta = []

    # Flush remaining
    if current_batch_pkts:
        client.send_batch_reliable(current_batch_pkts)
        count = len(current_batch_pkts)
        with stats_lock:
            total_committed_count += count
            total_batches_sent += 1
        with history_lock:
            COMMITTED_HISTORY.extend(current_batch_meta)
        local_committed += count

    # print(f"[Writer-{thread_id}] Finished. Committed {local_committed}.")

# -------------------------------------------------------------------------
# STATS REPORTER
# -------------------------------------------------------------------------
def report_stats():
    last_ops = 0
    while not stop_event.is_set():
        time.sleep(1.0)
        with stats_lock:
            curr_ops = total_committed_count
            curr_batches = total_batches_sent
        
        diff_ops = curr_ops - last_ops
        last_ops = curr_ops
        
        print(f"[INSERT] Rate: {diff_ops} ops/s | Total: {curr_ops} | Batches: {curr_batches}")

# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Insert Middleware (Batching)")
    parser.add_argument("--config", required=True, help="Path to cluster_conf.json")
    parser.add_argument("--threads", type=int, default=1, help="Number of threads")
    parser.add_argument("--count", type=int, default=100000, help="Total items to generate and insert")
    args = parser.parse_args()

    cluster_conf = load_config(args.config)
    print(f"Loaded config with {len(cluster_conf)} nodes.")

    # 1. Pre-compute Data
    print(f"Generating {args.count} requests in memory...")
    t0 = time.time()
    for i in range(args.count):
        ALL_REQUESTS.append(create_insert_request(i))
    print(f"Generation took {time.time() - t0:.2f}s. Buffer ready.")

    # 2. Start Stats Thread
    stats_thread = threading.Thread(target=report_stats, daemon=True)
    stats_thread.start()

    # 3. Spawn Workers
    workers = []
    items_per_thread = args.count // args.threads
    
    start_time = time.time()

    for i in range(args.threads):
        start_idx = i * items_per_thread
        # Ensure last thread gets remainder
        end_idx = start_idx + items_per_thread if i < args.threads - 1 else args.count
        
        t = threading.Thread(target=write_worker, args=(i, cluster_conf, start_idx, end_idx))
        t.start()
        workers.append(t)

    # 4. Wait for completion
    for t in workers:
        t.join()
    
    stop_event.set()
    duration = time.time() - start_time
    
    print(f"\n[DONE] Inserted {total_committed_count} items in {duration:.2f}s.")
    print(f"Average Throughput: {total_committed_count/duration:.2f} ops/s")

if __name__ == "__main__":
    main()

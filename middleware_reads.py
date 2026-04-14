import socket
import struct
import time
import threading
import argparse
import random
import sys
import collections

# --- Protocol Configuration ---
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)]
REQUEST_FMT = "<Q Q Q Q Q" 
REQUEST_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_KEY (16)] [MAX_KEY (16)]
# Total Size = Header (49) + Body
RESP_HEADER_FMT = "<Q Q B Q Q Q Q" 
RESP_HEADER_SIZE = 49 

# --- Shared State ---
# Maps ReqID -> StartTime (ns)
# Using a thread-safe dict (in CPython dicts are atomic for single ops, but lock is safer)
pending_requests = {} 
pending_lock = threading.Lock()

stats_lock = threading.Lock()
stats = {
    "sent": 0,
    "received": 0,
    "bytes_recv": 0,
    "total_latency_ns": 0,
    "errors": 0
}

stop_event = threading.Event()

class MiddlewareClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.connect()

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.connect((self.host, self.port))
            print(f"[System] Connected to {self.host}:{self.port}")
        except Exception as e:
            print(f"[System] Connection failed: {e}")
            sys.exit(1)

    def close(self):
        if self.sock:
            self.sock.close()

# --- Sender Thread ---
def sender_thread(client, target_qps):
    """
    Continually sends queries with random keys.
    Does NOT wait for response.
    """
    print("[Sender] Thread started.")
    req_id_seq = 0
    
    # Pre-pack reusable parts if possible, but keys are random here
    delay = 1.0 / target_qps if target_qps > 0 else 0

    while not stop_event.is_set():
        try:
            req_id = req_id_seq
            req_id_seq += 1

            # Generate Random Range
            # Simulate a mix of point lookups (start=end) and ranges
            range_width = random.choice([0, 10, 100]) 
            start_id = random.randint(0, 2**64 - range_width - 1)
            end_id = start_id + range_width
            
            # Pack
            req_data = struct.pack(REQUEST_FMT, 
                                   req_id, 
                                   start_id, 0,  # Start Key (ID, TS)
                                   end_id, 0)    # End Key

            # Record timestamp BEFORE sending
            with pending_lock:
                pending_requests[req_id] = time.perf_counter_ns()

            # Send (Atomic in Python for bytes)
            client.sock.sendall(req_data)

            with stats_lock:
                stats["sent"] += 1

            if delay > 0:
                time.sleep(delay)

        except Exception as e:
            print(f"[Sender] Error: {e}")
            with stats_lock: stats["errors"] += 1
            stop_event.set()
            break

# --- Receiver Thread ---
def receiver_thread(client):
    """
    Continually reads from the socket.
    Parses headers, matches ReqID to pending list, calculates latency.
    """
    print("[Receiver] Thread started.")
    
    def read_exact(n):
        buf = bytearray()
        while len(buf) < n:
            chunk = client.sock.recv(n - len(buf))
            if not chunk: raise ConnectionError("Socket closed")
            buf.extend(chunk)
        return buf

    while not stop_event.is_set():
        try:
            # 1. Read Header (49 Bytes)
            # This blocks until data is available
            header_bytes = read_exact(RESP_HEADER_SIZE)
            
            # Unpack
            (total_msg_size, req_id, limit_flag, 
             min_id, min_ts, max_id, max_ts) = struct.unpack(RESP_HEADER_FMT, header_bytes)

            # 2. Read Body
            # The total_msg_size includes the header (49 bytes). 
            # We need to read the remainder.
            body_size = total_msg_size - RESP_HEADER_SIZE
            if body_size > 0:
                _ = read_exact(body_size) # Consume and discard body for stats

            # 3. Calculate Stats
            now_ns = time.perf_counter_ns()
            
            with pending_lock:
                start_ns = pending_requests.pop(req_id, None)

            if start_ns:
                latency = now_ns - start_ns
                with stats_lock:
                    stats["received"] += 1
                    stats["bytes_recv"] += total_msg_size
                    stats["total_latency_ns"] += latency
            else:
                print(f"[Receiver] Warning: Received unknown ReqID {req_id}")

        except ConnectionError:
            print("[Receiver] Server disconnected.")
            stop_event.set()
            break
        except Exception as e:
            if not stop_event.is_set():
                print(f"[Receiver] Error: {e}")
                stop_event.set()
            break

# --- Monitor Thread ---
def monitor_thread():
    print(f"{'Time':<8} | {'Sent':<8} | {'Recv':<8} | {'Pending':<8} | {'RPS (Recv)':<10} | {'Latency (avg)':<15} | {'Bandwidth'}")
    print("-" * 90)
    
    start_time = time.time()
    last_recv = 0
    last_bytes = 0
    
    while not stop_event.is_set():
        time.sleep(1.0)
        
        with stats_lock:
            s_sent = stats["sent"]
            s_recv = stats["received"]
            s_bytes = stats["bytes_recv"]
            s_lat = stats["total_latency_ns"]
            # Reset latency accumulator to get instant average
            stats["total_latency_ns"] = 0
            period_count = s_recv - last_recv
        
        with pending_lock:
            pending = len(pending_requests)

        elapsed = time.time() - start_time
        
        # Calculations
        rps = period_count
        bw_mb = (s_bytes - last_bytes) / (1024 * 1024)
        
        avg_lat_ms = 0
        if period_count > 0:
            avg_lat_ms = (s_lat / period_count) / 1_000_000 # ns to ms

        print(f"{elapsed:<8.1f} | {s_sent:<8} | {s_recv:<8} | {pending:<8} | {rps:<10} | {avg_lat_ms:<15.3f} | {bw_mb:.2f} MB/s")

        last_recv = s_recv
        last_bytes = s_bytes

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Middleware Emulation (Threaded Pipeline)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11001)
    parser.add_argument("--qps", type=int, default=0, help="Target QPS (0 = max)")
    args = parser.parse_args()

    client = MiddlewareClient(args.host, args.port)

    # Threads
    sender = threading.Thread(target=sender_thread, args=(client, args.qps))
    receiver = threading.Thread(target=receiver_thread, args=(client,))
    monitor = threading.Thread(target=monitor_thread)

    sender.start()
    receiver.start()
    monitor.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[System] Stopping...")
        stop_event.set()

    sender.join()
    # Close socket to unblock receiver if it's stuck in recv
    client.close() 
    receiver.join()
    monitor.join()

    print("[System] Finished.")

if __name__ == "__main__":
    main()

import socket
import struct
import time
import random
import argparse

PIPELINE_DEPTH = 1024 

LSMT_TYPE_INT = 1 

# The format for the outer command struct: key (16 bytes) + content_size (1 byte)
# <  : Little-endian
# QQ : Two uint64_t for the 128-bit key
INSERT_CMD_FORMAT_PREFIX = "<QQ"

def create_insert_request():
    """
    Creates a valid, serialized insert request. This function correctly
    builds the inner payload in the [type][size][data] format that the
    lsmt.c storage engine expects, and then wraps it in the outer command.
    """
    # --- Step 1: Create the "Real Content" ---
    # This is the actual value you want to store.
    # We will simulate storing a 8-byte unsigned integer.
    real_content = random.randint(0, 2**64 - 1)
    real_content_size = 8 # This corresponds to sizeof(uint64_t)

    # --- Step 2: Create the "Inner Payload" (the value for the LSM-Tree) ---
    # This is the buffer that lsmt_insert() will receive.
    # Format: [content_type (1 byte)] [size_of_real_content (4 bytes)] [real_content (N bytes)]
    inner_payload = struct.pack(
        "<BIQ",  # B=uint8_t, I=uint32_t, Q=uint64_t
        LSMT_TYPE_INT,
        real_content_size,
        real_content
    )
    # The total size of this inner payload. This will become the `content_size` in the outer command.
    inner_payload_size = len(inner_payload) # This will be 1 + 4 + 8 = 13 bytes

    # --- Step 3: Create the "Outer Command" (the full Raft log entry) ---
    # Format: [key (16 bytes)] [content_size (1 byte)] [inner_payload (N bytes)]
    #key_id = random.randint(0, 2**64 - 1)
    key_id = real_content
    key_timestamp = int(time.time())

    # The format string for the full command payload.
    # The 's' format specifier takes a byte string.
    outer_payload_format = f"{INSERT_CMD_FORMAT_PREFIX}{inner_payload_size}s"
    
    outer_payload = struct.pack(
        outer_payload_format,
        key_id,
        key_timestamp,
        #inner_payload_size, # This is the value for the `content_size` field.
        inner_payload       # This is the value for the `encoded_data` field.
    )

    # --- Step 4: Prepend the entire message with its 4-byte network length ---
    message_length_prefix = struct.pack("<I", 4 + len(outer_payload))
    
    return message_length_prefix + outer_payload

def recv_exact(sock, n_bytes):
    """
    Helper to ensure we receive exactly n_bytes. 
    Socket.recv() might return fewer than requested.
    """
    data = b''
    while len(data) < n_bytes:
        chunk = sock.recv(n_bytes - len(data))
        if not chunk:
            raise ConnectionError("Server closed connection unexpectedly")
        data += chunk
    return data

def benchmark(host, port, num_requests):
    print(f"Connecting to {host}:{port}...")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        
        print(f"Sending {num_requests} requests (Pipeline Depth: {PIPELINE_DEPTH})...")
        
        start_time = time.time()
        pending_acks = 0

        for i in range(num_requests):
            # 1. Create and Send Request
            req = create_insert_request()
            s.sendall(req)
            pending_acks += 1

            # 2. Flow Control: Wait for ACKs if pipeline is full
            # This puts the client to sleep until the Server's Raft log is committed.
            if pending_acks >= PIPELINE_DEPTH:
                # We expect 1 byte per request
                recv_exact(s, pending_acks)
                pending_acks = 0

        # 3. Drain remaining ACKs (if total requests isn't divisible by 128)
        if pending_acks > 0:
            recv_exact(s, pending_acks)

        end_time = time.time()
        duration = end_time - start_time
        rps = num_requests / duration if duration > 0 else 0

        print("\n--- Benchmark Results ---")
        print(f"Total requests sent: {num_requests}")
        print(f"Total time taken:    {duration:.4f} seconds")
        print(f"Requests per second: {rps:.2f} (RPS)")
        print(f"Avg Latency per batch: {(duration / (num_requests/PIPELINE_DEPTH))*1000:.2f} ms")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark a distributed database.")
    parser.add_argument("--host", default="127.0.0.1", help="The host of the database server.")
    parser.add_argument("--port", type=int, default=7001, help="The port of the database server.")
    parser.add_argument("--requests", type=int, default=10000, help="The total number of requests to send.")
    args = parser.parse_args()

    benchmark(args.host, args.port, args.requests)
    #benchmark_2(args.host, args.port, args.requests)

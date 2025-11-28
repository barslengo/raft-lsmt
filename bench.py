import socket
import struct
import time
import random
import argparse

# Define constants from the C code for consistency
LSMT_TYPE_INT = 1 # Corresponds to the C macro

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

def benchmark_2(host, port, num_requests):
    """
    Connects to the server, sends a specified number of valid insert requests,
    and measures the resulting performance.
    """
    print(f"Connecting to server at {host}:{port}...")
    start_time = time.time()
    try:
        for _ in range(num_requests):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                #print(f"Connection successful. Sending {num_requests} requests...")
                request_message = create_insert_request()
                s.sendall(request_message)
    except ConnectionRefusedError:
       print(f"\nError: Connection refused. Is the server running on {host}:{port}?")
    except Exception as e:
       print(f"\nAn unexpected error occurred: {e}")

    end_time = time.time()

    duration = end_time - start_time
    rps = num_requests / duration if duration > 0 else float('inf')

    print("\n--- Benchmark Results ---")
    print(f"Total requests sent: {num_requests}")
    print(f"Total time taken:    {duration:.2f} seconds")
    print(f"Requests per second: {rps:.2f} (RPS)")


def benchmark(host, port, num_requests):
    """
    Connects to the server, sends a specified number of valid insert requests,
    and measures the resulting performance.
    """
    print(f"Connecting to server at {host}:{port}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            print(f"Connection successful. Sending {num_requests} requests...")

            start_time = time.time()

            for _ in range(num_requests):
                request_message = create_insert_request()
                s.sendall(request_message)

            end_time = time.time()
            
            duration = end_time - start_time
            rps = num_requests / duration if duration > 0 else float('inf')

            print("\n--- Benchmark Results ---")
            print(f"Total requests sent: {num_requests}")
            print(f"Total time taken:    {duration:.2f} seconds")
            print(f"Requests per second: {rps:.2f} (RPS)")

    except ConnectionRefusedError:
        print(f"\nError: Connection refused. Is the server running on {host}:{port}?")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark a distributed database.")
    parser.add_argument("--host", default="127.0.0.1", help="The host of the database server.")
    parser.add_argument("--port", type=int, default=7001, help="The port of the database server.")
    parser.add_argument("--requests", type=int, default=10000, help="The total number of requests to send.")
    args = parser.parse_args()

    benchmark(args.host, args.port, args.requests)
    #benchmark_2(args.host, args.port, args.requests)

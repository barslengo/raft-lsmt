import asyncio
import argparse
import random
import struct
import time

LSMT_TYPE_INT = 1
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

async def send_one_request(host, port):
    """
    This is the core function for the new logic.
    It connects, sends ONE request, and immediately disconnects.
    Returns True on success, False on failure.
    """
    writer = None
    try:
        # STEP 1: Open a new connection for this single request.
        _, writer = await asyncio.open_connection(host, port)
        
        # STEP 2: Send the single request.
        writer.write(create_insert_request())
        await writer.drain()
        
        return True # Success
    except Exception:
        return False # Failure
    finally:
        # STEP 3: Immediately close the connection.
        if writer:
            writer.close()
            await writer.wait_closed()

async def user_worker(host, port, num_requests, results_queue):
    """
    Simulates a single concurrent user.
    This worker now calls the 'send_one_request' function in a loop.
    """
    success_count = 0
    for _ in range(num_requests):
        # Each iteration of this loop performs a full connect-send-disconnect cycle.
        if await send_one_request(host, port):
            success_count += 1
    
    # Report the number of successful requests to the main thread.
    await results_queue.put(success_count)

async def main():
    parser = argparse.ArgumentParser(
        description="Massively concurrent benchmark: NEW CONNECTION PER REQUEST."
    )
    parser.add_argument("--host", default="127.0.0.1", help="The server host.")
    parser.add_argument("--port", type=int, default=7001, help="The server port.")
    parser.add_argument("--users", "-u", type=int, default=100, help="Number of concurrent users.")
    parser.add_argument("--requests-per-user", "-n", type=int, default=10, help="Number of requests each user will send (each with a new connection).")
    args = parser.parse_args()

    print("--- Starting Connection-per-Request Benchmark ---")
    print("!!! WARNING: This mode is very demanding on the OS and will yield lower RPS. !!!")
    print(f"Simulating {args.users} concurrent users...")
    print(f"Each user will send {args.requests_per_user} requests, opening a NEW connection for each one.")
    print("-------------------------------------------------")

    start_time = time.monotonic()
    
    # A queue is a safe way to gather results from many concurrent tasks.
    results_queue = asyncio.Queue()
    
    # Create a task for each simulated user.
    tasks = [
        user_worker(args.host, args.port, args.requests_per_user, results_queue)
        for _ in range(args.users)
    ]

    # Run all user simulations concurrently.
    await asyncio.gather(*tasks)

    end_time = time.monotonic()
    
    total_requests_sent = 0
    while not results_queue.empty():
        total_requests_sent += await results_queue.get()
    
    duration = end_time - start_time
    rps = total_requests_sent / duration if duration > 0 else float('inf')

    print("\n--- Benchmark Results ---")
    print(f"Total time taken:      {duration:.2f} seconds")
    print(f"Total requests sent:   {total_requests_sent}")
    print(f"Requests Per Second:   {rps:.2f} (RPS)")
    print("-------------------------")
    print("(This RPS measures connection handling AND request processing)")

if __name__ == "__main__":
    # You MUST increase your OS's open file and port limits for this test!
    # Linux: ulimit -n 100000
    # Also, you may need to adjust TCP TIME_WAIT settings for very high rates.
    asyncio.run(main())

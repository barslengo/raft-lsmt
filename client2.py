import asyncio
import random
import struct
import argparse
from datetime import datetime

import aiohttp
from tqdm import tqdm

# --- Configuration ---
DEFAULT_URL = "http://127.0.0.1:8001"
DEFAULT_REQUESTS = 100_000
DEFAULT_CONCURRENCY = 10  # Number of concurrent requests to have in flight

# --- Payload Generation ---
def generate_binary_payload():
    """
    Generates a binary payload with the structure:
    - 128-byte random key
    - 4-byte value size (big-endian)
    - n-byte random value
    """
    content_type = 1
    # 1. Generate a random 16-byte key
    key = random.randbytes(16)

    # 2. Generate a random value (e.g., between 32 and 256 bytes)
    #value_size = random.randint(32, 64)
    value_size = 128
    value = random.randbytes(value_size)

    format_string = f'=16sBI{value_size}s'

    # 3. Pack all three components into a single bytes object.
    #    The arguments must match the format string's order.
    payload = struct.pack(format_string, key, content_type, value_size, value)

    return payload 

# --- Worker Logic ---
async def worker(session, url, pbar, stats):
    """
    A single worker that continuously sends requests.
    """
    while True:
        try:
            payload = generate_binary_payload()
            headers = {'Content-Type': 'application/octet-stream'}
            
            async with session.post(url, data=payload, headers=headers) as response:
                # We want to process the response quickly to free up the connection.
                # Here we just check the status.
                if response.status >= 200 and response.status < 300:
                    stats['success'] += 1
                else:
                    stats['failure'] += 1
                
                # Ensure the response body is read and connection is released
                await response.read()

        except aiohttp.ClientError:
            stats['failure'] += 1
        except Exception:
            stats['failure'] += 1
        finally:
            # Update the progress bar for every completed request
            pbar.update(1)


async def main(url: str, total_requests: int, concurrency: int):
    """
    Main function to set up and run the stress test.
    """
    stats = {"success": 0, "failure": 0}
    
    print(f"Starting stress test with {total_requests:,} requests to {url} using {concurrency} workers.")
    
    # tqdm is our progress bar
    with tqdm(total=total_requests, unit="req", ncols=100) as pbar:
        start_time = datetime.now()
        
        # Create a single session to be shared by all workers for connection pooling
        async with aiohttp.ClientSession() as session:
            # Create a queue that workers will pull tasks from
            queue = asyncio.Queue()

            # Add all "tasks" (just numbers for counting) to the queue
            for _ in range(total_requests):
                await queue.put(None)

            # Define the task that the worker will execute
            async def run_task(worker_id):
                while not queue.empty():
                    try:
                        await queue.get()
                        await worker(session, url, pbar, stats)
                        queue.task_done()
                    except asyncio.CancelledError:
                        break

            # Start the desired number of concurrent worker tasks
            tasks = [asyncio.create_task(run_task(i)) for i in range(concurrency)]

            # Wait for the queue to be fully processed
            await queue.join()

            # Cancel all worker tasks
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rps = total_requests / duration if duration > 0 else float('inf')

        print("\n--- Test Complete ---")
        print(f"Total Requests: {total_requests:,}")
        print(f"Successful:     {stats['success']:,}")
        print(f"Failed:         {stats['failure']:,}")
        print(f"Duration:       {duration:.2f} seconds")
        print(f"Requests/Sec:   {rps:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="High-performance HTTP POST stress tool.")
    parser.add_argument("-u", "--url", default=DEFAULT_URL, help="The URL to target.")
    parser.add_argument("-n", "--requests", type=int, default=DEFAULT_REQUESTS, help="Total number of requests to send.")
    parser.add_argument("-c", "--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Number of concurrent requests.")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args.url, args.requests, args.concurrency))
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")

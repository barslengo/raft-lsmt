import socket
import struct
import threading
import time
import random
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from collections import defaultdict
from router import Router
from types import Node, Record, QueryRequest

@dataclass(frozen=True)
class DbClientConfig:
    thread_pool_size: int = 32
    batch_size: int = 4096
    write_timeout: float = 5.0
    read_timeout: float = 10.0
    max_retries: int = 3

class DbClient:
    def __init__(self, config: DbClientConfig, router: Router):
        self.config = config
        self.thread_pool = ThreadPoolExecutor(max_workers=config.thread_pool_size)
        self._sockets: Dict[Node, socket.socket] = {}  # Socket pool
        self._socket_locks: Dict[Node, threading.Lock] = defaultdict(threading.Lock)
        self._write_buffer: Dict[Node, List[Tuple[int, int, Any]]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self.clusters: Dict[str, List[Node]] = {}
        self.router = router 

    # ====================== CONNECTION MANAGEMENT ======================
    def connect(self, clusters: Dict[str, List[Node]]):
        """
        Establish connections to all nodes (leaders and replicas).
        Update the leader registry if a leader is found during connection.
        """
        self.clusters = clusters
        for cluster_name, nodes in clusters.items():
            for node in nodes:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((node.host, node.port))
                    self._sockets[node] = sock
                    # Assume the first node is the leader (or use a discovery protocol)
                    if not self.router.leader_registry.get_leader(cluster_name):
                        self.router.leader_registry.set_leader(node)
                except Exception as e:
                    print(f"Failed to connect to {node}: {e}")

    def disconnect(self):
        """Close all sockets and clean up."""
        for node, sock in self._sockets.items():
            try:
                sock.close()
            except:
                pass
        self._sockets.clear()

    def _get_socket(self, node: Node) -> socket.socket:
        """Get or create a socket for a node (thread-safe)."""
        with self._socket_locks[node]:
            if node not in self._sockets:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((node.host, node.port))
                self._sockets[node] = sock
            return self._sockets[node]

    # ====================== WRITE OPERATIONS ======================
    def write(self, records: List[Tuple[int, int, Any]], verbose: bool = False):
        """
        Buffer records and flush in batches.
        If the leader fails, retry on the new leader.
        """
        with self._buffer_lock:
            for record in records:
                key_id, timestamp, content = record
                node = self.router.get_node_insert(record)

                self._write_buffer[node].append(record)
                if len(self._write_buffer[node]) >= self.config.batch_size:
                    self._flush_batch(node)

        if verbose:
            return {"status": "buffered", "pending": sum(len(v) for v in self._write_buffer.values())}

    def _flush_batch(self, node: Node):
        """
        Flush a batch of records to a node.
        If the batch request fails or timeout, try to resend.

        TODO: if the node is not connecting or after max_retries the batch
        still isnt correctly stored on server, then trigger the function to
        discover the new leader.
        """
        with self._buffer_lock:
            batch = self._write_buffer[node]
            self._write_buffer[node] = []

        if not batch:
            return

        def _send_batch():
            retries = 0
            while retries < self.config.max_retries:
                try:
                    sock = self._get_socket(node)
                    payload = self._serialize_batch(batch)
                    sock.sendall(payload)
                    # Wait for ACKs (1 byte per record)
                    acks = self._recv_exact(sock, len(batch))
                    if len(acks) != len(batch):
                        raise RuntimeError(f"Incomplete ACKs from {node}")
                    return
                except Exception as e:
                    retries += 1
                    self.router.leader_registry.clear_leader(node)
                    if retries >= self.config.max_retries:
                        raise RuntimeError(f"Failed to send batch to {node} after {retries} retries: {e}")
                    # Wait and retry on new leader
                    time.sleep(0.1 * retries)

        self.thread_pool.submit(_send_batch)

    def _serialize_batch(self, batch: List[Tuple[int, int, Any]]) -> bytes:
        """Serialize a batch of records into a single payload."""
        payload = bytearray()
        for key_id, timestamp, content in batch:
            # Example: <I (msg_size) Q (key_id) Q (timestamp) ... (content)>
            inner_payload = struct.pack("<BIQ", 1, 8, key_id)  # LSMT_TYPE_INT, size=8, content
            outer_payload = struct.pack("<QQ", key_id, timestamp) + inner_payload
            payload.extend(struct.pack("<I", len(outer_payload)) + outer_payload)
        return bytes(payload)

    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        """Receive exactly n bytes (blocking)."""
        data = bytearray()
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket closed")
            data.extend(chunk)
        return bytes(data)

    def flush(self):
        """Force-flush all buffered writes."""
        with self._buffer_lock:
            for node, batch in self._write_buffer.items():
                if batch:
                    self._flush_batch(node)
                    self._write_buffer[node] = []

    # ====================== READ OPERATIONS ======================
    def read_sync(
        self,
        min_id: int,
        min_ts: int,
        max_id: int,
        max_ts: int,
        verbose: bool = False,
        timeout: float = None,
    ) -> Dict[str, Any]:
        """
        Blocking read: query one node per cluster and return aggregated results.
        If `verbose=True`, return {cluster: {node: response}}.
        If `timeout` is set, raise `TimeoutError` if not all responses arrive in time.
        """
        query = (min_id, min_ts, max_id, max_ts)
        nodes = self.router.get_nodes_for_read(query)

        futures = []
        for node in nodes:
            future = self.thread_pool.submit(
                self._query_single_node, node, min_id, min_ts, max_id, max_ts
            )
            futures.append(future)

        results = {}
        try:
            for future in as_completed(futures, timeout=timeout):
                node, response = future.result()
                if verbose:
                    if node.cluster_name not in results:
                        results[node.cluster_name] = {}
                    results[node.cluster_name][node] = response
                else:
                    results.update(response)
        except Exception as e:
            for f in futures:
                f.cancel()
            raise TimeoutError(f"read_interval timed out: {e}") from e

        return results

    def send_read_request(
        self,
        min_id: int,
        min_ts: int,
        max_id: int,
        max_ts: int,
        callback: Optional[Callable[[Node, Dict, float, Optional[Exception]], None]] = None,
        verbose: bool = False,
    ):
        """
        Fire-and-forget read request to one node per cluster.
        If `callback` is provided, it will be called with:
            callback(node, response, latency_seconds, error=None)
        """
        query = (min_id, min_ts, max_id, max_ts)
        nodes = self.router.get_nodes_for_read(query)
        send_time = time.time()

        for node in nodes:
            def _query_and_callback(node, send_time):
                query_start = time.time()
                try:
                    node, response = self._query_single_node(node, min_id, min_ts, max_id, max_ts)
                    latency = time.time() - query_start
                    if callback:
                        callback(node, response, latency, error=None)
                except Exception as e:
                    if callback:
                        callback(node, None, time.time() - query_start, error=e)

            self.thread_pool.submit(_query_and_callback, node, send_time)

    def _query_single_node(self, node: Node, min_id: int, min_ts: int, max_id: int, max_ts: int) -> Tuple[Node, Dict]:
        """Query a single node and return (node, response)."""
        try:
            sock = self._get_socket(node)
            req_data = struct.pack(QUERY_REQ_FMT, random.randint(1, 1000000), min_id, min_ts, max_id, max_ts)
            sock.sendall(req_data)

            # Read response header
            header_data = self._recv_exact(sock, QUERY_RESP_HEADER_SIZE)
            if not header_data:
                raise ConnectionError(f"Node {node} closed connection")

            unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
            total_size = unpacked[0]
            body_size = total_size - QUERY_RESP_HEADER_SIZE

            # Read body (if any)
            response = list(unpacked[1:])  # Exclude total_size
            if body_size > 0:
                body_data = self._recv_exact(sock, body_size)
                response.extend(struct.unpack(f"<{body_size}s", body_data))

            return (node, self._parse_response(response))
        except Exception as e:
            with self._socket_locks[node]:
                if node in self._sockets:
                    try:
                        self._sockets[node].close()
                    except:
                        pass
                    del self._sockets[node]
            raise RuntimeError(f"Query to {node} failed: {e}")

    def _parse_response(self, unpacked_data):
        """Parse raw response into a dict."""
        return {
            "req_id": unpacked_data[0],
            "min_id": unpacked_data[1],
            "min_ts": unpacked_data[2],
            "max_id": unpacked_data[3],
            "max_ts": unpacked_data[4],
            "records_bytes": unpacked_data[5],
            "records_count": unpacked_data[6],
        }

import socket
import struct
import threading
import time
import random
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from collections import defaultdict
from .router import Router
from .types import Node, Record, QueryRequest

# Protocol constants matching the server
LSMT_TYPE_INT = 1

# Query Protocol
# Request: [REQ_ID (8)] [START_KEY_ID (8)] [START_TS (8)] [END_KEY_ID (8)] [END_TS (8)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_ID (8)] [MIN_TS (8)] [MAX_ID (8)] [MAX_TS (8)] [RECORDS_BYTES (8)] [RECORDS_COUNT (4)]
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q Q I"
QUERY_RESP_HEADER_SIZE = 61

INSERT_CMD_FORMAT_PREFIX = "<QQ"  # Key (u64), Timestamp (u64)


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
        self._write_buffer: Dict[Node, List[Record]] = defaultdict(list)
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
    def write(self, records: List[Record], verbose: bool = False):
        """
        Buffer records and flush in batches.
        If the leader fails, retry on the new leader.
        """
        with self._buffer_lock:
            for record in records:
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
        If the node is not connecting or after max_retries the batch
        still isnt correctly stored on server, then discover the new leader.
        """
        with self._buffer_lock:
            batch = self._write_buffer[node]
            self._write_buffer[node] = []

        if not batch:
            return

        def _send_batch():
            retries = 0
            current_node = node
            while retries < self.config.max_retries:
                try:
                    sock = self._get_socket(current_node)
                    payload = self._serialize_batch(batch)
                    sock.sendall(payload)
                    # Wait for ACKs (1 byte per record)
                    acks = self._recv_exact(sock, len(batch))
                    if len(acks) != len(batch):
                        raise RuntimeError(f"Incomplete ACKs from {current_node}")
                    return
                except Exception as e:
                    retries += 1
                    self.router.leader_registry.clear_leader(current_node)
                    
                    # Try to discover new leader by iterating through cluster nodes
                    if retries < self.config.max_retries:
                        cluster_name = current_node.cluster_name
                        nodes = self.clusters.get(cluster_name, [])
                        # Find next node in the cluster
                        try:
                            current_idx = nodes.index(current_node)
                        except ValueError:
                            current_idx = 0
                        next_idx = (current_idx + 1) % len(nodes)
                        current_node = nodes[next_idx]
                    
                    if retries >= self.config.max_retries:
                        raise RuntimeError(f"Failed to send batch to {current_node} after {retries} retries: {e}")
                    # Wait and retry on new node/leader
                    time.sleep(0.1 * retries)

        self.thread_pool.submit(_send_batch)

    def _serialize_batch(self, batch: List[Record]) -> bytes:
        """Serialize a batch of records into a single payload."""
        payload = bytearray()
        for record in batch:
            # Each record: msg_size (4) + outer_payload
            # outer_payload: key_id (8) + timestamp (8) + inner_payload
            # inner_payload: type (1) + size (1) + content (variable)
            inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, 8, record.content)
            outer_payload = struct.pack("<QQ", record.key_id, record.timestamp) + inner_payload
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
        query: QueryRequest,
        verbose: bool = False,
        timeout: float = None,
    ) -> Dict[str, Any]:
        """
        Blocking read: query one node per cluster and return aggregated results.
        If `verbose=True`, return {cluster: {node: response}}.
        If `timeout` is set, raise `TimeoutError` if not all responses arrive in time.
        """
        nodes = self.router.get_nodes_for_read(query)

        futures = []
        for node in nodes:
            future = self.thread_pool.submit(
                self._query_single_node, node, query
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
        query: QueryRequest,
        callback: Optional[Callable[[Node, Dict, float, Optional[Exception]], None]] = None,
        verbose: bool = False,
    ):
        """
        Fire-and-forget read request to one node per cluster.
        If `callback` is provided, it will be called with:
            callback(node, response, latency_seconds, error=None)
        """
        nodes = self.router.get_nodes_for_read(query)
        send_time = time.time()

        for node in nodes:
            def _query_and_callback(node, send_time):
                query_start = time.time()
                try:
                    node, response = self._query_single_node(node, query)
                    latency = time.time() - query_start
                    if callback:
                        callback(node, response, latency, error=None)
                except Exception as e:
                    if callback:
                        callback(node, None, time.time() - query_start, error=e)

            self.thread_pool.submit(_query_and_callback, node, send_time)

    def _query_single_node(self, node: Node, query: QueryRequest) -> Tuple[Node, Dict]:
        """Query a single node and return (node, response)."""
        try:
            sock = self._get_socket(node)
            req_data = struct.pack(QUERY_REQ_FMT, random.randint(1, 1000000), 
                                   query.min_id, query.min_ts, query.max_id, query.max_ts)
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

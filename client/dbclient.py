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
from .db_datatypes import Node, Record, QueryRequest, BatchMetrics

# Protocol constants matching the server
LSMT_TYPE_INT = 1

# Query Protocol
# Request: [REQ_ID (8)] [START_KEY (16 bytes: id+timestamp)] [END_KEY (16 bytes: id+timestamp)] = 40 bytes
QUERY_REQ_FMT = "<Q 16s 16s"
QUERY_REQ_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_KEY (16)] [MAX_KEY (16)] [RECORDS_BYTES (8)] [RECORDS_COUNT (4)]
QUERY_RESP_HEADER_FMT = "<Q Q B 16s 16s Q I"
QUERY_RESP_HEADER_SIZE = 61

INSERT_CMD_FORMAT_PREFIX = "<QQ"  # Key (u64), Timestamp (u64)


@dataclass(frozen=True)
class DbClientConfig:
    thread_pool_size: int = 16
    batch_size: int = 4096
    write_timeout: float = 5.0
    read_timeout: float = 10.0
    max_retries: int = 10
    write_cb: Callable[[BatchMetrics], None] = None

class DbClient:
    def __init__(self, config: DbClientConfig, router: Router):
        self.config = config
        self.thread_pool = ThreadPoolExecutor(max_workers=config.thread_pool_size)
        self._sockets: Dict[Tuple[Node, bool], socket.socket] = {}  # Socket pool: (node, is_query_port)
        self._socket_locks: Dict[Node, threading.Lock] = defaultdict(threading.Lock)
        self._write_buffer: Dict[Node, List[Record]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self.clusters: Dict[str, List[Node]] = {}
        self.router = router 

    # ====================== CONNECTION MANAGEMENT ======================
    def connect(self, clusters: Dict[str, List[Node]]):
        """
        Establish eager connections ONLY to the actual leader of each cluster.
        Followers are quickly probed and discarded to avoid stale socket buffers.
        """
        self.clusters = clusters
        
        for cluster_name, nodes in clusters.items():
            for node in nodes:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((node.host, node.port))
                    
                    readable, _, _ = select.select([sock], [],[], 0.05)
                    
                    if readable:
                        sock.close()
                    else:
                        sock.settimeout(self.config.write_timeout)
                        self._sockets[(node, False)] = sock
                        self.router.leader_registry.set_leader(node)
                        print(f"Connected eagerly to LEADER {node.id} ({cluster_name}) on port {node.port}")
                        break
                        
                except Exception as e:
                    pass

    def disconnect(self):
        """Close all sockets and clean up."""
        for cache_key, sock in self._sockets.items():
            try:
                sock.close()
            except:
                pass
        self._sockets.clear()

    def _get_socket(self, node: Node, for_query: bool = False) -> socket.socket:
        """Get or create a socket for a node (thread-safe).
        
        Args:
            node: The node to connect to
            for_query: If True, connect to query port (node.port + 4000)
        """
        port = node.port + 4000 if for_query else node.port
        
        with self._socket_locks[node]:
            cache_key = (node, for_query)
            # Check if we have a valid socket
            if cache_key in self._sockets:
                sock = self._sockets[cache_key]
                # Test if socket is still connected
                try:
                    # Use peek to check if socket is readable (non-blocking check)
                    # For simplicity, just try to check if it's still open
                    if sock.fileno() >= 0:
                        return sock
                except (OSError, ValueError):
                    # Socket is closed or invalid, remove it
                    pass
            
            # Create new socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.config.write_timeout if not for_query else self.config.read_timeout)
            sock.connect((node.host, port))
            self._sockets[cache_key] = sock
            return self._sockets[cache_key]

    # ====================== WRITE OPERATIONS ======================
    def write(self, records: List[Record], verbose: bool = False):
        """
        Buffer records and flush exactly in sizes of config.batch_size.
        Leftover records remain in the buffer until flush() is called.
        """
        batches_to_send =[]
        
        node_records = defaultdict(list)
        for record in records:
            node = self.router.get_node_insert(record)
            node_records[node].append(record)
            
        with self._buffer_lock:
            for node, recs in node_records.items():
                self._write_buffer[node].extend(recs)
                
                while len(self._write_buffer[node]) >= self.config.batch_size:
                    batch = self._write_buffer[node][:self.config.batch_size]
                    del self._write_buffer[node][:self.config.batch_size]
                    batches_to_send.append((node, batch))
                    
        futures =[]
        for node, batch in batches_to_send:
            futures.append(self._flush_batch(node, batch))
            
        return futures

    def _flush_batch(self, node: Node, batch: List[Record]) -> Future:
            """
            Flush a batch of records to a node.
            If it receives a REDIRECT, it instantly targets the new leader.
            If it fails, it falls back to other nodes to discover the new leader.

            Returns:
                Future: A Future object that can be used to wait for completion
            """
            if not batch:
                future = Future()
                future.set_result([])
                return future

            def _send_batch():
                cluster_name = node.cluster_name
                nodes = self.clusters.get(cluster_name,[])
                retries = 0

                batch_send_time_ms = int(time.time() * 1000)
 
                while retries < self.config.max_retries:
                    current_node = self.router.leader_registry.get_leader(cluster_name)
                    if not current_node:
                        current_node = nodes[retries % len(nodes)]
                    
                    try:
                        sock = self._get_socket(current_node, for_query=False)
                        payload = self._serialize_batch(batch)
                        
                        try:
                            sock.sendall(payload)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        
                        first_byte = sock.recv(1)
                        if not first_byte:
                            raise ConnectionError("Connection closed by server")
                            
                        if first_byte == b'R':
                            rest = sock.recv(255)
                            msg_str = (b'R' + rest).decode('utf-8', errors='ignore').strip()
                            
                            with self._socket_locks[current_node]:
                                cache_key = (current_node, False)
                                if cache_key in self._sockets:
                                    try: self._sockets[cache_key].close()
                                    except: pass
                                    del self._sockets[cache_key]
                            
                            if msg_str.startswith("REDIRECT"):
                                parts = msg_str.split(" ")
                                print(parts)
                                if len(parts) >= 3:
                                    leader_id = int(parts[1])
                                    print(leader_id)
                                    if leader_id == 0:
                                        raise ConnectionError("Cluster in election...")
                                    
                                    new_leader = next((n for n in nodes if n.id == leader_id), None)
                                    if new_leader:
                                        self.router.leader_registry.set_leader(new_leader)
                                        continue 
                            
                            raise ConnectionError("Invalid Redirect") 
                        else:
                            remaining = len(batch) - 1
                            if remaining > 0:
                                self._recv_exact(sock, remaining, timeout=self.config.write_timeout)
                                
                            self.router.leader_registry.set_leader(current_node)

                            if self.config.write_cb:
                                batch_metrics = BatchMetrics(
                                        send_time_ms=batch_send_time_ms,
                                        ack_recv_time_ms = int(time.time() * 1000),
                                        record_count = len(batch),
                                        batch_bytes = len(payload))

                                self.config.write_cb(batch_metrics)

                            return batch
                            
                    except Exception as e:
                        self.router.leader_registry.clear_leader(current_node)
                        
                        with self._socket_locks[current_node]:
                            cache_key = (current_node, False)
                            if cache_key in self._sockets:
                                try: self._sockets[cache_key].close()
                                except: pass
                                del self._sockets[cache_key]
                        
                        retries += 1
                        time.sleep(5)
                
                raise RuntimeError(f"Failed batch request to the cluster {cluster_name} after {retries} attemps")

            return self.thread_pool.submit(_send_batch)

    def _serialize_batch(self, batch: List[Record]) -> bytes:
        """Serialize a batch of records into a single payload.
        
        Server expects: [msg_size (4)] [record_key (16)] [record_value (variable)]
        where msg_size = 4 + 16 + len(record_value) = total message size
        """
        payload = bytearray()
        for record in batch:
            # inner_payload: type (1) + content_size (4) + content (8)
            inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, 8, record.content)
            # outer_payload: key_id (8) + timestamp (8) + inner_payload (13)
            outer_payload = struct.pack("<QQ", record.key_id, record.timestamp) + inner_payload
            # msg_size = 4 (for msg_size field) + len(outer_payload)
            msg_size = 4 + len(outer_payload)
            payload.extend(struct.pack("<I", msg_size) + outer_payload)
        return bytes(payload)

    def _recv_exact(self, sock: socket.socket, n: int, timeout: Optional[float] = None) -> bytes:
        """Receive exactly n bytes (blocking with timeout)."""
        if timeout is not None:
            sock.settimeout(timeout)
        data = bytearray()
        while len(data) < n:
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise ConnectionError("Socket closed")
                data.extend(chunk)
            except socket.timeout:
                raise ConnectionError("Timeout waiting for data")
        return bytes(data)

    def flush(self):
        """Force-flush all buffered writes and wait for completion."""
        futures = []
        with self._buffer_lock:
            for node, batch in list(self._write_buffer.items()):
                if batch:
                    future = self._flush_batch(node, batch)
                    futures.append(future)
                    self._write_buffer[node] = []
        
        # Wait for all flush operations to complete
        if futures:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error during flush: {e}")
                    raise

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

        """
        result format:
        {
            clusterA: 
                [
                    {"id:ip:port" : (node_info, records)},
                    {"id:ip:port" : (node_info, records)},
                ]
        }
        """
        results = {}
        try:
            for future in as_completed(futures, timeout=timeout):
                node, response = future.result()
                if verbose:
                    if node.cluster_name not in results:
                        results[node.cluster_name] = []
                    node_name = f"{node.id}:{node.host}:{node.port}"
                    results[node.cluster_name].append({node_name: (node, response)})
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
        """Query a single node with pagination support.
        
        If the initial node fails, try other nodes in the same cluster
        (preferring non-leaders first, then leader).
        For each successful node, fetch all records using pagination.
        """
        cluster_name = node.cluster_name
        nodes = self.clusters.get(cluster_name, [])
        
        # Get current leader for this cluster
        leader = self.router.leader_registry.get_leader(cluster_name)
        
        # Build ordered list: non-leaders first, then leader
        non_leaders = [n for n in nodes if n != leader]
        leader_node = [n for n in nodes if n == leader]
        preferred_nodes = non_leaders + leader_node
        
        last_error = None
        for n in preferred_nodes:
            try:
                # Query with pagination
                all_records = []
                current_min_id = query.min_id
                current_min_ts = query.min_ts
                
                while True:
                    # Create page query
                    page_query = QueryRequest(
                        min_id=current_min_id,
                        min_ts=current_min_ts,
                        max_id=query.max_id,
                        max_ts=query.max_ts
                    )
                    
                    sock = self._get_socket(n, for_query=True)
                    start_key = struct.pack("<QQ", page_query.min_id, page_query.min_ts)
                    end_key = struct.pack("<QQ", page_query.max_id, page_query.max_ts)
                    req_data = struct.pack(QUERY_REQ_FMT, random.randint(1, 1000000), start_key, end_key)
                    sock.sendall(req_data)

                    # Read response header
                    header_data = self._recv_exact(sock, QUERY_RESP_HEADER_SIZE, timeout=self.config.read_timeout)
                    if not header_data:
                        raise ConnectionError(f"Node {n} closed connection")

                    unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                    total_size = unpacked[0]
                    req_id = unpacked[1]
                    limit_reached = unpacked[2]
                    min_key_raw = unpacked[3]  # 16 bytes
                    max_key_raw = unpacked[4]  # 16 bytes
                    records_bytes = unpacked[5]
                    records_count = unpacked[6]
                    
                    # Parse min_key and max_key from raw bytes
                    min_id, min_ts = struct.unpack("<QQ", min_key_raw)
                    max_id, max_ts = struct.unpack("<QQ", max_key_raw)
                    
                    body_size = total_size - QUERY_RESP_HEADER_SIZE

                    # Read body (if any)
                    body_data = b""
                    if body_size > 0:
                        body_data = self._recv_exact(sock, body_size, timeout=self.config.read_timeout)

                    response = self._parse_response(req_id, limit_reached, 
                                                    min_id, min_ts, max_id, max_ts,
                                                    records_bytes, records_count, body_data)
                    
                    all_records.extend(response["records"])
                    
                    # Check if we have more records to fetch
                    if not response["limit_reached"]:
                        # All records retrieved for this query range
                        aggregated = {
                            "req_id": req_id,
                            "limit_reached": False,
                            "min_id": query.min_id,
                            "min_ts": query.min_ts,
                            "max_id": max_id if all_records else query.max_id,
                            "max_ts": max_ts if all_records else query.max_ts,
                            "records_bytes": records_bytes,
                            "records_count": len(all_records),
                            "records": all_records
                        }
                        return (n, aggregated)
                    
                    # Pagination: start after the last record we got
                    if max_id >= query.max_id and max_ts >= query.max_ts:
                        # Reached the end of the query range
                        aggregated = {
                            "req_id": req_id,
                            "limit_reached": False,
                            "min_id": query.min_id,
                            "min_ts": query.min_ts,
                            "max_id": query.max_id,
                            "max_ts": query.max_ts,
                            "records_bytes": records_bytes,
                            "records_count": len(all_records),
                            "records": all_records
                        }
                        return (n, aggregated)
                    
                    # Next page: start from the next key
                    current_min_id = max_id + 1
                    current_min_ts = 0
                    
            except Exception as e:
                last_error = e
                cache_key = (n, True)
                with self._socket_locks[n]:
                    if cache_key in self._sockets:
                        try:
                            self._sockets[cache_key].close()
                        except:
                            pass
                        del self._sockets[cache_key]
        
        raise RuntimeError(f"Query to cluster {cluster_name} failed after trying {len(preferred_nodes)} nodes. Last error: {last_error}")

    def _parse_response(self, req_id, limit_reached, min_id, min_ts, max_id, max_ts, 
                         records_bytes, records_count, body_data):
        """Parse raw response into a dict with parsed records."""
        result = {
            "req_id": req_id,
            "limit_reached": bool(limit_reached),
            "min_id": min_id,
            "min_ts": min_ts,
            "max_id": max_id,
            "max_ts": max_ts,
            "records_bytes": records_bytes,
            "records_count": records_count,
            "records": []
        }
        
        if body_data and records_count > 0:
            offset = 0
            for _ in range(records_count):
                # Query response format: key_id (8) + timestamp (8) + type (1) + content_size (4) + content
                if offset + 21 > len(body_data):
                    break
                key_id, timestamp = struct.unpack_from("<QQ", body_data, offset)
                offset += 16
                rec_type = struct.unpack_from("<B", body_data, offset)[0]
                offset += 1
                content_size = struct.unpack_from("<I", body_data, offset)[0]
                offset += 4
                
                if offset + content_size > len(body_data):
                    break
                content_raw = body_data[offset:offset + content_size]
                offset += content_size
                
                # Parse content based on type
                if rec_type == LSMT_TYPE_INT and content_size == 8:
                    content = struct.unpack("<Q", content_raw)[0]
                else:
                    content = content_raw
                
                result["records"].append(Record(
                    key_id=key_id,
                    timestamp=timestamp,
                    content=content
                ))
        
        return result

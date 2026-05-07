import socket
import struct
import threading
import time
import random
import select
import queue
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

        self._socket_pools: Dict[Tuple[Node, bool], queue.Queue] = defaultdict(queue.Queue)

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
                        self._socket_pools[(node, False)].put(sock)
                        self.router.leader_registry.set_leader(node)
                        print(f"Connected eagerly to LEADER {node.id} ({cluster_name}) on port {node.port}")
                        break
                        
                except Exception as e:
                    pass

    def disconnect(self):
        """Close all sockets and clean up."""
        for pool in self._socket_pools.values():
            while not pool.empty():
                try:
                    sock = pool.get_nowait()
                    sock.close()
                except:
                    pass
        self._socket_pools.clear()

    def _get_socket(self, node: Node, for_query: bool = False) -> socket.socket:
        """Get or create a socket for a node (thread-safe)."""
        pool = self._socket_pools[(node, for_query)]

        while True:
            try:
                sock = pool.get_nowait()
                readable, _, _ = select.select([sock], [],[], 0.0)
                if readable:
                    sock.close() # Socket has EOF or junk, discard it
                    continue
                return sock
            except queue.Empty:
                break
                
        # If pool is empty, open a new parallel connection
        port = node.port + 4000 if for_query else node.port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.config.write_timeout if not for_query else self.config.read_timeout)
        sock.connect((node.host, port))
        return sock

    def _return_socket(self, node: Node, sock: socket.socket, for_query: bool = False):
        """Return a healthy socket to the pool for the next thread to use."""
        self._socket_pools[(node, for_query)].put(sock)

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

                sock = None
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
                        
                        sock.close()
                        sock = None
                       
                        if msg_str.startswith("REDIRECT"):
                            parts = msg_str.split(" ")
                            if len(parts) >= 3:
                                leader_id = int(parts[1])
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
                
                        self._return_socket(current_node, sock, for_query=False)
                        self.router.leader_registry.set_leader(current_node)

                        if self.config.write_cb:
                            batch_metrics = BatchMetrics(
                                    send_time_ms=batch_send_time_ms,
                                    ack_recv_time_ms=int(time.time() * 1000),
                                    record_count=len(batch),
                                    batch_bytes=len(payload))
                            self.config.write_cb(batch_metrics)

                        return batch
                            
                except Exception as e:
                    self.router.leader_registry.clear_leader(current_node)
                    if sock:
                        sock.close()

                    retries += 1
                    time.sleep(5)
            
            raise RuntimeError(f"Failed batch request to the cluster {cluster_name} after {retries} attemps")

        return self.thread_pool.submit(_send_batch)

    def _serialize_batch(self, batch: List[Record]) -> bytes:
        payload = bytearray()
        for record in batch:
            inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, 8, record.content)
            outer_payload = struct.pack("<QQ", record.key_id, record.timestamp) + inner_payload
            msg_size = 4 + len(outer_payload)
            payload.extend(struct.pack("<I", msg_size) + outer_payload)
        return bytes(payload)

    def _recv_exact(self, sock: socket.socket, n: int, timeout: Optional[float] = None) -> bytes:
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
        futures =[]
        with self._buffer_lock:
            for node, batch in list(self._write_buffer.items()):
                if batch:
                    future = self._flush_batch(node, batch)
                    futures.append(future)
                    self._write_buffer[node] =[]
        
        if futures:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error during flush: {e}")
                    raise

    # ====================== READ OPERATIONS ======================
    def read(self,
        query: QueryRequest,
        timeout: Optional[float] = None) -> Future[Dict[str, Any]]:
        
        nodes = self.router.get_nodes_for_read(query)
        futures =[]
        for node in nodes:
            future = self.thread_pool.submit(self._query_single_node, node, query)
            futures.append(future)

        def _aggregate():
            results = {}
            try:
                for future in as_completed(futures, timeout=timeout):
                    node, response = future.result()
                    if node.cluster_name not in results:
                        results[node.cluster_name] = []
                    node_name = f"{node.id}:{node.host}:{node.port}"
                    results[node.cluster_name].append({node_name: (node, response)})
            except Exception as e:
                for f in futures:
                    f.cancel()
                raise TimeoutError(f"read_interval timed out: {e}") from e

            return results
        return self.thread_pool.submit(_aggregate)

    def _query_single_node(self, node: Node, query: QueryRequest) -> Tuple[Node, Dict]:
        cluster_name = node.cluster_name
        nodes = self.clusters.get(cluster_name,[])
        leader = self.router.leader_registry.get_leader(cluster_name)
        
        non_leaders = [n for n in nodes if n != leader]
        leader_node =[n for n in nodes if n == leader]
        preferred_nodes = non_leaders + leader_node
        
        last_error = None
        for n in preferred_nodes:
            try:
                all_records =[]
                current_min_id = query.min_id
                current_min_ts = query.min_ts
                
                sock = None
                while True:
                    page_query = QueryRequest(
                        min_id=current_min_id, min_ts=current_min_ts,
                        max_id=query.max_id, max_ts=query.max_ts
                    )
                    
                    req_id = random.randint(1, 1000000)
                    start_key = struct.pack("<QQ", page_query.min_id, page_query.min_ts)
                    end_key = struct.pack("<QQ", page_query.max_id, page_query.max_ts)
                    req_data = struct.pack(QUERY_REQ_FMT, req_id, start_key, end_key)

                    try:
                        if sock is None:
                            sock = self._get_socket(n, for_query=True)

                        sock.sendall(req_data)

                        # Read response header
                        header_data = self._recv_exact(sock, QUERY_RESP_HEADER_SIZE, timeout=self.config.read_timeout)
                        if not header_data:
                            raise ConnectionError(f"Node {n} closed connection")

                        unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                        total_size, resp_req_id, limit_reached, min_key_raw, max_key_raw, records_bytes, records_count = unpacked
                        
                        min_id, min_ts = struct.unpack("<QQ", min_key_raw)
                        max_id, max_ts = struct.unpack("<QQ", max_key_raw)
                        
                        body_size = total_size - QUERY_RESP_HEADER_SIZE

                        # Read body
                        body_data = b""
                        if body_size > 0:
                            body_data = self._recv_exact(sock, body_size, timeout=self.config.read_timeout)
                            
                    except Exception as e:
                        if sock:
                            sock.close()
                            sock = None
                        raise e  # re-raise to failover to next node

                    response = self._parse_response(resp_req_id, limit_reached, 
                                                    min_id, min_ts, max_id, max_ts,
                                                    records_bytes, records_count, body_data)
                    
                    all_records.extend(response["records"])
                    
                    # Check if we have more records to fetch
                    if not response["limit_reached"]:
                        aggregated = {
                            "req_id": resp_req_id, "limit_reached": False,
                            "min_id": query.min_id, "min_ts": query.min_ts,
                            "max_id": max_id if all_records else query.max_id,
                            "max_ts": max_ts if all_records else query.max_ts,
                            "records_bytes": records_bytes,
                            "records_count": len(all_records),
                            "records": all_records
                        }
                        self._return_socket(n, sock, for_query=True)
                        return (n, aggregated)
                    
                    # Pagination checks
                    if max_id >= query.max_id and max_ts >= query.max_ts:
                        aggregated = {
                            "req_id": resp_req_id, "limit_reached": False,
                            "min_id": query.min_id, "min_ts": query.min_ts,
                            "max_id": query.max_id, "max_ts": query.max_ts,
                            "records_bytes": records_bytes,
                            "records_count": len(all_records),
                            "records": all_records
                        }
                        self._return_socket(n, sock, for_query=True)
                        return (n, aggregated)
                    
                    # Next page: start from the next key
                    current_min_id = max_id + 1
                    current_min_ts = 0
                    
            except Exception as e:
                last_error = e
        
        raise RuntimeError(f"Query to cluster {cluster_name} failed after trying {len(preferred_nodes)} nodes. Last error: {last_error}")

    def _parse_response(self, req_id, limit_reached, min_id, min_ts, max_id, max_ts, 
                         records_bytes, records_count, body_data):
        result = {
            "req_id": req_id,
            "limit_reached": bool(limit_reached),
            "min_id": min_id,
            "min_ts": min_ts,
            "max_id": max_id,
            "max_ts": max_ts,
            "records_bytes": records_bytes,
            "records_count": records_count,
            "records":[]
        }
        
        if body_data and records_count > 0:
            offset = 0
            for _ in range(records_count):
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

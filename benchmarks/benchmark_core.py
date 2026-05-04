import socket
import struct
import threading
import time
import json
import argparse
import random
import sys
import hashlib
import select
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Protocol
from dataclasses import dataclass

class ZipfGenerator:
    def __init__(self, n: int, alpha: float = 1.0):
        self.n = n
        size = min(n, 100000)
        self.population = list(range(1, size + 1))
        self.weights = [1.0 / (i ** alpha) for i in range(1, size + 1)]
        
    def next(self) -> int:
        return random.choices(self.population, weights=self.weights, k=1)[0]

# ==============================================================================
# PROTOCOL CONSTANTS
# ==============================================================================
LSMT_TYPE_INT = 1
PIPELINE_DEPTH = 4096 

INSERT_CMD_FORMAT_PREFIX = "<QQ" # Key (u64), Timestamp (u64)
# Query Protocol
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response Header: [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_ID (8)] [MIN_TS (8)] [MAX_ID (8)] [MAX_TS (8)] [RECORDS_BYTES (8)] [RECORDS_COUNT (4)]
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q Q I"
QUERY_RESP_HEADER_SIZE = 61

# ==============================================================================
# TELEMETRY MODULE (StatsTracker)
# ==============================================================================
class StatsTracker:
    """Thread-safe telemetry module for tracking benchmark metrics."""
    def __init__(self):
        self._lock = threading.Lock()
        self.write_ops = 0
        self.write_batches = 0
        self.read_ops = 0
        self.read_bytes = 0
        self.read_errors = 0
        self.write_errors = 0
        self.start_time = time.time()
        self.query_latencies = []
        self.latency_lock = threading.Lock()

    def record_query_latency(self, latency_sec: float):
        with self.latency_lock:
            self.query_latencies.append(latency_sec)

    def get_and_reset_latency(self) -> float:
        with self.latency_lock:
            if not self.query_latencies:
                return 0.0
            avg = sum(self.query_latencies) / len(self.query_latencies)
            self.query_latencies = []
            return avg

    def record_write(self, count: int):
        with self._lock:
            self.write_ops += count
            self.write_batches += 1

    def record_write_error(self):
        with self._lock:
            self.write_errors += 1

    def record_read(self, bytes_len: int):
        with self._lock:
            self.read_ops += 1
            self.read_bytes += bytes_len

    def record_read_error(self):
        with self._lock:
            self.read_errors += 1

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "write_ops": self.write_ops,
                "read_ops": self.read_ops,
                "read_bytes": self.read_bytes,
                "write_errors": self.write_errors,
                "read_errors": self.read_errors,
                "elapsed": time.time() - self.start_time
            }

# ==============================================================================
# LOGIC MODULE (Router)
# ==============================================================================
@dataclass(frozen=True)
class Node:
    cluster_name: str
    id: int
    host: str
    port: int

class LeaderRegistry:
    """Thread-safe registry to track the current leader of the cluster."""
    def __init__(self):
        self._leaders: Dict[str, Node] = {}
        self._lock = threading.Lock()

    def set_leader(self, node: Node):
        with self._lock:
            self._leaders[node.cluster_name] = node

    def get_leader(self, cluster_name: str) -> Optional[Node]:
        with self._lock:
            return self._leaders.get(cluster_name)

    def clear_leader(self, node: Node):
        with self._lock:
            if self._leaders.get(node.cluster_name) == node:
                self._leaders[node.cluster_name] = None

class RoutingStrategy(ABC):
    """Abstract base class for routing strategies."""
    @abstractmethod
    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        pass

class HashRoutingStrategy(RoutingStrategy):
    """Default strategy: Hashing (id + timestamp) to determine target node."""
    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        val = f"{key_id}-{timestamp}".encode()
        h = int(hashlib.md5(val).hexdigest(), 16)
        return nodes[h % len(nodes)]

class RoundRobinRoutingStrategy(RoutingStrategy):
    """Alternative strategy: Round Robin distribution."""
    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        with self._lock:
            node = nodes[self._counter % len(nodes)]
            self._counter += 1
            return node

class LeaderRoutingStrategy(RoutingStrategy):
    """Strategy that always returns a random node."""
    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        return random.choice(nodes)

class Router:
    """Orchestrator for routing logic."""
    def __init__(self, clusters: Dict[str, List[Node]], strategy: RoutingStrategy):
        self.clusters = clusters
        self.cluster_names = sorted(list(clusters.keys()))
        self.strategy = strategy

    def get_cluster_for_key(self, key_id: int, timestamp: int) -> str:
        val = struct.pack("<QQ", key_id, timestamp)
        h = int(hashlib.md5(val).hexdigest(), 16)
        return self.cluster_names[h % len(self.cluster_names)]

    def route(self, key_id: int, timestamp: int) -> Node:
        cluster_name = self.get_cluster_for_key(key_id, timestamp)
        nodes = self.clusters[cluster_name]
        return self.strategy.get_node(key_id, timestamp, nodes)

# ==============================================================================
# TRANSPORT MODULE (NetworkClient)
# ==============================================================================
class NetworkClientInterface(ABC):
    @abstractmethod
    def connect(self, host: str, port: int):
        pass

    @abstractmethod
    def send(self, data: bytes):
        pass

    @abstractmethod
    def recv_exact(self, n: int) -> bytes:
        pass

    @abstractmethod
    def close(self):
        pass

class TCPClient(NetworkClientInterface):
    """Robust TCP client for socket lifecycle management."""
    def __init__(self, timeout: Optional[float] = 5.0):
        self.sock: Optional[socket.socket] = None
        self.timeout = timeout
        self.host: Optional[str] = None
        self.port: Optional[int] = None

    def connect(self, host: str, port: int):
        self.close()
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.timeout:
            self.sock.settimeout(self.timeout)
        self.sock.connect((host, port))

    def send(self, data: bytes):
        if not self.sock:
            raise ConnectionError("Not connected")
        self.sock.sendall(data)

    def recv_exact(self, n: int) -> bytes:
        if not self.sock:
            raise ConnectionError("Not connected")
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket closed by peer")
            data.extend(chunk)
        return bytes(data)

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

# ==============================================================================
# WORKERS
# ==============================================================================
class WriteWorker(threading.Thread):
    def __init__(self, requests: List[Tuple[bytes, Dict]], router: Router, stats: StatsTracker, stop_event: threading.Event, history: List[Dict], history_lock: threading.Lock, leader_registry: LeaderRegistry):
        super().__init__()
        self.requests = requests
        self.router = router
        self.stats = stats
        self.stop_event = stop_event
        self.history = history
        self.history_lock = history_lock
        self.leader_registry = leader_registry
        self.clients: Dict[Node, TCPClient] = {}

    def get_client(self, node: Node) -> TCPClient:
        if node not in self.clients:
            client = TCPClient()
            client.connect(node.host, node.port)
            self.clients[node] = client
        return self.clients[node]

    def run(self):
        node_batches: Dict[Node, List[Tuple[bytes, Dict]]] = {}
        for pkt, meta in self.requests:
            if self.stop_event.is_set():
                break
            node = self.router.route(meta['id'], meta['ts'])
            if node not in node_batches:
                node_batches[node] = []
            node_batches[node].append((pkt, meta))
            if len(node_batches[node]) >= PIPELINE_DEPTH:
                self.flush_batch(node, node_batches[node])
                node_batches[node] = []
        for node, batch in node_batches.items():
            if batch:
                self.flush_batch(node, batch)
        for client in self.clients.values():
            client.close()

    def flush_batch(self, preferred_node: Node, batch: List[Tuple[bytes, Dict]]):
        """
        Sends a batch of requests. It prioritizes the known leader from the registry.
        If it fails, it falls back to other nodes to discover the new leader.
        """
        cluster_name = preferred_node.cluster_name
        # 1. Try known leader first
        known_leader = self.leader_registry.get_leader(cluster_name)
        
        # 2. Build trial list: [KnownLeader, PreferredNode, ...Others]
        nodes_to_try = []
        if known_leader:
            nodes_to_try.append(known_leader)
        if preferred_node not in nodes_to_try:
            nodes_to_try.append(preferred_node)
        
        for n in self.router.clusters[cluster_name]:
            if n not in nodes_to_try:
                nodes_to_try.append(n)
        
        node_idx = 0
        while not self.stop_event.is_set():
            current_node = nodes_to_try[node_idx]
            try:
                client = self.get_client(current_node)
                payload = b''.join(p for p, m in batch)
                client.send(payload)
                # Wait for ACKs (1 byte per request)
                _ = client.recv_exact(len(batch))
                
                # Success! This is definitely the leader
                self.leader_registry.set_leader(current_node)
                
                self.stats.record_write(len(batch))
                with self.history_lock:
                    self.history.extend(m for p, m in batch)
                return
            except Exception:
                self.stats.record_write_error()
                # Clear leader if it was this node
                self.leader_registry.clear_leader(current_node)
                
                if current_node in self.clients:
                    self.clients[current_node].close()
                    del self.clients[current_node]
                
                # Fallback to next node
                node_idx = (node_idx + 1) % len(nodes_to_try)
                time.sleep(0.1)

class QueryWorker(threading.Thread):
    def __init__(self, router: Router, stats: StatsTracker, stop_event: threading.Event, leader_registry: LeaderRegistry, max_key: int):
        super().__init__()
        self.router = router
        self.stats = stats
        self.stop_event = stop_event
        self.leader_registry = leader_registry
        self.max_key = max_key
        self.clients: Dict[str, TCPClient] = {}
        self.pending_requests = {}
        self.pending_lock = threading.Lock()
        self.semaphore = threading.Semaphore(50)
        self.active = False

    def connect_all(self):
        for cluster_name, nodes in self.router.clusters.items():
            if cluster_name not in self.clients:
                self.clients[cluster_name] = TCPClient()
            
            client = self.clients[cluster_name]
            if client.sock is None:
                leader = self.leader_registry.get_leader(cluster_name)
                # Filter out the leader for queries (followers only)
                available_nodes = [n for n in nodes if n != leader]
                if not available_nodes:
                    available_nodes = nodes # Fallback if only one node exists
                
                node = random.choice(available_nodes)
                try:
                    client.connect(node.host, node.port + 4000)
                except:
                    pass

        self.active = all(c.sock is not None for c in self.clients.values())
        if not self.active:
            time.sleep(0.5)

    def sender_loop(self):
        while self.active and not self.stop_event.is_set():
            try:
                if not self.semaphore.acquire(timeout=0.1):
                    continue
                    
                start_id = random.randint(1, self.max_key)
                end_id = start_id + random.randint(10, 5000)
                req_id = random.randint(1, 1000000)
                req_data = struct.pack(QUERY_REQ_FMT, req_id, start_id, 0, end_id, 2**64 - 1)
                
                with self.pending_lock:
                    self.pending_requests[req_id] = {'start': time.time(), 'count': 0}
                
                for client in self.clients.values():
                    client.send(req_data)
            except Exception:
                self.active = False
                break

    def receiver_loop(self):
        while self.active and not self.stop_event.is_set():
            try:
                sockets = [c.sock for c in self.clients.values() if c.sock]
                if not sockets:
                    time.sleep(0.1)
                    continue
                
                readable, _, _ = select.select(sockets, [], [], 0.5)
                for sock in readable:
                    client = next(c for c in self.clients.values() if c.sock == sock)
                    header_data = client.recv_exact(QUERY_RESP_HEADER_SIZE)
                    if not header_data:
                        raise Exception("Connection closed")
                    unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                    
                    total_size = unpacked[0]
                    req_id = unpacked[1]
                    body_size = total_size - QUERY_RESP_HEADER_SIZE
                    if body_size > 0:
                        _ = client.recv_exact(body_size)
                    
                    self.stats.record_read(total_size)
                    
                    with self.pending_lock:
                        if req_id in self.pending_requests:
                            self.pending_requests[req_id]['count'] += 1
                            if self.pending_requests[req_id]['count'] == len(self.clients):
                                latency = time.time() - self.pending_requests[req_id]['start']
                                self.stats.record_query_latency(latency)
                                del self.pending_requests[req_id]
                                self.semaphore.release()
            except Exception:
                self.active = False
                break

    def run(self):
        while not self.stop_event.is_set():
            self.connect_all()
            if not self.active:
                continue
                
            self.pending_requests.clear()
            self.semaphore = threading.Semaphore(50)
            
            t_send = threading.Thread(target=self.sender_loop, daemon=True)
            t_recv = threading.Thread(target=self.receiver_loop, daemon=True)
            t_send.start()
            t_recv.start()
            t_recv.join()
            t_send.join()
            if not self.stop_event.is_set():
                self.stats.record_read_error()
                for client in self.clients.values():
                    client.close()

# ==============================================================================
# UTILS
# ==============================================================================
def create_insert_request(seq_id: int) -> Tuple[bytes, Dict]:
    real_content = seq_id
    inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, 8, real_content)
    key_timestamp = int(time.time() * 1000)
    outer_payload = struct.pack(f"{INSERT_CMD_FORMAT_PREFIX}{len(inner_payload)}s", seq_id, key_timestamp, inner_payload)
    binary_packet = struct.pack("<I", 4 + len(outer_payload)) + outer_payload
    return binary_packet, {'id': seq_id, 'ts': key_timestamp}

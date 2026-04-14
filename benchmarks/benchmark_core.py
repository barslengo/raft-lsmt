import socket
import struct
import threading
import time
import json
import argparse
import random
import sys
import hashlib
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Protocol
from dataclasses import dataclass

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
    id: int
    host: str
    port: int

class LeaderRegistry:
    """Thread-safe registry to track the current leader of the cluster."""
    def __init__(self):
        self._leader: Optional[Node] = None
        self._lock = threading.Lock()

    def set_leader(self, node: Node):
        with self._lock:
            self._leader = node

    def get_leader(self) -> Optional[Node]:
        with self._lock:
            return self._leader

    def clear_leader(self, node: Node):
        with self._lock:
            if self._leader == node:
                self._leader = None

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
    def __init__(self, nodes: List[Node], strategy: RoutingStrategy):
        self.nodes = nodes
        self.strategy = strategy

    def route(self, key_id: int, timestamp: int) -> Node:
        return self.strategy.get_node(key_id, timestamp, self.nodes)

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
        # 1. Try known leader first
        known_leader = self.leader_registry.get_leader()
        
        # 2. Build trial list: [KnownLeader, PreferredNode, ...Others]
        nodes_to_try = []
        if known_leader:
            nodes_to_try.append(known_leader)
        if preferred_node not in nodes_to_try:
            nodes_to_try.append(preferred_node)
        
        for n in self.router.nodes:
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
    def __init__(self, router: Router, stats: StatsTracker, stop_event: threading.Event, history: List[Dict], history_lock: threading.Lock, leader_registry: LeaderRegistry):
        super().__init__()
        self.router = router
        self.stats = stats
        self.stop_event = stop_event
        self.history = history
        self.history_lock = history_lock
        self.leader_registry = leader_registry
        self.client = TCPClient()
        self.active = False

    def connect_any(self):
        while not self.stop_event.is_set():
            leader = self.leader_registry.get_leader()
            # Filter out the leader for queries (followers only)
            available_nodes = [n for n in self.router.nodes if n != leader]
            if not available_nodes:
                available_nodes = self.router.nodes # Fallback if only one node exists
            
            node = random.choice(available_nodes)
            try:
                self.client.connect(node.host, node.port + 4000)
                self.active = True
                return
            except:
                time.sleep(0.5)

    def get_valid_query_range(self):
        try:
            with self.history_lock:
                hist_len = len(self.history)
                if hist_len < 10:
                    return None
                idx = random.randint(0, hist_len - 1)
                start_id = self.history[idx]['id']
            range_len = random.randint(10, 5000)
            return start_id, start_id + range_len
        except:
            return None

    def sender_loop(self):
        while self.active and not self.stop_event.is_set():
            try:
                rng = self.get_valid_query_range()
                if not rng:
                    time.sleep(0.1)
                    continue
                start_id, end_id = rng
                req_id = random.randint(1, 1000000)
                req_data = struct.pack(QUERY_REQ_FMT, req_id, start_id, 0, end_id, 2**64 - 1)
                self.client.send(req_data)
            except Exception:
                self.active = False
                break

    def receiver_loop(self):
        while self.active and not self.stop_event.is_set():
            try:
                header_data = self.client.recv_exact(QUERY_RESP_HEADER_SIZE)
                unpacked = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)
                # Indices: 0:total_size, 1:req_id, 2:limit, 3:min_id, 4:min_ts, 5:max_id, 6:max_ts, 7:records_bytes, 8:records_count
                total_size = unpacked[0]
                body_size = total_size - QUERY_RESP_HEADER_SIZE
                if body_size > 0:
                    _ = self.client.recv_exact(body_size)
                self.stats.record_read(total_size)
            except Exception:
                self.active = False
                break

    def run(self):
        while not self.stop_event.is_set():
            self.connect_any()
            t_send = threading.Thread(target=self.sender_loop, daemon=True)
            t_recv = threading.Thread(target=self.receiver_loop, daemon=True)
            t_send.start()
            t_recv.start()
            t_recv.join()
            t_send.join()
            if not self.stop_event.is_set():
                self.stats.record_read_error()

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

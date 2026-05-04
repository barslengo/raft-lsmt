"""
Middleware module for Raft-LSMT database clusters.

This module provides a Python middleware that:
- Loads cluster configuration from JSON files
- Implements hash-based routing for write requests
- Forwards query requests to followers across all clusters
- Buffers requests and sends them in batches
- Exposes SendWriteRequest(), SendQueryRequest(), and FlushRequests() methods
"""

import socket
import struct
import threading
import random
import hashlib
import json
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field


# ==============================================================================
# Protocol Constants (matching server.c)
# ==============================================================================

# Write request format
INSERT_CMD_FORMAT_PREFIX = "<QQ"  # Key (u64), Timestamp (u64)
LSMT_TYPE_INT = 1

# Query request format
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Query response header
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q Q I"
QUERY_RESP_HEADER_SIZE = 61

# Batch configuration
DEFAULT_BATCH_SIZE = 4096


# ==============================================================================
# Data Structures
# ==============================================================================

@dataclass(frozen=True)
class Node:
    """Represents a single database node."""
    cluster_name: str
    id: int
    host: str
    port: int  # Base port for writes (queries use port + 4000)


@dataclass
class Cluster:
    """Represents a Raft cluster with nodes."""
    name: str
    nodes: List[Node]
    leader: Optional[Node] = None


@dataclass
class WriteRequest:
    """Represents a buffered write request."""
    key_id: int
    timestamp: int
    payload: bytes


@dataclass
class QueryRequest:
    """Represents a buffered query request."""
    req_id: int
    start_key_id: int
    start_key_ts: int
    end_key_id: int
    end_key_ts: int


# ==============================================================================
# Middleware Class
# ==============================================================================

class RaftLSMTMiddleware:
    """
    Middleware for interacting with Raft-LSMT clusters.
    
    Features:
    - Hash-based routing for write requests to clusters
    - Query forwarding to followers across all clusters
    - Request batching for improved throughput
    - Thread-safe operations
    
    Configuration file format (JSON):
    {
        "cluster_A": [
            {"id": 1, "host": "127.0.0.1", "port": 7001},
            {"id": 2, "host": "127.0.0.1", "port": 7002}
        ],
        "cluster_B": [
            {"id": 3, "host": "127.0.0.1", "port": 7003},
            {"id": 4, "host": "127.0.0.1", "port": 7004}
        ]
    }
    """
    
    def __init__(self, config_path: str, batch_size: int = DEFAULT_BATCH_SIZE):
        """
        Initialize the middleware.
        
        Args:
            config_path: Path to JSON configuration file
            batch_size: Number of requests to buffer before sending (default: 4096)
        """
        self.batch_size = batch_size
        self.clusters: Dict[str, Cluster] = {}
        self._lock = threading.Lock()
        
        # Buffered requests
        self._write_buffer: List[WriteRequest] = []
        self._query_buffer: List[QueryRequest] = []
        
        # Connection pool
        self._write_sockets: Dict[str, Dict[Node, socket.socket]] = {}  # cluster -> {node -> socket}
        self._query_sockets: Dict[str, Dict[Node, socket.socket]] = {}   # cluster -> {node -> socket}
        
        # Load configuration
        self.load_config(config_path)
        
        # Note: Leader discovery is disabled for now to avoid connection attempts
        # during initialization. Clients should call GetKnownLeader() or manually
        # set leaders on clusters as needed.
        # threading.Thread(target=self._discover_leaders, daemon=True).start()
    
    def load_config(self, config_path: str) -> None:
        """Load cluster configuration from JSON file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        with self._lock:
            self.clusters = {}
            for cluster_name, nodes_data in config_data.items():
                nodes = [
                    Node(
                        cluster_name=cluster_name,
                        id=n['id'],
                        host=n['host'],
                        port=n['port']
                    )
                    for n in nodes_data
                ]
                self.clusters[cluster_name] = Cluster(name=cluster_name, nodes=nodes)
                
                # Initialize connection pools
                self._write_sockets[cluster_name] = {}
                self._query_sockets[cluster_name] = {}
    
    def _get_node_for_key(self, key_id: int, timestamp: int) -> Tuple[str, Node]:
        """
        Determine which cluster and node should handle a write request
        using hash-based routing.
        
        Returns:
            Tuple of (cluster_name, target_node)
        """
        # Hash the key to determine the cluster
        val = struct.pack("<QQ", key_id, timestamp)
        h = int(hashlib.md5(val).hexdigest(), 16)
        
        cluster_names = list(self.clusters.keys())
        cluster_name = cluster_names[h % len(cluster_names)]
        
        # For writes, target the leader if known, otherwise pick a random node
        cluster = self.clusters[cluster_name]
        if cluster.leader:
            return cluster_name, cluster.leader
        else:
            # Fallback: pick a random node (will discover leader on first attempt)
            return cluster_name, random.choice(cluster.nodes)
    
    def _get_follower_for_cluster(self, cluster_name: str) -> Optional[Node]:
        """
        Get a random follower node from a cluster for query requests.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            A follower node, or None if no followers available
        """
        cluster = self.clusters.get(cluster_name)
        if not cluster:
            return None
        
        # If we know the leader, pick a non-leader node
        if cluster.leader:
            followers = [n for n in cluster.nodes if n != cluster.leader]
            if followers:
                return random.choice(followers)
        
        # Otherwise, just pick a random node
        if cluster.nodes:
            return random.choice(cluster.nodes)
        
        return None
    
    def _get_socket(self, node: Node, is_query: bool = False) -> Optional[socket.socket]:
        """
        Get or create a socket connection to a node.
        
        Args:
            node: Target node
            is_query: True for query port (port + 4000), False for write port
            
        Returns:
            Socket connection or None if connection failed
        """
        cluster_name = node.cluster_name
        port = node.port + (4000 if is_query else 0)
        
        socket_pool = self._query_sockets if is_query else self._write_sockets
        
        if cluster_name not in socket_pool:
            socket_pool[cluster_name] = {}
        
        # Check if we already have a connection
        if node in socket_pool[cluster_name]:
            sock = socket_pool[cluster_name][node]
            try:
                # Test if socket is still alive
                sock.settimeout(0.1)
                # Send a zero-byte packet to check if connection is alive
                # Actually, just try to get peer address
                sock.getpeername()
                return sock
            except:
                # Connection is dead, remove it
                del socket_pool[cluster_name][node]
        
        # Create new connection
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((node.host, port))
            sock.settimeout(None)  # No timeout for subsequent operations
            socket_pool[cluster_name][node] = sock
            return sock
        except Exception:
            return None
    
    def _send_write_batch(self, cluster_name: str, node: Node, requests: List[WriteRequest]) -> bool:
        """
        Send a batch of write requests to a specific node.
        
        Args:
            cluster_name: Name of the cluster
            node: Target node
            requests: List of WriteRequest objects to send
            
        Returns:
            True if successful, False otherwise
        """
        sock = self._get_socket(node, is_query=False)
        if not sock:
            return False
        
        try:
            # Concatenate all payloads
            payload = b''.join(req.payload for req in requests)
            sock.settimeout(5.0)  # Timeout for network operations
            sock.sendall(payload)
            
            # Wait for ACKs (1 byte per request)
            ack_data = sock.recv(len(requests))
            if len(ack_data) != len(requests):
                return False
            
            # Verify ACKs are valid (should be 0x01 bytes)
            # In server.c, ACK_BUFFER is filled with 1s
            if ack_data != b'\x01' * len(requests):
                return False
            
            return True
            
            return True
        except Exception:
            return False
    
    def _send_query_request(self, node: Node, query: QueryRequest) -> Optional[bytes]:
        """
        Send a single query request to a node.
        
        Args:
            node: Target node
            query: QueryRequest to send
            
        Returns:
            Response bytes or None if failed
        """
        sock = self._get_socket(node, is_query=True)
        if not sock:
            return None
        
        try:
            # Pack the query request
            req_data = struct.pack(
                QUERY_REQ_FMT,
                query.req_id,
                query.start_key_id,
                query.start_key_ts,
                query.end_key_id,
                query.end_key_ts
            )
            sock.sendall(req_data)
            
            # Read response header
            header_data = sock.recv(QUERY_RESP_HEADER_SIZE)
            if not header_data or len(header_data) != QUERY_RESP_HEADER_SIZE:
                return None
            
            # Parse header to get total size
            total_size = struct.unpack(QUERY_RESP_HEADER_FMT, header_data)[0]
            body_size = total_size - QUERY_RESP_HEADER_SIZE
            
            # Read body if present
            if body_size > 0:
                body_data = sock.recv(body_size)
                if not body_data or len(body_data) != body_size:
                    return None
                return header_data + body_data
            
            return header_data
        except Exception:
            return None
    
    def _discover_leaders(self) -> None:
        """
        Attempt to discover the current leader for each cluster.
        This is called during initialization and after connection failures.
        """
        for cluster_name, cluster in self.clusters.items():
            # Try each node to find the leader
            for node in cluster.nodes:
                try:
                    # Use a short timeout for discovery
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.5)  # Short timeout for discovery
                    sock.connect((node.host, node.port))
                    sock.settimeout(None)
                    
                    # Send a small test request
                    test_key = random.randint(1, 1000000)
                    test_ts = int(time.time() * 1000)
                    test_payload = self._create_write_payload(test_key, test_ts)
                    sock.sendall(test_payload)
                    
                    # Try to receive ACK
                    sock.settimeout(0.5)
                    ack = sock.recv(1)
                    if ack == b'\x01':
                        cluster.leader = node
                        sock.close()
                        break
                    sock.close()
                except:
                    pass
    
    def _create_write_payload(self, key_id: int, timestamp: int, value: int = 0) -> bytes:
        """
        Create a binary payload for a write request.
        
        Args:
            key_id: The key ID
            timestamp: The timestamp
            value: The value to store (default: 0)
            
        Returns:
            Binary payload ready to send
        """
        inner_payload = struct.pack("<BIQ", LSMT_TYPE_INT, 8, value)
        outer_payload = struct.pack(f"{INSERT_CMD_FORMAT_PREFIX}{len(inner_payload)}s", 
                                     key_id, timestamp, inner_payload)
        binary_packet = struct.pack("<I", 4 + len(outer_payload)) + outer_payload
        return binary_packet
    
    # =========================================================================
    # Public API Methods
    # =========================================================================
    
    def SendWriteRequest(self, key_id: int, value: int = 0, timestamp: Optional[int] = None) -> None:
        """
        Buffer a write request for later transmission.
        
        The request is not sent immediately but buffered until the batch is full
        or FlushRequests() is called.
        
        Args:
            key_id: The key to write
            value: The value to store (default: 0)
            timestamp: Optional timestamp (default: current time in ms)
        """
        if timestamp is None:
            import time
            timestamp = int(time.time() * 1000)
        
        payload = self._create_write_payload(key_id, timestamp, value)
        
        need_flush = False
        with self._lock:
            self._write_buffer.append(WriteRequest(
                key_id=key_id,
                timestamp=timestamp,
                payload=payload
            ))
            
            # Check if batch is full
            if len(self._write_buffer) >= self.batch_size:
                need_flush = True
        
        # Auto-flush if batch is full (outside the lock to avoid deadlock)
        if need_flush:
            self.FlushRequests(write_only=True)
    
    def SendQueryRequest(self, req_id: int, start_key_id: int, start_key_ts: int, 
                         end_key_id: int, end_key_ts: int) -> None:
        """
        Buffer a query request for later transmission.
        
        The request is not sent immediately but buffered until FlushRequests() is called.
        
        For queries, the request will be forwarded to one random follower in each cluster.
        
        Args:
            req_id: Unique request ID
            start_key_id: Start key ID for range query
            start_key_ts: Start key timestamp
            end_key_id: End key ID for range query
            end_key_ts: End key timestamp
        """
        with self._lock:
            self._query_buffer.append(QueryRequest(
                req_id=req_id,
                start_key_id=start_key_id,
                start_key_ts=start_key_ts,
                end_key_id=end_key_id,
                end_key_ts=end_key_ts
            ))
    
    def FlushRequests(self, write_only: bool = False, query_only: bool = False) -> Dict[str, Any]:
        """
        Flush all buffered requests to the appropriate nodes.
        
        Args:
            write_only: If True, only flush write requests
            query_only: If True, only flush query requests
            
        Returns:
            Dictionary with statistics about the flush operation:
            {
                'writes_sent': int,
                'writes_failed': int,
                'queries_sent': int,
                'queries_failed': int
            }
        """
        stats = {
            'writes_sent': 0,
            'writes_failed': 0,
            'queries_sent': 0,
            'queries_failed': 0
        }
        
        with self._lock:
            # Flush write requests
            if not query_only and self._write_buffer:
                # Group requests by target cluster/node
                node_requests: Dict[Node, List[WriteRequest]] = {}
                
                for req in self._write_buffer:
                    cluster_name, node = self._get_node_for_key(req.key_id, req.timestamp)
                    if (cluster_name, node) not in node_requests:
                        node_requests[(cluster_name, node)] = []
                    node_requests[(cluster_name, node)].append(req)
                
                # Send each batch
                for (cluster_name, node), requests in node_requests.items():
                    success = self._send_write_batch(cluster_name, node, requests)
                    if success:
                        stats['writes_sent'] += len(requests)
                        
                        # Update leader discovery
                        cluster = self.clusters[cluster_name]
                        cluster.leader = node
                    else:
                        stats['writes_failed'] += len(requests)
                        
                        # Clear leader if it was this node
                        cluster = self.clusters[cluster_name]
                        if cluster.leader == node:
                            cluster.leader = None
                
                self._write_buffer = []
            
            # Flush query requests
            if not write_only and self._query_buffer:
                # For queries, send to one random follower per cluster
                for req in self._query_buffer:
                    # Send to all clusters
                    for cluster_name in self.clusters:
                        node = self._get_follower_for_cluster(cluster_name)
                        if node:
                            result = self._send_query_request(node, req)
                            if result is not None:
                                stats['queries_sent'] += 1
                            else:
                                stats['queries_failed'] += 1
                
                self._query_buffer = []
        
        return stats
    
    def GetClusterNames(self) -> List[str]:
        """Get list of all cluster names."""
        return list(self.clusters.keys())
    
    def GetClusterNodes(self, cluster_name: str) -> List[Node]:
        """Get all nodes in a specific cluster."""
        cluster = self.clusters.get(cluster_name)
        if cluster:
            return cluster.nodes.copy()
        return []
    
    def GetKnownLeader(self, cluster_name: str) -> Optional[Node]:
        """Get the currently known leader for a cluster."""
        cluster = self.clusters.get(cluster_name)
        if cluster:
            return cluster.leader
        return None
    
    def Close(self) -> None:
        """Close all connections."""
        with self._lock:
            # Close write sockets
            for cluster_name, sockets in self._write_sockets.items():
                for node, sock in sockets.items():
                    try:
                        sock.close()
                    except:
                        pass
                self._write_sockets[cluster_name] = {}
            
            # Close query sockets
            for cluster_name, sockets in self._query_sockets.items():
                for node, sock in sockets.items():
                    try:
                        sock.close()
                    except:
                        pass
                self._query_sockets[cluster_name] = {}
            
            # Clear buffers
            self._write_buffer = []
            self._query_buffer = []


# ==============================================================================
# Convenience function for quick initialization
# ==============================================================================

def create_middleware(config_path: str, batch_size: int = DEFAULT_BATCH_SIZE) -> RaftLSMTMiddleware:
    """
    Create and return a new RaftLSMTMiddleware instance.
    
    Args:
        config_path: Path to JSON configuration file
        batch_size: Batch size for write requests
        
    Returns:
        Initialized RaftLSMTMiddleware instance
    """
    return RaftLSMTMiddleware(config_path, batch_size)

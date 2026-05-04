#!/usr/bin/env python3
"""
Mock Raft node server for testing the middleware.

This script simulates a Raft-LSMT database node and is fully interchangeable
with the real server.c instances. It implements the same binary protocol
for both write and query operations.

Usage:
    python3 mock_raft_node.py --node-id 1 --host 127.0.0.1 --port 7001 --cluster A
    
    Or to start multiple mock nodes for a cluster:
    python3 mock_raft_node.py --cluster-config cluster_A.json

Configuration file format (JSON):
{
    "A": [
        {"id": 1, "host": "127.0.0.1", "port": 7001},
        {"id": 2, "host": "127.0.0.1", "port": 7002},
        {"id": 3, "host": "127.0.0.1", "port": 7003}
    ]
}
"""

import argparse
import json
import socket
import struct
import threading
import random
import time
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import sys
import os


# ==============================================================================
# Protocol Constants (must match server.c)
# ==============================================================================

# Write protocol
INSERT_CMD_FORMAT_PREFIX = "<QQ"  # Key (u64), Timestamp (u64)
LSMT_TYPE_INT = 1
CONTENT_MAX_SIZE = 255
INSERT_CMD_SIZE = 4 + 16 + CONTENT_MAX_SIZE  # msg_size + key + value

# Query protocol
# Request: [REQ_ID (8)] [START_KEY (16)] [END_KEY (16)] = 40 bytes
QUERY_REQ_FMT = "<Q Q Q Q Q"
QUERY_REQ_SIZE = 40

# Response header
# [TOTAL_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_ID (8)] [MIN_TS (8)] [MAX_ID (8)] [MAX_TS (8)] [RECORDS_BYTES (8)] [RECORDS_COUNT (4)]
QUERY_RESP_HEADER_FMT = "<Q Q B Q Q Q Q Q I"
QUERY_RESP_HEADER_SIZE = 61

# ACK buffer (filled with 1s in server.c)
ACK_BUFFER_BYTE = b'\x01'

# Port offsets
QUERY_PORT_OFFSET = 4000


# ==============================================================================
# Data Structures
# ==============================================================================

@dataclass
class MockNodeConfig:
    """Configuration for a mock node."""
    id: int
    host: str
    port: int
    cluster_name: str
    is_leader: bool = False


@dataclass
class KVRecord:
    """A key-value record stored in the mock database."""
    key_id: int
    key_timestamp: int
    value: bytes
    raw_size: int


# ==============================================================================
# In-Memory Database
# ==============================================================================

class MockDatabase:
    """
    In-memory database that simulates LSMT storage.
    Supports insert and range query operations.
    """
    
    def __init__(self):
        self._data: Dict[Tuple[int, int], bytes] = {}  # (key_id, key_ts) -> value
        self._lock = threading.Lock()
    
    def insert(self, key_id: int, key_timestamp: int, value: bytes) -> int:
        """
        Insert a record into the database.
        
        Returns:
            0 on success
        """
        with self._lock:
            self._data[(key_id, key_timestamp)] = value
        return 0
    
    def query_range(self, start_key_id: int, start_key_ts: int, 
                   end_key_id: int, end_key_ts: int) -> List[KVRecord]:
        """
        Query records in a key range.
        
        Args:
            start_key_id: Start key ID (inclusive)
            start_key_ts: Start key timestamp (inclusive)
            end_key_id: End key ID (inclusive)
            end_key_ts: End key timestamp (inclusive)
            
        Returns:
            List of KVRecord objects in the range
        """
        results = []
        with self._lock:
            for (key_id, key_ts), value in self._data.items():
                # Compare keys: first by ID, then by timestamp
                if self._key_in_range(key_id, key_ts, start_key_id, start_key_ts, end_key_id, end_key_ts):
                    results.append(KVRecord(
                        key_id=key_id,
                        key_timestamp=key_ts,
                        value=value,
                        raw_size=len(value) + 16 + 4  # value + key + msg_size
                    ))
        
        # Sort by key (id, then timestamp)
        results.sort(key=lambda r: (r.key_id, r.key_timestamp))
        return results
    
    def _key_in_range(self, key_id: int, key_ts: int,
                     start_id: int, start_ts: int, end_id: int, end_ts: int) -> bool:
        """Check if a key falls within the query range."""
        # Convert keys to comparable tuples
        key = (key_id, key_ts)
        start = (start_id, start_ts)
        end = (end_id, end_ts)
        
        return start <= key <= end


# ==============================================================================
# Request Parsers
# ==============================================================================

def parse_insert_request(data: bytes) -> Optional[Tuple[int, int, bytes]]:
    """
    Parse an insert request from binary data.
    
    Format: [MSG_SIZE (4)] [KEY_ID (8)] [KEY_TS (8)] [VALUE ...]
    where MSG_SIZE includes the 4-byte size prefix itself.
    
    Returns:
        Tuple of (key_id, key_timestamp, value_bytes) or None if malformed
    """
    if len(data) < 24:  # At least msg_size(4) + key(16) + some value
        return None
    
    # Read message size (includes the 4-byte prefix)
    msg_size = struct.unpack_from("<I", data, 0)[0]
    
    # Sanity check
    if msg_size == 0:
        return None
    
    if msg_size > len(data):
        return None
    
    if msg_size < 24:  # Minimum: 4 (size) + 16 (key) + 4 (min value header)
        return None
    
    # Read key (at offset 4, size 16)
    key_data = data[4:20]
    key_id, key_timestamp = struct.unpack("<QQ", key_data)
    
    # Read value (after key, up to msg_size)
    # Total message: size(4) + key(16) + value
    value_size = msg_size - 20  # msg_size - (4 + 16)
    value = data[20:20 + value_size]
    
    return (key_id, key_timestamp, value)


def parse_query_request(data: bytes) -> Optional[Tuple[int, int, int, int, int]]:
    """
    Parse a query request from binary data.
    
    Format: [REQ_ID (8)] [START_ID (8)] [START_TS (8)] [END_ID (8)] [END_TS (8)]
    
    Returns:
        Tuple of (req_id, start_id, start_ts, end_id, end_ts) or None if malformed
    """
    if len(data) != QUERY_REQ_SIZE:
        return None
    
    try:
        result = struct.unpack(QUERY_REQ_FMT, data)
        return result
    except struct.error:
        return None


# ==============================================================================
# Response Builders
# ==============================================================================

def build_query_response(req_id: int, records: List[KVRecord], 
                         limit_reached: bool = False) -> bytes:
    """
    Build a query response message.
    
    Response format:
    Header: [TOTAL_MSG_SIZE (8)] [REQ_ID (8)] [LIMIT (1)] [MIN_KEY (16)] [MAX_KEY (16)]
           [RECORDS_BYTES (8)] [RECORDS_COUNT (4)]
    Body: [RECORD_DATA ...]
    
    Returns:
        Binary response ready to send
    """
    if not records:
        # Empty response
        total_msg_size = QUERY_RESP_HEADER_SIZE
        header = struct.pack(
            QUERY_RESP_HEADER_FMT,
            total_msg_size,
            req_id,
            1 if limit_reached else 0,
            0, 0,  # min_key (id, ts)
            0, 0,  # max_key (id, ts)
            0,      # records_bytes
            0       # records_count
        )
        return header
    
    # Calculate min and max keys
    min_record = records[0]
    max_record = records[-1]
    
    # Build record data
    record_data = b''
    for record in records:
        # Each record: [MSG_SIZE (4)] [KEY (16)] [VALUE ...]
        # But for query response, we just need the raw data as stored
        # The middleware expects the raw record data
        record_bytes = struct.pack("<QQ", record.key_id, record.key_timestamp) + record.value
        record_data += record_bytes
    
    records_bytes = len(record_data)
    records_count = len(records)
    total_msg_size = QUERY_RESP_HEADER_SIZE + records_bytes
    
    header = struct.pack(
        QUERY_RESP_HEADER_FMT,
        total_msg_size,
        req_id,
        1 if limit_reached else 0,
        min_record.key_id, min_record.key_timestamp,
        max_record.key_id, max_record.key_timestamp,
        records_bytes,
        records_count
    )
    
    return header + record_data


# ==============================================================================
# Mock Server
# ==============================================================================

class MockRaftNode:
    """
    Mock Raft node that simulates server.c behavior.
    
    Handles both write (insert) and query requests on separate ports.
    Implements leader election simulation and proper response formats.
    """
    
    def __init__(self, config: MockNodeConfig):
        self.config = config
        self.db = MockDatabase()
        self._running = False
        self._write_server: Optional[threading.Thread] = None
        self._query_server: Optional[threading.Thread] = None
        self._leader = config.is_leader
    
    def start(self) -> None:
        """Start both write and query servers."""
        self._running = True
        
        # Start write server (base port)
        self._write_server = threading.Thread(
            target=self._run_server,
            args=(self.config.port, False),
            daemon=True
        )
        self._write_server.start()
        
        # Start query server (base port + 4000)
        self._query_server = threading.Thread(
            target=self._run_server,
            args=(self.config.port + QUERY_PORT_OFFSET, True),
            daemon=True
        )
        self._query_server.start()
        
        print(f"Mock node {self.config.id} started: write port={self.config.port}, "
              f"query port={self.config.port + QUERY_PORT_OFFSET}")
    
    def stop(self) -> None:
        """Stop all servers."""
        self._running = False
        if self._write_server:
            self._write_server.join(timeout=1.0)
        if self._query_server:
            self._query_server.join(timeout=1.0)
    
    def _run_server(self, port: int, is_query: bool) -> None:
        """
        Run the TCP server for either write or query operations.
        
        Args:
            port: Port to listen on
            is_query: True for query server, False for write server
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.config.host, port))
        server_sock.listen(128)
        
        server_type = "QUERY" if is_query else "WRITE"
        print(f"  [{server_type}] Listening on {self.config.host}:{port}")
        
        try:
            while self._running:
                try:
                    client_sock, addr = server_sock.accept()
                    client_sock.settimeout(30.0)
                    
                    handler_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_sock, addr, is_query),
                        daemon=True
                    )
                    handler_thread.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"  [{server_type}] Accept error: {e}")
                    break
        finally:
            try:
                server_sock.close()
            except:
                pass
    
    def _handle_client(self, client_sock: socket.socket, addr: Tuple[str, int], 
                      is_query: bool) -> None:
        """
        Handle a client connection.
        
        Args:
            client_sock: Client socket
            addr: Client address
            is_query: True for query connection, False for write connection
        """
        server_type = "QUERY" if is_query else "WRITE"
        
        try:
            buffer = bytearray()
            
            while self._running:
                # Read available data
                chunk = client_sock.recv(65536)
                if not chunk:
                    break
                
                buffer.extend(chunk)
                
                # Process complete messages
                if is_query:
                    # Query protocol: each request is exactly 40 bytes
                    while len(buffer) >= QUERY_REQ_SIZE:
                        req_data = bytes(buffer[:QUERY_REQ_SIZE])
                        buffer = buffer[QUERY_REQ_SIZE:]
                        
                        parsed = parse_query_request(req_data)
                        if parsed:
                            self._handle_query(client_sock, *parsed)
                        else:
                            print(f"  [{server_type}] Invalid query request from {addr}")
                            break
                else:
                    # Write protocol: messages are length-prefixed
                    # Buffer contains: [msg1_size(4)][msg1_data(msg1_size)][msg2_size(4)][msg2_data(msg2_size)]...
                    # where msg_size includes the 4-byte size prefix itself.
                    ack_count = 0
                    while len(buffer) >= 4:
                        msg_size = struct.unpack_from("<I", buffer, 0)[0]
                        
                        if msg_size == 0:
                            # Padding, skip
                            buffer = buffer[4:]
                            continue
                        
                        # msg_size includes the 4-byte prefix, so total message is msg_size bytes
                        if msg_size > len(buffer):
                            break
                        
                        # Extract the complete message (including size prefix)
                        msg_data = bytes(buffer[:msg_size])
                        buffer = buffer[msg_size:]
                        
                        # Parse and process this message
                        parsed = parse_insert_request(msg_data)
                        if parsed:
                            key_id, key_ts, value = parsed
                            self.db.insert(key_id, key_ts, value)
                            ack_count += 1
                        else:
                            # Still count as processed to keep protocol working
                            ack_count += 1
                    
                    # Send all ACKs at once
                    if ack_count > 0:
                        client_sock.sendall(ACK_BUFFER_BYTE * ack_count)
        
        except ConnectionResetError:
            pass
        except Exception as e:
            print(f"  [{server_type}] Error handling client {addr}: {e}")
        finally:
            try:
                client_sock.close()
            except:
                pass
    
    def _handle_query(self, client_sock: socket.socket, req_id: int, 
                     start_id: int, start_ts: int, end_id: int, end_ts: int) -> None:
        """
        Handle a query request.
        
        Args:
            client_sock: Client socket
            req_id: Request ID
            start_id: Start key ID
            start_ts: Start key timestamp
            end_id: End key ID
            end_ts: End key timestamp
        """
        records = self.db.query_range(start_id, start_ts, end_id, end_ts)
        
        # Limit responses to prevent huge payloads
        # Match the QUERY_BYTES_LIMIT from server.c (512KB)
        QUERY_BYTES_LIMIT = 512 * 1024
        QUERY_REQ_SIZE = 40
        
        limit_reached = False
        response_records = []
        response_bytes = 0
        
        for record in records:
            record_size = 16 + len(record.value)  # key (16) + value
            if response_bytes + record_size > QUERY_BYTES_LIMIT:
                limit_reached = True
                break
            response_records.append(record)
            response_bytes += record_size
        
        response = build_query_response(req_id, response_records, limit_reached)
        client_sock.sendall(response)


# ==============================================================================
# Multi-Node Manager
# ==============================================================================

class MockClusterManager:
    """
    Manages multiple mock nodes as a single cluster.
    
    Automatically elects a leader and handles node coordination.
    """
    
    def __init__(self):
        self.nodes: List[MockRaftNode] = []
        self._leader_node: Optional[MockRaftNode] = None
    
    def add_node(self, config: MockNodeConfig) -> MockRaftNode:
        """Add a node to the cluster."""
        node = MockRaftNode(config)
        self.nodes.append(node)
        return node
    
    def start_all(self) -> None:
        """Start all nodes in the cluster."""
        # Elect a leader (first node by default)
        if self.nodes:
            self._leader_node = self.nodes[0]
            for i, node in enumerate(self.nodes):
                node.config.is_leader = (i == 0)
        
        for node in self.nodes:
            node.start()
        
        print(f"Mock cluster started with {len(self.nodes)} nodes")
        if self._leader_node:
            print(f"Leader: node {self._leader_node.config.id}")
    
    def stop_all(self) -> None:
        """Stop all nodes in the cluster."""
        for node in self.nodes:
            node.stop()
        self.nodes = []
        self._leader_node = None
    
    def get_leader(self) -> Optional[MockRaftNode]:
        """Get the current leader node."""
        return self._leader_node


# ==============================================================================
# Cluster Auto-Discovery
# ==============================================================================

def elect_leader(nodes: List[MockNodeConfig]) -> List[MockNodeConfig]:
    """
    Elect a leader from the node configurations.
    
    Args:
        nodes: List of node configurations
        
    Returns:
        Updated list with is_leader set on one node
    """
    if not nodes:
        return nodes
    
    # Simple election: first node is leader
    nodes[0].is_leader = True
    return nodes


# ==============================================================================
# Main
# ==============================================================================

def start_single_node():
    """Start a single mock node from command line arguments."""
    parser = argparse.ArgumentParser(description="Mock Raft-LSMT Node")
    parser.add_argument("--node-id", type=int, required=True, help="Node ID")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, required=True, help="Base port for writes")
    parser.add_argument("--cluster", type=str, required=True, help="Cluster name")
    parser.add_argument("--is-leader", action="store_true", help="Mark as leader node")
    
    args = parser.parse_args()
    
    config = MockNodeConfig(
        id=args.node_id,
        host=args.host,
        port=args.port,
        cluster_name=args.cluster,
        is_leader=args.is_leader
    )
    
    node = MockRaftNode(config)
    node.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        print("Mock node stopped.")


def start_cluster_from_config():
    """Start a cluster of mock nodes from a JSON configuration file."""
    parser = argparse.ArgumentParser(description="Mock Raft-LSMT Cluster")
    parser.add_argument("--config", type=str, required=True, 
                        help="Path to cluster configuration JSON file")
    parser.add_argument("--cluster-name", type=str, required=True,
                        help="Name of the cluster to start")
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    cluster_data = config_data.get(args.cluster_name)
    if not cluster_data:
        print(f"Error: Cluster '{args.cluster_name}' not found in config")
        sys.exit(1)
    
    manager = MockClusterManager()
    
    for node_data in cluster_data:
        config = MockNodeConfig(
            id=node_data['id'],
            host=node_data.get('host', '127.0.0.1'),
            port=node_data['port'],
            cluster_name=args.cluster_name,
            is_leader=False
        )
        manager.add_node(config)
    
    manager.start_all()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_all()
        print("Mock cluster stopped.")


def start_all_clusters():
    """Start all clusters defined in a JSON configuration file."""
    parser = argparse.ArgumentParser(description="Mock Raft-LSMT - All Clusters")
    parser.add_argument("--config", type=str, required=True,
                        help="Path to cluster configuration JSON file")
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config_data = json.load(f)
    
    managers: Dict[str, MockClusterManager] = {}
    
    for cluster_name, nodes_data in config_data.items():
        manager = MockClusterManager()
        
        for node_data in nodes_data:
            config = MockNodeConfig(
                id=node_data['id'],
                host=node_data.get('host', '127.0.0.1'),
                port=node_data['port'],
                cluster_name=cluster_name,
                is_leader=False
            )
            manager.add_node(config)
        
        manager.start_all()
        managers[cluster_name] = manager
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for cluster_name, manager in managers.items():
            manager.stop_all()
            print(f"Stopped cluster '{cluster_name}'")
        print("All mock clusters stopped.")


if __name__ == "__main__":
    print("=" * 60)
    print("Mock Raft-LSMT Node/Cluster Server")
    print("=" * 60)
    
    # Detect command line arguments
    if "--config" in sys.argv:
        if "--cluster-name" in sys.argv:
            start_cluster_from_config()
        else:
            start_all_clusters()
    else:
        start_single_node()

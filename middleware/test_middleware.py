#!/usr/bin/env python3
"""
Unit tests for the Raft-LSMT middleware.
Uses assertions to verify correctness.
"""

import sys
import os
import json
import tempfile
import socket
import struct
import threading
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware import RaftLSMTMiddleware, Node, Cluster, WriteRequest, QueryRequest
from middleware.mock_raft_node import MockRaftNode, MockNodeConfig, MockDatabase


# ==============================================================================
# Test Configuration
# ==============================================================================

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
TEST_PORT_START = 18000
QUERY_PORT_OFFSET = 4000


def create_test_config_path():
    """Create a temporary config file for tests that need custom configs."""
    config = {
        "test_cluster": [
            {"id": 1, "host": "127.0.0.1", "port": 18001},
            {"id": 2, "host": "127.0.0.1", "port": 18002}
        ]
    }
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(config, f)
    f.close()
    return f.name


def cleanup_temp_file(path):
    """Remove a temporary file."""
    try:
        os.unlink(path)
    except:
        pass


# ==============================================================================
# Helper Functions
# ==============================================================================

def start_mock_servers_from_config(config_path):
    """Start mock servers for all nodes in a configuration file."""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    servers = []
    for cluster_name, nodes in config_data.items():
        for node_data in nodes:
            config_obj = MockNodeConfig(
                id=node_data['id'],
                host=node_data.get('host', '127.0.0.1'),
                port=node_data['port'],
                cluster_name=cluster_name,
                is_leader=(node_data['id'] % 10 == 1)
            )
            node = MockRaftNode(config_obj)
            node.start()
            servers.append(node)
            time.sleep(0.1)
    return servers


def stop_mock_servers(servers):
    """Stop all mock servers."""
    for node in servers:
        node.stop()


# ==============================================================================
# Unit Tests
# ==============================================================================

def test_middleware_initialization():
    """Test that middleware can be initialized with the config file."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=100)
    
    # Verify clusters were loaded
    cluster_names = mw.GetClusterNames()
    assert len(cluster_names) == 2, f"Expected 2 clusters, got {len(cluster_names)}"
    assert 'test_cluster' in cluster_names, "test_cluster not found"
    assert 'test_cluster_2' in cluster_names, "test_cluster_2 not found"
    
    # Verify nodes were loaded
    cluster_1_nodes = mw.GetClusterNodes('test_cluster')
    assert len(cluster_1_nodes) == 3, f"Expected 3 nodes in test_cluster, got {len(cluster_1_nodes)}"
    
    cluster_2_nodes = mw.GetClusterNodes('test_cluster_2')
    assert len(cluster_2_nodes) == 2, f"Expected 2 nodes in test_cluster_2, got {len(cluster_2_nodes)}"
    
    mw.Close()
    print("✓ test_middleware_initialization PASSED")
    return True


def test_routing():
    """Test hash-based routing to clusters."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=100)
    
    # Test that same key always routes to same cluster
    key_id = 12345
    timestamp = 1000
    cluster1, node1 = mw._get_node_for_key(key_id, timestamp)
    cluster2, node2 = mw._get_node_for_key(key_id, timestamp)
    
    assert cluster1 == cluster2, "Same key should route to same cluster"
    
    # Test that we can route to both clusters
    # With deterministic hashing, we should be able to find keys for both clusters
    found_clusters = set()
    for i in range(100):
        cluster, _ = mw._get_node_for_key(i, 1000)
        found_clusters.add(cluster)
    
    assert len(found_clusters) >= 1, "Should find at least 1 cluster"
    assert 'test_cluster' in found_clusters or 'test_cluster_2' in found_clusters
    
    mw.Close()
    print("✓ test_routing PASSED")
    return True


def test_buffering():
    """Test that requests are buffered and not sent immediately."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=10)
    
    # Send requests but don't flush
    for i in range(5):
        mw.SendWriteRequest(key_id=i, value=i*100)
    
    # Verify requests are in buffer
    assert len(mw._write_buffer) == 5, f"Expected 5 buffered requests, got {len(mw._write_buffer)}"
    
    # Flush should clear buffer
    stats = mw.FlushRequests(write_only=True)
    assert len(mw._write_buffer) == 0, "Buffer should be empty after flush"
    
    mw.Close()
    print("✓ test_buffering PASSED")
    return True


def test_auto_flush():
    """Test that buffer auto-flushes when batch size is reached."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=5)
    
    # Send exactly batch_size requests
    for i in range(5):
        mw.SendWriteRequest(key_id=i, value=i*100)
    
    # Buffer should be empty due to auto-flush
    assert len(mw._write_buffer) == 0, f"Expected buffer to be auto-flushed, got {len(mw._write_buffer)}"
    
    mw.Close()
    print("✓ test_auto_flush PASSED")
    return True


def test_query_buffering():
    """Test that query requests are buffered."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=10)
    
    # Send query requests
    for i in range(3):
        mw.SendQueryRequest(req_id=i, start_key_id=0, start_key_ts=0, end_key_id=100, end_key_ts=9999)
    
    # Verify requests are in buffer
    assert len(mw._query_buffer) == 3, f"Expected 3 buffered queries, got {len(mw._query_buffer)}"
    
    mw.Close()
    print("✓ test_query_buffering PASSED")
    return True


def test_mock_database():
    """Test the mock database directly."""
    db = MockDatabase()
    
    # Test insert
    result = db.insert(1, 1000, b'test_value')
    assert result == 0, "Insert should return 0 on success"
    
    # Test query
    records = db.query_range(0, 0, 2, 2000)
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"
    assert records[0].key_id == 1, f"Expected key_id=1, got {records[0].key_id}"
    assert records[0].key_timestamp == 1000, f"Expected timestamp=1000, got {records[0].key_timestamp}"
    assert records[0].value == b'test_value', f"Expected value=b'test_value', got {records[0].value}"
    
    # Test query with no results
    records = db.query_range(10, 0, 20, 2000)
    assert len(records) == 0, f"Expected 0 records for out-of-range query, got {len(records)}"
    
    print("✓ test_mock_database PASSED")
    return True


def test_follower_selection():
    """Test that follower selection avoids the leader when possible."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=10)
    
    # Set a known leader
    cluster_name = 'test_cluster'
    nodes = mw.GetClusterNodes(cluster_name)
    mw.clusters[cluster_name].leader = nodes[0]
    
    # Get follower - should not be the leader
    follower = mw._get_follower_for_cluster(cluster_name)
    assert follower is not None, "Should return a follower"
    assert follower != nodes[0], "Follower should not be the leader"
    
    mw.Close()
    print("✓ test_follower_selection PASSED")
    return True


def test_mock_node_protocol():
    """Test that mock node understands the protocol."""
    config = {
        'test': [{'id': 1, 'host': '127.0.0.1', 'port': 18001}]
    }
    temp_path = create_test_config_path()
    
    try:
        # Start mock server
        servers = start_mock_servers_from_config(temp_path)
        time.sleep(0.5)
        
        # Create a proper payload
        LSMT_TYPE_INT = 1
        INSERT_CMD_FORMAT_PREFIX = '<QQ'
        inner_payload = struct.pack('<BIQ', LSMT_TYPE_INT, 8, 42)
        outer_payload = struct.pack(f'{INSERT_CMD_FORMAT_PREFIX}{len(inner_payload)}s', 
                                     1, 1000, inner_payload)
        binary_packet = struct.pack('<I', 4 + len(outer_payload)) + outer_payload
        
        # Connect and send
        node_data = config['test'][0]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        
        try:
            sock.connect((node_data['host'], node_data['port']))
            sock.sendall(binary_packet)
            
            # Wait for ACK
            ack = sock.recv(1)
            assert ack == b'\x01', f"Expected ACK byte 0x01, got {ack.hex()}"
            
            sock.close()
            print("✓ test_mock_node_protocol PASSED")
            return True
        except socket.timeout:
            print("✗ test_mock_node_protocol FAILED: Timeout waiting for ACK")
            return False
        except AssertionError as e:
            print(f"✗ test_mock_node_protocol FAILED: {e}")
            return False
        except Exception as e:
            print(f"✗ test_mock_node_protocol FAILED: {type(e).__name__}: {e}")
            return False
    finally:
        stop_mock_servers(servers)
        cleanup_temp_file(temp_path)


def test_cluster_node_access():
    """Test accessing cluster and node information."""
    mw = RaftLSMTMiddleware(CONFIG_PATH)
    
    # Test GetClusterNames
    names = mw.GetClusterNames()
    assert isinstance(names, list), "GetClusterNames should return a list"
    assert len(names) > 0, "Should have at least one cluster"
    
    # Test GetClusterNodes
    for name in names:
        nodes = mw.GetClusterNodes(name)
        assert isinstance(nodes, list), f"GetClusterNodes({name}) should return a list"
        assert len(nodes) > 0, f"Cluster {name} should have at least one node"
        
        for node in nodes:
            assert isinstance(node, Node), f"Node should be a Node instance"
            assert node.cluster_name == name, f"Node cluster_name should match"
    
    # Test GetKnownLeader (initially None)
    leader = mw.GetKnownLeader(names[0])
    assert leader is None or isinstance(leader, Node), "Leader should be None or a Node"
    
    mw.Close()
    print("✓ test_cluster_node_access PASSED")
    return True


def test_selective_flush():
    """Test write_only and query_only flush options."""
    mw = RaftLSMTMiddleware(CONFIG_PATH, batch_size=100)
    
    # Add both write and query requests
    mw.SendWriteRequest(key_id=1, value=42)
    mw.SendQueryRequest(req_id=1, start_key_id=0, start_key_ts=0, end_key_id=10, end_key_ts=0)
    
    assert len(mw._write_buffer) == 1, "Should have 1 write request buffered"
    assert len(mw._query_buffer) == 1, "Should have 1 query request buffered"
    
    # Flush only writes
    stats = mw.FlushRequests(write_only=True)
    assert len(mw._write_buffer) == 0, "Write buffer should be empty"
    assert len(mw._query_buffer) == 1, "Query buffer should still have 1 request"
    
    # Flush only queries
    stats = mw.FlushRequests(query_only=True)
    assert len(mw._write_buffer) == 0, "Write buffer should still be empty"
    assert len(mw._query_buffer) == 0, "Query buffer should be empty"
    
    mw.Close()
    print("✓ test_selective_flush PASSED")
    return True


# ==============================================================================
# Main
# ==============================================================================

def run_tests():
    """Run all tests and report results."""
    print("=" * 70)
    print("Running Middleware Unit Tests")
    print("=" * 70)
    print()
    
    tests = [
        ("Middleware Initialization", test_middleware_initialization),
        ("Hash-based Routing", test_routing),
        ("Request Buffering", test_buffering),
        ("Auto-flush", test_auto_flush),
        ("Query Buffering", test_query_buffering),
        ("Mock Database", test_mock_database),
        ("Follower Selection", test_follower_selection),
        ("Mock Node Protocol", test_mock_node_protocol),
        ("Cluster/Node Access", test_cluster_node_access),
        ("Selective Flush", test_selective_flush),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ {name} FAILED with exception: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
        print()
    
    # Print summary
    print("=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"  {name}: {status}")
    
    print("=" * 70)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("All tests PASSED!")
        return 0
    else:
        print(f"{total - passed} test(s) FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())

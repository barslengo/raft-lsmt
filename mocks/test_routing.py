import unittest
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.db_datatypes import Record, Node
from client.router import LeaderRegistry
from client.routing_strats import RoundRobinRoutingStrategy

class TestRoundRobinRoutingStrategy(unittest.TestCase):
    def test_round_robin_clusters_and_leader(self):
        # Setup clusters:
        # Cluster A: nodes 1, 2, 3
        # Cluster B: nodes 4, 5, 6
        # Cluster C: nodes 7, 8, 9
        nodes_a = [
            Node("A", 1, "127.0.0.1", 18091),
            Node("A", 2, "127.0.0.1", 18092),
            Node("A", 3, "127.0.0.1", 18093),
        ]
        nodes_b = [
            Node("B", 4, "127.0.0.1", 18094),
            Node("B", 5, "127.0.0.1", 18095),
            Node("B", 6, "127.0.0.1", 18096),
        ]
        nodes_c = [
            Node("C", 7, "127.0.0.1", 18097),
            Node("C", 8, "127.0.0.1", 18098),
            Node("C", 9, "127.0.0.1", 18099),
        ]
        
        clusters = {
            "A": nodes_a,
            "B": nodes_b,
            "C": nodes_c,
        }
        
        leader_registry = LeaderRegistry()
        # Set leaders for each cluster
        leader_registry.set_leader(nodes_a[1]) # node 2 is leader of A
        leader_registry.set_leader(nodes_b[2]) # node 6 is leader of B
        # Let's NOT set a leader for C (to test fallback)
        
        strategy = RoundRobinRoutingStrategy()
        
        # We perform 6 inserts.
        # Since cluster names are sorted: "A", "B", "C".
        # 1st insert: should go to cluster A, and pick node 2 (leader)
        # 2nd insert: should go to cluster B, and pick node 6 (leader)
        # 3rd insert: should go to cluster C, and fallback to node 7 (first node in C)
        # 4th insert: should go to cluster A, and pick node 2 (leader)
        # 5th insert: should go to cluster B, and pick node 6 (leader)
        # 6th insert: should go to cluster C, and fallback to node 7
        
        r1 = Record(key_id=10, timestamp=1000, content="val1")
        n1 = strategy.get_node_insert(r1, leader_registry, clusters)
        self.assertEqual(n1.cluster_name, "A")
        self.assertEqual(n1.id, 2)
        
        r2 = Record(key_id=11, timestamp=1001, content="val2")
        n2 = strategy.get_node_insert(r2, leader_registry, clusters)
        self.assertEqual(n2.cluster_name, "B")
        self.assertEqual(n2.id, 6)
        
        r3 = Record(key_id=12, timestamp=1002, content="val3")
        n3 = strategy.get_node_insert(r3, leader_registry, clusters)
        self.assertEqual(n3.cluster_name, "C")
        self.assertEqual(n3.id, 7) # fallback to nodes_c[0]
        
        r4 = Record(key_id=13, timestamp=1003, content="val4")
        n4 = strategy.get_node_insert(r4, leader_registry, clusters)
        self.assertEqual(n4.cluster_name, "A")
        self.assertEqual(n4.id, 2)
        
        r5 = Record(key_id=14, timestamp=1004, content="val5")
        n5 = strategy.get_node_insert(r5, leader_registry, clusters)
        self.assertEqual(n5.cluster_name, "B")
        self.assertEqual(n5.id, 6)
        r6 = Record(key_id=15, timestamp=1005, content="val6")
        n6 = strategy.get_node_insert(r6, leader_registry, clusters)
        self.assertEqual(n6.cluster_name, "C")
        self.assertEqual(n6.id, 7)

        # Test query routing: should query all 3 clusters
        from client.db_datatypes import QueryRequest
        q = QueryRequest(min_id=1, min_ts=0, max_id=10, max_ts=0)
        query_nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(query_nodes), 3)
        self.assertEqual({n.cluster_name for n in query_nodes}, {"A", "B", "C"})

class TestHashRoutingStrategy(unittest.TestCase):
    def test_hash_routing_uses_id_and_timestamp(self):
        nodes_a = [Node("A", 1, "127.0.0.1", 18091)]
        nodes_b = [Node("B", 2, "127.0.0.1", 18092)]
        clusters = {"A": nodes_a, "B": nodes_b}
        
        leader_registry = LeaderRegistry()
        leader_registry.set_leader(nodes_a[0])
        leader_registry.set_leader(nodes_b[0])
        
        from client.routing_strats import HashRoutingStrategy
        strategy = HashRoutingStrategy()
        
        # Test that same key with different timestamps can go to different clusters
        r1 = Record(key_id=42, timestamp=1000, content="val1")
        r2 = Record(key_id=42, timestamp=2000, content="val2")
        
        n1 = strategy.get_node_insert(r1, leader_registry, clusters)
        n2 = strategy.get_node_insert(r2, leader_registry, clusters)
        
        self.assertIn(n1, [nodes_a[0], nodes_b[0]])
        self.assertIn(n2, [nodes_a[0], nodes_b[0]])

        # Test query routing: should query all clusters
        from client.db_datatypes import QueryRequest
        q = QueryRequest(min_id=1, min_ts=0, max_id=10, max_ts=0)
        query_nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(query_nodes), 2)
        self.assertEqual({n.cluster_name for n in query_nodes}, {"A", "B"})

class TestLeaderRoutingStrategy(unittest.TestCase):
    def test_leader_routing_returns_leader(self):
        nodes_a = [
            Node("A", 1, "127.0.0.1", 18091),
            Node("A", 2, "127.0.0.1", 18092),
        ]
        clusters = {"A": nodes_a}
        
        leader_registry = LeaderRegistry()
        # Set node 2 as leader
        leader_registry.set_leader(nodes_a[1])
        
        from client.routing_strats import LeaderRoutingStrategy
        strategy = LeaderRoutingStrategy()
        
        r1 = Record(key_id=10, timestamp=1000, content="val1")
        n1 = strategy.get_node_insert(r1, leader_registry, clusters)
        # Should return the registered leader (node 2) instead of a random node
        self.assertEqual(n1.id, 2)

        # Test query routing: should query all clusters
        from client.db_datatypes import QueryRequest
        q = QueryRequest(min_id=1, min_ts=0, max_id=10, max_ts=0)
        query_nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(query_nodes), 1)
        self.assertEqual({n.cluster_name for n in query_nodes}, {"A"})

class TestRangeRoutingStrategy(unittest.TestCase):
    def test_range_routing(self):
        # 3 clusters A, B, C with max_keyspace=300
        # Cluster A should get keys <= 100
        # Cluster B should get keys 101 to 200
        # Cluster C should get keys 201 to inf
        nodes_a = [Node("A", 1, "127.0.0.1", 18091)]
        nodes_b = [Node("B", 2, "127.0.0.1", 18092)]
        nodes_c = [Node("C", 3, "127.0.0.1", 18093)]
        clusters = {"A": nodes_a, "B": nodes_b, "C": nodes_c}
        
        leader_registry = LeaderRegistry()
        leader_registry.set_leader(nodes_a[0])
        leader_registry.set_leader(nodes_b[0])
        leader_registry.set_leader(nodes_c[0])
        
        from client.routing_strats import RangeRoutingStrategy
        strategy = RangeRoutingStrategy(max_keyspace=300)
        
        # Test get_node_insert
        # key 50 -> A
        n = strategy.get_node_insert(Record(key_id=50, timestamp=0, content=""), leader_registry, clusters)
        self.assertEqual(n.cluster_name, "A")
        # key 150 -> B
        n = strategy.get_node_insert(Record(key_id=150, timestamp=0, content=""), leader_registry, clusters)
        self.assertEqual(n.cluster_name, "B")
        # key 250 -> C
        n = strategy.get_node_insert(Record(key_id=250, timestamp=0, content=""), leader_registry, clusters)
        self.assertEqual(n.cluster_name, "C")
        
        # Test get_node_query range overlaps
        from client.db_datatypes import QueryRequest
        # Query: min_id=10, max_id=50 -> overlap with A only
        q = QueryRequest(min_id=10, min_ts=0, max_id=50, max_ts=0)
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].cluster_name, "A")
        
        # Query: min_id=50, max_id=150 -> overlap with A and B
        q = QueryRequest(min_id=50, min_ts=0, max_id=150, max_ts=0)
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 2)
        names = {n.cluster_name for n in nodes}
        self.assertEqual(names, {"A", "B"})
        
        # Query: min_id=150, max_id=250 -> overlap with B and C
        q = QueryRequest(min_id=150, min_ts=0, max_id=250, max_ts=0)
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 2)
        names = {n.cluster_name for n in nodes}
        self.assertEqual(names, {"B", "C"})
        
        # Query: min_id=10, max_id=250 -> overlap with A, B, C
        q = QueryRequest(min_id=10, min_ts=0, max_id=250, max_ts=0)
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 3)
        names = {n.cluster_name for n in nodes}
        self.assertEqual(names, {"A", "B", "C"})

class TestFollowerRoundRobinQueryRouting(unittest.TestCase):
    def test_round_robin_selection_among_followers(self):
        # Cluster A has 3 nodes: leader (node 1), follower (node 2), follower (node 3)
        nodes_a = [
            Node("A", 1, "127.0.0.1", 18091),
            Node("A", 2, "127.0.0.1", 18092),
            Node("A", 3, "127.0.0.1", 18093),
        ]
        clusters = {"A": nodes_a}
        
        leader_registry = LeaderRegistry()
        leader_registry.set_leader(nodes_a[0]) # Node 1 is leader
        
        # Test with HashRoutingStrategy
        from client.routing_strats import HashRoutingStrategy
        strategy = HashRoutingStrategy()
        
        from client.db_datatypes import QueryRequest
        q = QueryRequest(min_id=1, min_ts=0, max_id=10, max_ts=0)
        
        # Call multiple times and assert they alternate
        # 1st query: should return node 2
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 2)
        
        # 2nd query: should return node 3
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 3)
        
        # 3rd query: should return node 2 again
        nodes = strategy.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 2)

        # Test with RangeRoutingStrategy
        from client.routing_strats import RangeRoutingStrategy
        strategy_range = RangeRoutingStrategy(max_keyspace=100)
        
        # 1st query: should return node 2
        nodes = strategy_range.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 2)
        
        # 2nd query: should return node 3
        nodes = strategy_range.get_node_query(q, leader_registry, clusters)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 3)

if __name__ == "__main__":
    unittest.main()


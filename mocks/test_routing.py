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

if __name__ == "__main__":
    unittest.main()

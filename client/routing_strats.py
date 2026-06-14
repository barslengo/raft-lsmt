import struct
import hashlib
import random
import threading
from typing import List, Dict
from .router import RoutingStrategy, LeaderRegistry
from .db_datatypes import Record, QueryRequest, Node
import math

class RangeRoutingStrategy(RoutingStrategy):
    """
    Dumb Strategy: Range Partitioning.
    Divide lo spazio delle chiavi in blocchi contigui uguali per ogni cluster.
    Esempio con 3 milioni di chiavi e 3 cluster:
    Cluster A: chiavi 1 - 1.000.000
    Cluster B: chiavi 1.000.001 - 2.000.000
    Cluster C: chiavi 2.000.001 - 3.000.000
    """
    def __init__(self, max_keyspace: int = 5000000):
        # Definisci il limite massimo di chiavi per calcolare le frazioni
        self.max_keyspace = max_keyspace

    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters: Dict[str, List[Node]]) -> Node:
        
        cluster_names = sorted(list(clusters.keys()))
        num_clusters = len(cluster_names)
        
        # Calcola la grandezza di ogni "fetta" (chunk)
        chunk_size = math.ceil(self.max_keyspace / num_clusters)
        
        # Trova l'indice del cluster. 
        # (record.key_id - 1) serve perché le chiavi partono da 1.
        # Il min() garantisce che se per sbaglio arriva una chiave superiore a max_keyspace, 
        # finisca nell'ultimo cluster senza dare IndexError.
        target_idx = min(max(0, record.key_id - 1) // chunk_size, num_clusters - 1)
        
        target_cluster = cluster_names[target_idx]
        nodes = clusters[target_cluster]
        leader = leader_registry.get_leader(target_cluster)
        
        return leader if leader else (nodes[0] if nodes else None)

    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters: Dict[str, List[Node]]) -> List[Node]:
        
        # Per le query di range, manteniamo lo Scatter-Gather interrogando 
        # un follower per ogni cluster (come nelle altre strategie).
        nodes_to_query = []
        for cluster_name, nodes in clusters.items():
            leader = leader_registry.get_leader(cluster_name)
            selected_node = None
            for node in nodes:
                if node != leader:
                    selected_node = node
                    break
            
            if not selected_node and nodes:
                selected_node = nodes[0]
                
            if selected_node:
                nodes_to_query.append(selected_node)
                
        return nodes_to_query

class HashRoutingStrategy(RoutingStrategy):
    """Default strategy: Hashing (id + timestamp) to determine target node."""
    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters: Dict[str, List[Node]]) -> Node:

        val = struct.pack("<QQ", record.key_id, record.timestamp)
        h = int.from_bytes(hashlib.md5(val).digest(), byteorder='big')
        cluster_names = sorted(list(clusters.keys()))
        target_cluster = cluster_names[h % len(cluster_names)]
        
        nodes = clusters[target_cluster]
        leader = leader_registry.get_leader(target_cluster)
        
        # Fallback to the first node if no leader is known
        return leader if leader else (nodes[0] if nodes else None)

    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters: Dict[str, List[Node]]) -> List[Node]:
        nodes_to_query = []
        for cluster_name, nodes in clusters.items():
            leader = leader_registry.get_leader(cluster_name)
            # Prefer a follower; fall back to leader if no followers
            selected_node = None
            for node in nodes:
                if node != leader:
                    selected_node = node
                    break
            
            if not selected_node and nodes:
                selected_node = nodes[0]
                
            if selected_node:
                nodes_to_query.append(selected_node)
                
        return nodes_to_query


class RoundRobinRoutingStrategy(RoutingStrategy):
    """Alternative strategy: Round Robin distribution."""
    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters: Dict[str, List[Node]]) -> Node:
        cluster_names = sorted(list(clusters.keys()))
        if not cluster_names:
            return None

        with self._lock:
            target_cluster = cluster_names[self._counter % len(cluster_names)]
            self._counter += 1

        nodes = clusters[target_cluster]
        leader = leader_registry.get_leader(target_cluster)
        return leader if leader else (nodes[0] if nodes else None)

    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters: Dict[str, List[Node]]) -> List[Node]:
        # For queries, prefer followers
        nodes_to_query = []
        for cluster_name, nodes in clusters.items():
            leader = leader_registry.get_leader(cluster_name)
            selected_node = None
            for node in nodes:
                if node != leader:
                    selected_node = node
                    break
            
            if not selected_node and nodes:
                selected_node = nodes[0]
                
            if selected_node:
                nodes_to_query.append(selected_node)
        return nodes_to_query


class LeaderRoutingStrategy(RoutingStrategy):
    """Strategy that always returns the leader of the target cluster determined by modulo on key_id."""
    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters: Dict[str, List[Node]]) -> Node:
        cluster_names = sorted(list(clusters.keys()))
        target_cluster = cluster_names[record.key_id % len(cluster_names)]
        nodes = clusters[target_cluster]
        leader = leader_registry.get_leader(target_cluster)
        return leader if leader else (nodes[0] if nodes else None)

    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters: Dict[str, List[Node]]) -> List[Node]:
        nodes_to_query = []
        for cluster_name, nodes in clusters.items():
            leader = leader_registry.get_leader(cluster_name)
            selected_node = None
            for node in nodes:
                if node != leader:
                    selected_node = node
                    break
            
            if not selected_node and nodes:
                selected_node = nodes[0]
                
            if selected_node:
                nodes_to_query.append(selected_node)
        return nodes_to_query




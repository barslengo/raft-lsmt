import threading
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Callable
from .types import Record, QueryRequest, Node

class LeaderRegistry:
    """Thread-safe registry to track the current leader of each cluster."""
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
    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters: Dict[str, List[Node]]) -> Node:
        pass

    @abstractmethod
    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters: Dict[str, List[Node]]) -> List[Node]:
        pass

class Router:
    """Orchestrator for routing logic. Keeps track of cluster state."""
    
    def __init__(self, strategy: RoutingStrategy):
        self.strategy = strategy
        self.clusters: Dict[str, List[Node]] = {}
        self.leader_registry = LeaderRegistry()

    def update_topology(self, clusters: Dict[str, List[Node]]):
        """Updates the cluster map."""
        self.clusters = clusters
        
    def get_node_insert(self, record: Record) -> Node:
        return self.strategy.get_node_insert(
            record, self.leader_registry, self.clusters
        )

    def get_nodes_for_read(self, query: QueryRequest) -> List[Node]:
        return self.strategy.get_node_query(
            query, self.leader_registry, self.clusters
        )

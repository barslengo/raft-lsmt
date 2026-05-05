import threading
from typing import List, Dict, Any, Optional, Tuple, Callable
from types import Record, QueryRequest

class RoutingStrategy(ABC):
    """Abstract base class for routing strategies."""
    @abstractmethod
    def get_node_insert(self,
                        record: Record,
                        leader_registry: LeaderRegistry,
                        clusters) -> Node:
        pass

    @abstractmethod
    def get_node_query(self,
                       query: QueryRequest,
                       leader_registry: LeaderRegistry,
                       clusters) -> List['Node']:
        pass

class Router:
    """Orchestrator for routing logic. Keeps track of cluster state."""
    
    def __init__(self, strategy: RoutingStrategy):
        self.strategy = strategy
        self.clusters: Dict[str, List['Node']] = {}
        self.leader_registry = LeaderRegistry()

    def update_topology(self, clusters: Dict[str, List['Node']]):
        """Updates the cluster map."""
        self.clusters = clusters
        
    def get_node_for_write(self, record: Record) -> 'Node':
        return self.strategy.get_node_for_write(
            record, self.clusters, self.leader_registry
        )

    def get_nodes_for_read(self, query: QueryRequest) -> List['Node']:
        return self.strategy.get_nodes_for_read(
            query, self.clusters, self.leader_registry
        )

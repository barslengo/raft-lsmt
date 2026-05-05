from router import RoutingStrategy, LeaderRegistry

class HashRoutingStrategy(RoutingStrategy):
    """Default strategy: Hashing (id + timestamp) to determine target node."""
    def get_node_insert(self,
                        record,
                        leader_registry: LeaderRegistry,
                        clusters) -> Node:

        key_id, timestamp, _ = record
        val = struct.pack("<QQ", key_id, timestamp)
        h = int(hashlib.md5(val).hexdigest(), 16)
        cluster_names = sorted(list(clusters.keys()))
        target_cluster = cluster_names[h % len(cluster_names)]
        
        nodes = clusters[target_cluster]
        leader = leader_registry.get_leader(target_cluster)
        
        # Fallback to the first node if no leader is known
        return leader if leader else (nodes[0] if nodes else None)

    def get_node_query(self,
                       query,
                       leader_registry: LeaderRegistry,
                       clusters) -> List['Node']:
        nodes_to_query =[]
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

    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        with self._lock:
            node = nodes[self._counter % len(nodes)]
            self._counter += 1
            return node

class LeaderRoutingStrategy(RoutingStrategy):
    """Strategy that always returns a random node."""
    def get_node(self, key_id: int, timestamp: int, nodes: List[Node]) -> Node:
        return random.choice(nodes)




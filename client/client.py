import socket
import time
import itertools

TIMEOUT_SECONDS = 2.0  # Timeout for socket operations

class RaftClient:
    def __init__(self, cluster_nodes, leader_id=None):
        """
        cluster_nodes: Dict { node_id: (host, port) }
        """
        self.nodes = cluster_nodes
        self.node_ids = list(self.nodes.keys())
        # Cycle iterator to implement Round-Robin
        self.node_cycle = itertools.cycle(self.node_ids)

        self.priority_node = leader_id
        self.current_node_id = None
        self.sock = None

        self.connect_next_available()

    def _recv_exact(self, sock, n_bytes):
        """
        Helper to ensure we receive exactly n_bytes. 
        """
        data = b''
        while len(data) < n_bytes:
            try:
                chunk = sock.recv(n_bytes - len(data))
                if not chunk:
                    raise ConnectionError("Server closed connection")
                data += chunk
            except socket.timeout:
                raise ConnectionError("Socket timed out waiting for ACKs")
        return data

    def connect_next_available(self):
        """
        Cycles through the cluster nodes until a connection is established.
        """
        if self.sock:
            try: self.sock.close()
            except: pass

        print(f"[Consumer] looking for a Raft Leader...")

        # Try to connect indefinitely until we find a responsive node
        while True:
            if self.priority_node is not None:
                next_id = self.priority_node
                self.priority_node = None
            else:
                next_id = next(self.node_cycle)

            host, port = self.nodes[next_id]

            try:
                # print(f"[Consumer] Trying Node {next_id} ({host}:{port})...")
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(TIMEOUT_SECONDS)
                self.sock.connect((host, port))

                self.current_node_id = next_id
                print(f"[Consumer] Connected to Node {next_id} ({host}:{port})")
                return # Success

            except socket.error as e:
                # print(f"[Consumer] Failed to connect to Node {next_id}: {e}")
                try: self.sock.close()
                except: pass
                # Short sleep to avoid CPU spin if whole cluster is down
                time.sleep(0.1) 

    def send_batch_reliable(self, batch_items):
        """
        Sends a batch. If the connection drops or the node turns out to be
        a Follower (connection closed), we switch to the next node and retry.
        """
        if not batch_items:
            return

        while True:
            try:
                # 1. Send all items
                data_blob = b''.join(batch_items)
                self.sock.sendall(data_blob)

                # 2. Wait for ACKs
                # If the node is a Follower, your C code disconnects us here.
                # recv_exact raises ConnectionError, triggering the except block.
                expected_acks = len(batch_items)
                self._recv_exact(self.sock, expected_acks)

                # 3. Success!
                return

            except (socket.error, ConnectionError) as e:
                print(f"[Consumer] Error on Node {self.current_node_id}: {e}")
                print(f"[Consumer] Switching nodes and re-sending batch...")

                # FAILOVER LOGIC:
                # Pick the next node in the dictionary and reconnect
                self.connect_next_available()

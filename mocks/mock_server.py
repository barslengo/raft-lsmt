import socket
import threading
import struct
import select

# Constants matching dbclient
LSMT_TYPE_INT = 1
QUERY_REQ_FMT = "<Q 16s 16s"
QUERY_REQ_SIZE = 40
QUERY_RESP_HEADER_FMT = "<Q Q B 16s 16s Q I"
QUERY_RESP_HEADER_SIZE = 61

class MockDatabaseServer:
    def __init__(self, host: str, write_port: int, leader_id: int = 1, is_leader: bool = True):
        self.host = host
        self.write_port = write_port
        self.read_port = write_port + 1000
        self.leader_id = leader_id
        self.is_leader = is_leader
        self.running = False
        
        self.write_sock = None
        self.read_sock = None
        self.threads = []

    def start(self):
        self.running = True
        self.write_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.write_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.write_sock.bind((self.host, self.write_port))
        self.write_sock.listen(128)
        
        self.read_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.read_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.read_sock.bind((self.host, self.read_port))
        self.read_sock.listen(128)
        
        t1 = threading.Thread(target=self._accept_write_conn, daemon=True)
        t2 = threading.Thread(target=self._accept_read_conn, daemon=True)
        t1.start()
        t2.start()
        self.threads.extend([t1, t2])

    def stop(self):
        self.running = False
        if self.write_sock:
            try:
                self.write_sock.close()
            except Exception:
                pass
        if self.read_sock:
            try:
                self.read_sock.close()
            except Exception:
                pass
        for t in self.threads:
            try:
                t.join(timeout=1.0)
            except Exception:
                pass

    def _accept_write_conn(self):
        while self.running:
            try:
                r, _, _ = select.select([self.write_sock], [], [], 0.2)
                if not r:
                    continue
                conn, _ = self.write_sock.accept()
                t = threading.Thread(target=self._handle_write, args=(conn,), daemon=True)
                t.start()
            except Exception:
                break

    def _accept_read_conn(self):
        while self.running:
            try:
                r, _, _ = select.select([self.read_sock], [], [], 0.2)
                if not r:
                    continue
                conn, _ = self.read_sock.accept()
                t = threading.Thread(target=self._handle_read, args=(conn,), daemon=True)
                t.start()
            except Exception:
                break

    def _handle_write(self, conn):
        try:
            if not self.is_leader:
                # Send redirect message in the format expected by the client
                redirect_msg = f"REDIRECT {self.leader_id} 127.0.0.1 {self.write_port}\n".encode('utf-8')
                conn.sendall(redirect_msg)
                conn.close()
            else:
                # Hold connection or read incoming data to act as leader
                while self.running:
                    r, _, _ = select.select([conn], [], [], 0.2)
                    if r:
                        data = conn.recv(4096)
                        if not data:
                            break
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _handle_read(self, conn):
        try:
            while self.running:
                req_data = bytearray()
                while len(req_data) < QUERY_REQ_SIZE and self.running:
                    r, _, _ = select.select([conn], [], [], 0.2)
                    if not r:
                        continue
                    chunk = conn.recv(QUERY_REQ_SIZE - len(req_data))
                    if not chunk:
                        return
                    req_data.extend(chunk)
                
                if len(req_data) < QUERY_REQ_SIZE:
                    return

                req_id, start_key_raw, end_key_raw = struct.unpack(QUERY_REQ_FMT, bytes(req_data))
                min_id, min_ts = struct.unpack("<QQ", start_key_raw)
                max_id, max_ts = struct.unpack("<QQ", end_key_raw)

                # Generate fake response records
                records_count = min(10, max(0, max_id - min_id + 1))
                body_data = bytearray()
                actual_max_id = min_id
                
                for idx in range(records_count):
                    key_id = min_id + idx
                    actual_max_id = key_id
                    content_raw = struct.pack("<Q", key_id * 10)
                    record_bytes = struct.pack("<QQBI", key_id, 0, LSMT_TYPE_INT, 8) + content_raw
                    body_data.extend(record_bytes)
                
                limit_reached = 1 if (actual_max_id < max_id) else 0
                min_key_raw = struct.pack("<QQ", min_id, 0)
                max_key_raw = struct.pack("<QQ", actual_max_id, 0)
                records_bytes = len(body_data)
                total_size = QUERY_RESP_HEADER_SIZE + records_bytes

                header_data = struct.pack(
                    QUERY_RESP_HEADER_FMT,
                    total_size,
                    req_id,
                    limit_reached,
                    min_key_raw,
                    max_key_raw,
                    records_bytes,
                    records_count
                )
                
                conn.sendall(header_data + body_data)
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

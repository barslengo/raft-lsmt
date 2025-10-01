import socket
import struct

class TicketdClient:
    """
    A client for interacting with the ticketd server.
    """

    def __init__(self, host='127.0.0.1', port=8000):
        """
        Initializes the client.

        Args:
            host (str): The server's hostname or IP address.
            port (int): The server's client port.
        """
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        """Establishes a connection to the server."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
        except socket.error as e:
            print(f"Failed to connect: {e}")
            self.sock = None

    def close(self):
        """Closes the connection to the server."""
        if self.sock:
            self.sock.close()
            print("Connection closed.")
            self.sock = None

    def get(self, key: int):
        """
        Sends a GET request to the server.

        Args:
            key (int): A 128-bit integer key.
        """
        if not self.sock:
            print("Not connected.")
            return

        # Pack the request code and the 128-bit key
        # The server expects a 128-bit unsigned integer (sl_uint128_t).
        # In Python's struct, we can represent this as two 64-bit unsigned long longs (Q).
        request = struct.pack('>BQQ', 1, (key >> 64) & 0xFFFFFFFFFFFFFFFF, key & 0xFFFFFFFFFFFFFFFF)
        self.sock.sendall(request)
        response = self.sock.recv(1024)
        print(f"Received: {response.decode('utf-8')}")


    def insert(self, data: str):
        """
        Sends an INSERT request to the server.

        Args:
            data (str): The data to be inserted.
        """
        if not self.sock:
            print("Not connected.")
            return

        # Prepend the INSERT_REQUEST_CODE to the data
        request = b'\x02' + data.encode('utf-8')
        self.sock.sendall(request)
        response = self.sock.recv(1024)
        print(f"Received: {response.decode('utf-8')}")

if __name__ == '__main__':
    # Replace with the actual client port of your server from the command line arguments
    client = TicketdClient(port=8001)
    client.connect()

    if client.sock:
        # Example INSERT request
        client.insert("some_data_to_insert")

        # Example GET request with a dummy 128-bit key
        # In a real application, this key would be meaningful
        dummy_key = 123456789012345678901234567890123456789
        client.get(dummy_key)

        client.close()

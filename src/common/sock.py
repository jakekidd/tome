import socket
import threading

class SocketServer:
    def __init__(self, host='localhost', port=65432):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = []
        self.active = True

    def start_server(self):
        try:
            self.socket.bind((self.host, self.port))
            self.socket.listen()
            print(f"Server started on {self.host}:{self.port}")
            self.accept_connections()
        except Exception as e:
            print(f"Failed to start server: {e}")
            self.socket.close()

    def accept_connections(self):
        while self.active:
            client, address = self.socket.accept()
            print(f"Connected to {address}")
            self.connections.append(client)
            thread = threading.Thread(target=self.handle_client, args=(client,))
            thread.start()

    def handle_client(self, client):
        while self.active:
            try:
                data = client.recv(1024)
                if not data:
                    break
                # Handle received data or send responses
                print(f"Received data: {data.decode()}")
                self.broadcast(data)
            except Exception as e:
                print(f"Error handling data from client: {e}")
                break
        client.close()

    def broadcast(self, message):
        for conn in self.connections:
            try:
                conn.sendall(message)
            except Exception as e:
                print(f"Failed to send message to {conn}: {e}")
                self.connections.remove(conn)

    def stop_server(self):
        self.active = False
        self.socket.close()
        for conn in self.connections:
            conn.close()
        print("Server stopped.")

# Example usage
if __name__ == "__main__":
    server = SocketServer()
    try:
        server.start_server()
    except KeyboardInterrupt:
        server.stop_server()

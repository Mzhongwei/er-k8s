import socket
import time

LISTEN_HOST = "0.0.0.0"
GRAPH_CONSTRUCTION_LISTEN_PORT = 8080

EMBEDDING_TRAINING_SERVICE = {"HOST": "service-embedding-training", "PORT": 80}

def handle_client_connection(client_socket):
    with client_socket:
        graph_path = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {graph_path}")

        try:
            with open(graph_path, "r") as f:
                content = f.read()
                print(f"Graph file content: {content}")
        except FileNotFoundError:
            print(f"ERROR: Graph file {graph_path} not found", flush=True)
            return

        sequences = "sequences_simulated"
        time.sleep(1)
        send(sequences, EMBEDDING_TRAINING_SERVICE)
        print("Sequences sent to EMBEDDING_TRAINING_SERVICE", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, GRAPH_CONSTRUCTION_LISTEN_PORT))
    server_socket.listen()

    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    handle_client_connection(client_socket)

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    daemon()
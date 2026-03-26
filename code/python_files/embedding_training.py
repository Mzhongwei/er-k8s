import socket
import time
import os

LISTEN_HOST = "0.0.0.0"
RANDOM_WALK_LISTEN_PORT = 8080

CALCULATING_SIMILARITY_SERVICE = {"HOST": "service-calculating-similarity", "PORT": 81}

embedding_model_path = "/pipeline/embedding/embedding_model.txt"

def handle_client_connection(client_socket):
    try:
        with client_socket:
            data = client_socket.recv(1024).decode("utf-8").strip()
            print(f"Received data: {data}")

            os.makedirs(os.path.dirname(embedding_model_path), exist_ok=True)
            with open(embedding_model_path, "w", encoding="utf-8") as f:
                f.write("embedding_model_content")

            send(embedding_model_path, CALCULATING_SIMILARITY_SERVICE)
            print("Embedding model sent to CALCULATING_SIMILARITY_SERVICE", flush=True)
            
    except OSError as e:
        print(f"ERROR in handle_client_connection: {e}", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((LISTEN_HOST, RANDOM_WALK_LISTEN_PORT))
    server_socket.listen(1)

    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    handle_client_connection(client_socket)

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    daemon()
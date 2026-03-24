import socket
import time

LISTEN_HOST = "0.0.0.0"
RANDOM_WALK_LISTEN_PORT = 8080

# INCREMENTAL MODE
CALCULATING_SIMILARITY_SERVICE = {"HOST": "service-calculating-similarity", "PORT": 80}

MODE = "INCREMENTAL"

def handle_client_connection(client_socket):
    try:
        with client_socket:
            mode_data = client_socket.recv(1024).decode("utf-8").strip()
            if mode_data != MODE:
                print(f"Received mode {mode_data} does not match expected mode {MODE}. Ignoring.")
                return
            data = client_socket.recv(1024).decode("utf-8").strip()
            print(f"Received data: {data}")

            embedding_model = "embedding_model_simulated"
            send(embedding_model, CALCULATING_SIMILARITY_SERVICE)
            print("Embedding model sent to CALCULATING_SIMILARITY_SERVICE", flush=True)
            
    except OSError as e:
        print(f"ERROR in handle_client_connection: {e}", flush=True)

def send(payload, service):
    mode_data = (MODE + "\n").encode("utf-8")
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        sock.sendall(mode_data)
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)
        time.sleep(0.1)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((LISTEN_HOST, RANDOM_WALK_LISTEN_PORT))
    server_socket.listen(1)

    client_socket, addr = server_socket.accept()
    print(f"Accepted connection from {addr}")
    handle_client_connection(client_socket)

if __name__ == "__main__":
	main()
import socket
import time

LISTEN_HOST = "0.0.0.0"
NORMALIZATION_LISTEN_PORT = 8080

# INCREMENTAL MODE
RANDOM_WALK_SERVICE = {"HOST": "service-random-walk", "PORT": 80}

MODE = "INCREMENTAL"

def handle_client_connection(client_socket):
    with client_socket:
        mode_data = client_socket.recv(1024).decode("utf-8").strip()
        if mode_data != MODE:
            print(f"Received mode {mode_data} does not match expected mode {MODE}. Ignoring.")
            return
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

def send(payload,service):
    mode_data = (MODE + "\n").encode("utf-8")
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        sock.sendall(mode_data)
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, NORMALIZATION_LISTEN_PORT))
    server_socket.listen()

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        handle_client_connection(client_socket)
        representation_graph = "representation_graph_simulated"
        time.sleep(1)
        send(representation_graph, RANDOM_WALK_SERVICE)
        print("Representation graph sent to RANDOM_WALK_SERVICE", flush=True)

if __name__ == "__main__":
	main()
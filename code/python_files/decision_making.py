import socket
import time

LISTEN_HOST = "0.0.0.0"
CALCULATING_SIMILARITY_LISTEN_PORT = 8080

# FINAL TASK OF THE PIPELINE

MODE = "INCREMENTAL"

def handle_client_connection(client_socket):
    with client_socket:
        mode_data = client_socket.recv(1024).decode("utf-8").strip()
        if mode_data != MODE:
            print(f"Received mode {mode_data} does not match expected mode {MODE}. Ignoring.")
            return
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, CALCULATING_SIMILARITY_LISTEN_PORT))
    server_socket.listen()

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        handle_client_connection(client_socket)
        print("Prediction matching completed", flush=True)

if __name__ == "__main__":
	main()
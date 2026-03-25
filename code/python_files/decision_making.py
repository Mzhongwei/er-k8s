import socket
import time

LISTEN_HOST = "0.0.0.0"
CALCULATING_SIMILARITY_LISTEN_PORT = 8080

# FINAL TASK OF THE PIPELINE

def handle_client_connection(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")
        
        print("Prediction matching completed", flush=True)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, CALCULATING_SIMILARITY_LISTEN_PORT))
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
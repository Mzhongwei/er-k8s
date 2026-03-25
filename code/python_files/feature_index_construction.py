import socket
import time

LISTEN_HOST = "0.0.0.0"
CG_FEATURE_EXTRACTION_LISTEN_PORT = 8080

CANDIDATE_ENUMERATION_SERVICE = {"HOST": "service-candidate-enumeration", "PORT": 81}

def handle_client_connection(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

        cg_features_index = "cg_features_index_simulated"
        time.sleep(1)
        send(cg_features_index, CANDIDATE_ENUMERATION_SERVICE)
        print("CG features index sent to CANDIDATE_ENUMERATION_SERVICE", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, CG_FEATURE_EXTRACTION_LISTEN_PORT))
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
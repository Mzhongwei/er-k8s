import socket
import time
import threading

LISTEN_HOST = "0.0.0.0"
CANDIDATE_ENUMERATION_LISTEN_PORT = 8080
EMBEDDING_TRAINING_LISTEN_PORT = 8081

DECISION_MAKING_SERVICE = {"HOST": "service-decision-making", "PORT": 80}

threads = []

def handle_client_connection(client_socket):
    try:
        with client_socket:
            data = client_socket.recv(1024).decode("utf-8").strip()
            print(f"Received data: {data}")
    except OSError as e:
        print(f"ERROR in handle_client_connection: {e}", flush=True)

def accept_connections(server_socket, port_name):
    try:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection on {port_name} from {addr}")
        t = threading.Thread(target=handle_client_connection, args=(client_socket,))
        t.daemon = True
        threads.append(t)
        t.start()
    except Exception as e:
        print(f"ERROR in accept_connections ({port_name}): {e}", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)


def main():
    server_socket_embedding_training = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_embedding_training.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket_embedding_training.bind((LISTEN_HOST, EMBEDDING_TRAINING_LISTEN_PORT))
    server_socket_embedding_training.listen(5)

    server_socket_candidate_enumeration = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_candidate_enumeration.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket_candidate_enumeration.bind((LISTEN_HOST, CANDIDATE_ENUMERATION_LISTEN_PORT))
    server_socket_candidate_enumeration.listen(5)

    t = threading.Thread(target=accept_connections, args=(server_socket_embedding_training, "EMBEDDING_TRAINING_LISTEN_PORT"))
    t.daemon = True
    threads.append(t)

    t = threading.Thread(target=accept_connections, args=(server_socket_candidate_enumeration, "CANDIDATE_ENUMERATION_LISTEN_PORT"))
    t.daemon = True
    threads.append(t)

    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    time.sleep(1)
    similarity_data = "similarity_data_simulated"
    send(similarity_data, DECISION_MAKING_SERVICE)
    print("Similarity data sent to DECISION_MAKING_SERVICE", flush=True)

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    daemon()
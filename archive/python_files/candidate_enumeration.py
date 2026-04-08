import socket
import time
import threading

LISTEN_HOST = "0.0.0.0"
CG_FEATURE_EXTRACTION_LISTEN_PORT = 8080
FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT = 8081

CALCULATING_SIMILARITY_SERVICE = {"HOST": "service-calculating-similarity", "PORT": 80}
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 82}

threads = []

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

def handle_client_connection(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

        if(data.startswith("/pipeline/cg_features_index/")):
            with open(data, "r") as f:
                content = f.read()
                print(f"CG features index file content: {content}")

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket_feature_index_construction = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_feature_index_construction.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket_feature_index_construction.bind((LISTEN_HOST, FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT))
    server_socket_feature_index_construction.listen(5)

    server_socket_cg_feature_extraction = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_cg_feature_extraction.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket_cg_feature_extraction.bind((LISTEN_HOST, CG_FEATURE_EXTRACTION_LISTEN_PORT))
    server_socket_cg_feature_extraction.listen(5)

    t = threading.Thread(
        target=accept_connections,
        args=(server_socket_feature_index_construction, "FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT"),
    )
    t.daemon = True
    threads.append(t)

    t = threading.Thread(
        target=accept_connections,
        args=(server_socket_cg_feature_extraction, "CG_FEATURE_EXTRACTION_LISTEN_PORT"),
    )
    t.daemon = True
    threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    candidate_pairs = "candidate_pairs_simulated"
    send(candidate_pairs, CALCULATING_SIMILARITY_SERVICE)
    print("Candidate pairs sent to CALCULATING_SIMILARITY_SERVICE", flush=True)
    send(candidate_pairs, BERT_INFERENCE_SERVICE)
    print("Candidate pairs sent to BERT_INFERENCE_SERVICE", flush=True)

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    daemon()
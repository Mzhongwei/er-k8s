import socket
import time

LISTEN_HOST = "0.0.0.0"
CG_FEATURE_EXTRACTION_LISTEN_PORT = 8080
FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT = 8081

CALCULATING_SIMILARITY_SERVICE = {"HOST": "service-calculating-similarity", "PORT": 80}
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 82}

def handle_client_connection(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

        candidate_pairs = "candidate_pairs_simulated"
        time.sleep(1)
        send(candidate_pairs, CALCULATING_SIMILARITY_SERVICE)
        print("Candidate pairs sent to CALCULATING_SIMILARITY_SERVICE", flush=True)
        send(candidate_pairs, BERT_INFERENCE_SERVICE)
        print("Candidate pairs sent to BERT_INFERENCE_SERVICE", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    server_socket_feature_index_construction = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_feature_index_construction.bind((LISTEN_HOST, FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT))
    server_socket_feature_index_construction.listen()

    server_socket_cg_feature_extraction = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_cg_feature_extraction.bind((LISTEN_HOST, CG_FEATURE_EXTRACTION_LISTEN_PORT))
    server_socket_cg_feature_extraction.listen()

    while True:
        client_socket, addr = server_socket_feature_index_construction.accept()
        print(f"Accepted connection from {addr}")
        handle_client_connection(client_socket)

if __name__ == "__main__":
	main()
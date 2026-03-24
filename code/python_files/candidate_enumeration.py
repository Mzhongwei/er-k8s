import socket
import time

LISTEN_HOST = "0.0.0.0"
FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT = 8080
CG_FEATURE_EXTRACTION_LISTEN_PORT = 8081

# INCREMENTAL MODE
CALCULATING_SIMILARITY_SERVICE = {"HOST": "service-calculating-similarity", "PORT": 80}

# BATCH MODE
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 80}

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
    if (MODE == "INCREMENTAL"):
        server_socket.bind((LISTEN_HOST, FEATURE_INDEX_CONSTRUCTION_LISTEN_PORT))
    else:
        server_socket.bind((LISTEN_HOST, CG_FEATURE_EXTRACTION_LISTEN_PORT))    
    server_socket.listen()

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        handle_client_connection(client_socket)
        candidate_pairs = "candidate_pairs_simulated"
        time.sleep(1)
        if MODE == "INCREMENTAL":
            send(candidate_pairs, CALCULATING_SIMILARITY_SERVICE)
            print("Candidate pairs sent to CALCULATING_SIMILARITY_SERVICE", flush=True)
        else:
            send(candidate_pairs, BERT_INFERENCE_SERVICE)
            print("Candidate pairs sent to BERT_INFERENCE_SERVICE", flush=True)

if __name__ == "__main__":
	main()
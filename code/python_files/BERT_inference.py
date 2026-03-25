import socket
import time
import threading

LISTEN_HOST = "0.0.0.0"
NORMALIZATION_LISTEN_PORT = 8080
BERT_TRAINING_LISTEN_PORT = 8081
CANDIDATE_ENUMERATION_LISTEN_PORT = 8082

threads = []

def accept_connections(server_socket, port_name):
    try:
        while True:
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


def main():
    server_socket_normalization = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_normalization.bind((LISTEN_HOST, NORMALIZATION_LISTEN_PORT))
    server_socket_normalization.listen()

    server_socket_bert_training = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_bert_training.bind((LISTEN_HOST, BERT_TRAINING_LISTEN_PORT))
    server_socket_bert_training.listen()

    server_socket_candidate_enumeration = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_candidate_enumeration.bind((LISTEN_HOST, CANDIDATE_ENUMERATION_LISTEN_PORT))
    server_socket_candidate_enumeration.listen()

    t = threading.Thread(target=accept_connections, args=(server_socket_normalization, "Normalization"))
    t.daemon = True
    threads.append(t)

    t = threading.Thread(target=accept_connections, args=(server_socket_bert_training, "BERT Training"))
    t.daemon = True
    threads.append(t)

    t = threading.Thread(target=accept_connections, args=(server_socket_candidate_enumeration, "Candidate Enumeration"))
    t.daemon = True
    threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
    
    print("Prediction matching completed", flush=True)
    


if __name__ == "__main__":
	main()
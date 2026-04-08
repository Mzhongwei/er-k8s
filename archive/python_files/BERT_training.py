import socket
import time
import os

LISTEN_HOST = "0.0.0.0"
NORMALIZATION_LISTEN_PORT = 8080

BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 81} 

bert_model_path = "/pipeline/bert/bert_model.txt"

def handle_client_connection(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")
        bert_model = "BERT_model_content"

        os.makedirs(os.path.dirname(bert_model_path), exist_ok=True)
        with open(bert_model_path, "w", encoding="utf-8") as f:
            f.write(bert_model)
        send(bert_model_path, BERT_INFERENCE_SERVICE)
        print("BERT model sent to BERT_INFERENCE_SERVICE", flush=True)

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)
		
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((LISTEN_HOST, NORMALIZATION_LISTEN_PORT))
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
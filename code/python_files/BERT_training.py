import socket
import time

LISTEN_HOST = "0.0.0.0"
NORMALIZATION_LISTEN_PORT = 8080

# BATCH MODE
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 80} 

MODE = "BATCH"

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
    while True:
        print("Running...", flush=True)
        time.sleep(1)

if __name__ == "__main__":
	main()
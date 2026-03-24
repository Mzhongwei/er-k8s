import socket
import time

LISTEN_HOST = "0.0.0.0"
BERT_TRAINING_LISTEN_PORT = 8080
NORMALIZATION_LISTEN_PORT = 8081
CANDIDATE_ENUMERATION_LISTEN_PORT = 8082

# FINAL TASK OF THE PIPELINE

MODE = "BATCH"

def handle_client_connection(client_socket):
    with client_socket:
        mode_data = client_socket.recv(1024).decode("utf-8").strip()
        if mode_data != MODE:
            print(f"Received mode {mode_data} does not match expected mode {MODE}. Ignoring.")
            return
        data = client_socket.recv(1024).decode("utf-8").strip()
        print(f"Received data: {data}")

def main():
    while True:
        print("Running...", flush=True)
        time.sleep(1)

if __name__ == "__main__":
	main()
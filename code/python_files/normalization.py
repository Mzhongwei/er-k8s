import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

GRAPH_CONSTRUCTION_SERVICE = {"HOST": "service-graph-construction", "PORT": 80}
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 80} 
BERT_TRAINING_SERVICE = {"HOST": "service-bert-training", "PORT": 80}
CG_FEATURE_EXTRACTION_SERVICE = {"HOST": "service-cg-feature-extraction", "PORT": 80}

source_data = "source_data_simulated"

def send(payload,service):
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    print("Input : " + source_data, flush=True)
    process_data = "processed_data_simulated"
    time.sleep(1)
    send(process_data, CG_FEATURE_EXTRACTION_SERVICE)
    print("Processed data sent to CG_FEATURE_EXTRACTION_SERVICE", flush=True)
    send(process_data, GRAPH_CONSTRUCTION_SERVICE)
    print("Processed data sent to GRAPH_CONSTRUCTION_SERVICE", flush=True)
    send(process_data, BERT_INFERENCE_SERVICE)
    print("Processed data sent to BERT_INFERENCE_SERVICE", flush=True)
    send(process_data, BERT_TRAINING_SERVICE)
    print("Processed data sent to BERT_TRAINING_SERVICE", flush=True)

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    daemon()
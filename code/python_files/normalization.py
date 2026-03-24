import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

MODE = "INCREMENTAL"

# INCREMENTAL MODE
GRAPH_CONSTRUCTION_SERVICE = {"HOST": "service-graph-construction", "PORT": 80}

# BATCH MODE
BERT_INFERENCE_SERVICE = {"HOST": "service-bert-inference", "PORT": 80} 
BERT_TRAINING_SERVICE = {"HOST": "service-bert-training", "PORT": 80}

# BOTH MODES
CG_FEATURE_EXTRACTION_SERVICE = {"HOST": "service-cg-feature-extraction", "PORT": 80}

source_data = "source_data_simulated"

def send(payload,service):
    mode_data = (MODE + "\n").encode("utf-8")
    with socket.create_connection((service["HOST"], service["PORT"]), timeout=5) as sock:
        sock.sendall(mode_data)
        time.sleep(0.1)
        data = (payload + "\n").encode("utf-8")
        sock.sendall(data)

def main():
    print("Input : " + source_data, flush=True)
    process_data = "processed_data_simulated"
    time.sleep(1)
    send(process_data, CG_FEATURE_EXTRACTION_SERVICE)
    print("Processed data sent to CG_FEATURE_EXTRACTION_SERVICE", flush=True)
    if(MODE == "INCREMENTAL"):
        send(process_data, GRAPH_CONSTRUCTION_SERVICE)
        print("Processed data sent to GRAPH_CONSTRUCTION_SERVICE", flush=True)
    elif(MODE == "BATCH"):
        send(process_data, BERT_INFERENCE_SERVICE)
        print("Processed data sent to BERT_INFERENCE_SERVICE", flush=True)
        send(process_data, BERT_TRAINING_SERVICE)
        print("Processed data sent to BERT_TRAINING_SERVICE", flush=True)

if __name__ == "__main__":
	main()
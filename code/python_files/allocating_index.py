import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

# INCREMENTAL MODE
GRAPH_CONSTRUCTION_HOST = "service-graph-construction"
GRAPH_CONSTRUCTION_PORT = 80

# BATCH MODE
BERT_TRAINING_HOST = "service-bert-training"
BERT_TRAINING_PORT = 80
BERT_INFERENCE_HOST = "service-bert-inference"
BERT_INFERENCE_PORT = 80

# BOTH MODES
CG_FEATURE_EXTRACTION_HOST = "service-cg-feature-extraction"
CG_FEATURE_EXTRACTION_PORT = 80

while True:
    print("Running...")
    time.sleep(1)
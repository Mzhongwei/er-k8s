import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

# INCREMENTAL MODE
EMBEDDING_TRAINING_HOST = "service-embedding-training"
EMBEDDING_TRAINING_PORT = 80

while True:
    print("Running...")
    time.sleep(1)
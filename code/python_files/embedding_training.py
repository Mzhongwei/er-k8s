import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

# INCREMENTAL MODE
CALCULATING_SIMILARITY_HOST = "service-calculating-similarity"
CALCULATING_SIMILARITY_PORT = 80

while True:
    print("Running...")
    time.sleep(1)
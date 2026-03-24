import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

# INCREMENTAL MODE
DECISION_MAKING_HOST = "service-decision-making"
DECISION_MAKING_PORT = 80

while True:
    print("Running...")
    time.sleep(1)
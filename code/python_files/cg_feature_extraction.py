import socket
import time

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080

# INCREMENTAL MODE
FEATURE_INDEX_CONSTRUCTION_HOST = "service-feature-index-construction"
FEATURE_INDEX_CONSTRUCTION_PORT = 80

# BATCH MODE
CANDIDATE_ENUMERATION_HOST = "service-candidate-enumeration"
CANDIDATE_ENUMERATION_PORT = 80

while True:
    print("Running...")
    time.sleep(1)
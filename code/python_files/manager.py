import socket
import time

def daemon():
    while True:
        time.sleep(1)

if __name__ == "__main__":
    daemon()
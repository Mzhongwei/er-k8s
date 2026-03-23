import socket
import time


INPUT_PATH = "/data/input.txt"
PROCESS_HOST = "service-process"
PROCESS_PORT = 80


def read_input(path):
	with open(path, "r", encoding="utf-8") as f:
		return f.read().rstrip("\n")


def send_once(payload):
	data = (payload + "\n").encode("utf-8")
	with socket.create_connection((PROCESS_HOST, PROCESS_PORT), timeout=5) as sock:
		sock.sendall(data)


def main():
	payload = read_input(INPUT_PATH)
	while True:
		try:
			print("Sending content of /data/input.txt to process service...", flush=True)
			send_once(payload)
			print("Done.", flush=True)
			return
		except OSError as exc:
			print(f"Send failed ({exc}), retrying in 2s...", flush=True)
			time.sleep(2)


if __name__ == "__main__":
	main()
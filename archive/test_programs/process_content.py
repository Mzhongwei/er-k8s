import socket
import time


LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080
PRINTER_HOST = "service-printer"
PRINTER_PORT = 80


def forward_payload(payload):
	data = (payload + "..............processed at " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n").encode("utf-8")
	while True:
		try:
			with socket.create_connection((PRINTER_HOST, PRINTER_PORT), timeout=5) as out_sock:
				out_sock.sendall(data)
			return
		except OSError as exc:
			print(f"Forward failed ({exc}), retrying in 2s...", flush=True)
			time.sleep(2)


def main():
	print("Process listening on port 8080", flush=True)
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.bind((LISTEN_HOST, LISTEN_PORT))
		server.listen()

		while True:
			conn, _ = server.accept()
			with conn:
				data = conn.recv(65535)
			incoming = data.decode("utf-8", errors="replace").strip()
			if not incoming:
				continue
			outgoing = f"{incoming} + process (python)"
			print(f"Forwarding: {outgoing}", flush=True)
			forward_payload(incoming)


if __name__ == "__main__":
	main()
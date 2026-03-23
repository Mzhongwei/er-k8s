import socket


LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8080


def main():
	print("Printer listening on port 8080", flush=True)
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.bind((LISTEN_HOST, LISTEN_PORT))
		server.listen()

		while True:
			conn, _ = server.accept()
			with conn:
				data = conn.recv(65535)
			text = data.decode("utf-8", errors="replace").strip()
			if text:
				print(text, flush=True)


if __name__ == "__main__":
	main()

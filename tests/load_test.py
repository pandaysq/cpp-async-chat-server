"""Small, reproducible connection smoke/load check using only the Python stdlib."""

import argparse
import socket
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--clients", type=int, default=100)
    args = parser.parse_args()

    clients: list[socket.socket] = []
    started = time.monotonic()
    try:
        for index in range(args.clients):
            connection = socket.create_connection((args.host, args.port), timeout=5)
            connection.settimeout(5)
            connection.recv(4096)
            connection.sendall(f"tester_{index}\n".encode())
            connection.recv(4096)
            clients.append(connection)
        clients[0].sendall(b"/who\n")
        response = clients[0].recv(16384).decode(errors="replace")
        if "Online:" not in response:
            raise RuntimeError(f"Unexpected /who response: {response!r}")
        elapsed = time.monotonic() - started
        print(f"connected={len(clients)} elapsed={elapsed:.3f}s")
    finally:
        for connection in clients:
            connection.close()


if __name__ == "__main__":
    main()
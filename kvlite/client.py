from __future__ import annotations

import socket


class KVClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.file = self.sock.makefile("rwb")

    def send(self, command: str) -> str:
        self.file.write((command + "\n").encode())
        self.file.flush()
        reply = self.file.readline()
        if not reply:
            raise ConnectionError("server closed the connection")
        return reply.decode().rstrip("\n")

    def close(self) -> None:
        self.file.close()
        self.sock.close()

    def __enter__(self) -> "KVClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def repl(host: str, port: int) -> None:
    with KVClient(host, port) as client:
        print(f"Connected to {host}:{port}. Try: SET name Ada · GET name · INCR visits · STATS · QUIT")
        while True:
            try:
                line = input("kvlite> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            reply = client.send(line)
            print(reply)
            if reply == "+BYE":
                break

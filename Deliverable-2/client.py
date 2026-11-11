"""Interactive TCP task client."""

from __future__ import annotations

import argparse
import json
import socket


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("broker_host", nargs="?", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5002)
    parser.add_argument("payload", nargs="?")
    args = parser.parse_args()
    with socket.create_connection((args.broker_host, args.port), timeout=10) as conn:
        payload = args.payload
        if payload is None:
            payload = input("payload> ")
        conn.sendall(f"SUBMIT {payload}\n".encode("utf-8"))
        buffer = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                return
            buffer += chunk
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                line = raw.decode("utf-8")
                print(line, flush=True)
                if line.startswith("TASK_DONE "):
                    json.loads(line[10:])
                    return
                if line.startswith("TASK_FAILED "):
                    return


if __name__ == "__main__":
    main()

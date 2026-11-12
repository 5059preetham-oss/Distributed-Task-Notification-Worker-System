"""
Submit a task to the broker.
Examples:
  python3 client.py sum 10 20 30
  python3 client.py uppercase "hello mininet"
  python3 client.py reverse "distributed systems"
"""
import json
import socket
import sys

BROKER_HOST = "10.0.0.1"
CLIENT_PORT = 6000


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 client.py sum <numbers...>")
        print('   or: python3 client.py uppercase|reverse "<text>"')
        return

    task = sys.argv[1].lower()
    if task == "sum":
        payload = [float(x) if "." in x else int(x) for x in sys.argv[2:]]
    elif task in ("uppercase", "reverse"):
        payload = " ".join(sys.argv[2:])
    else:
        print("Unsupported task. Use sum, uppercase, or reverse.")
        return

    request = {"task": task, "payload": payload}
    with socket.create_connection((BROKER_HOST, CLIENT_PORT), timeout=10) as sock:
        sock.sendall((json.dumps(request) + "\\n").encode())
        reader = sock.makefile("r", encoding="utf-8")
        response = reader.readline()
        print(json.dumps(json.loads(response), indent=2) if response else "No response")
        reader.close()


if __name__ == "__main__":
    main()

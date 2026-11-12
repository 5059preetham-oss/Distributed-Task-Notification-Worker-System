"""
Worker process. Each worker registers with the broker and waits for tasks.
Usage: python3 worker.py --id worker1
"""
import argparse
import json
import socket
import time

BROKER_HOST = "10.0.0.1"
BROKER_PORT = 5000


def execute(task, payload):
    if task == "sum":
        if not isinstance(payload, list) or not all(isinstance(x, (int, float)) for x in payload):
            raise ValueError("sum payload must be a list of numbers")
        return sum(payload)
    if task == "uppercase":
        if not isinstance(payload, str):
            raise ValueError("uppercase payload must be a string")
        return payload.upper()
    if task == "reverse":
        if not isinstance(payload, str):
            raise ValueError("reverse payload must be a string")
        return payload[::-1]
    raise ValueError("Unsupported task")


def send_json(sock, obj):
    sock.sendall((json.dumps(obj) + "\\n").encode())


def run(worker_id):
    while True:
        try:
            print(f"[{worker_id}] Connecting to broker...")
            with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10) as sock:
                sock.settimeout(None)
                reader = sock.makefile("r", encoding="utf-8")
                send_json(sock, {"type": "REGISTER", "worker_id": worker_id})
                response = reader.readline()
                if not response:
                    raise ConnectionError("Broker closed during registration")
                print(f"[{worker_id}] {response.strip()}")

                for line in reader:
                    message = json.loads(line)
                    if message.get("type") != "TASK":
                        continue
                    task_id = message.get("task_id")
                    try:
                        result = execute(message.get("task"), message.get("payload"))
                        reply = {"type": "RESULT", "task_id": task_id, "result": result}
                        print(f"[{worker_id}] Completed {task_id}: {result}")
                    except Exception as exc:
                        reply = {"type": "RESULT", "task_id": task_id, "result": f"ERROR: {exc}"}
                    send_json(sock, reply)
                reader.close()
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(f"[{worker_id}] Connection error: {exc}; retrying in 2 seconds")
            time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", default="worker1")
    args = parser.parse_args()
    run(args.id)

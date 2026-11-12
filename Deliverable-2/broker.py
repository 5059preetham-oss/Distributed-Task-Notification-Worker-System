"""
TCP task broker and scheduler.

Workers maintain persistent TCP connections to port 5000.
Clients submit JSON tasks to port 6000. The broker assigns each task to an
idle worker, returns the result to the client, and informs the SDN controller
when workers connect or disconnect.
"""
import json
import socket
import threading
import time
import uuid
from collections import deque

HOST = "0.0.0.0"
WORKER_PORT = 5000
CLIENT_PORT = 6000
CONTROLLER_HOST = "10.0.0.254"
CONTROLLER_PORT = 9999

workers = {}
worker_lock = threading.Lock()
queue = deque()
queue_lock = threading.Lock()
pending_clients = {}
pending_lock = threading.Lock()


def notify_controller(action, ip):
    try:
        with socket.create_connection((CONTROLLER_HOST, CONTROLLER_PORT), timeout=3) as conn:
            conn.sendall(f"{action} {ip}\\n".encode())
            return conn.recv(128).decode().strip()
    except OSError as exc:
        print(f"[SDN] Controller notification failed: {exc}")
        return "ERROR"


def send_json(conn, obj):
    conn.sendall((json.dumps(obj) + "\\n").encode())


def schedule_tasks():
    while True:
        task = None
        with queue_lock:
            if queue:
                task = queue[0]

        if task is None:
            time.sleep(0.1)
            continue

        chosen = None
        with worker_lock:
            idle = [w for w in workers.values() if not w["busy"]]
            if idle:
                chosen = min(idle, key=lambda w: w["completed"])

        if chosen is None:
            time.sleep(0.1)
            continue

        with queue_lock:
            if not queue or queue[0]["task_id"] != task["task_id"]:
                continue
            queue.popleft()

        try:
            with worker_lock:
                chosen["busy"] = True
            send_json(chosen["conn"], {"type": "TASK", **task})
            print(f"[SCHEDULER] {task['task_id']} -> {chosen['id']} ({chosen['ip']})")
        except OSError:
            with worker_lock:
                chosen["busy"] = False
            with queue_lock:
                queue.appendleft(task)
            time.sleep(0.1)


def handle_worker(conn, addr):
    worker_ip = addr[0]
    worker_id = None
    file_obj = conn.makefile("r", encoding="utf-8")
    try:
        first = file_obj.readline()
        if not first:
            return
        registration = json.loads(first)
        if registration.get("type") != "REGISTER" or not registration.get("worker_id"):
            send_json(conn, {"type": "ERROR", "message": "REGISTER required"})
            return

        worker_id = registration["worker_id"]
        with worker_lock:
            old = workers.get(worker_id)
            if old:
                try:
                    old["conn"].close()
                except OSError:
                    pass
            workers[worker_id] = {
                "id": worker_id, "ip": worker_ip, "conn": conn,
                "busy": False, "completed": 0
            }

        status = notify_controller("WORKER_UP", worker_ip)
        print(f"[BROKER] {worker_id} connected from {worker_ip}; SDN={status}")
        send_json(conn, {"type": "REGISTERED", "worker_id": worker_id})

        for line in file_obj:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue

            if message.get("type") == "RESULT":
                task_id = message.get("task_id")
                with worker_lock:
                    if worker_id in workers:
                        workers[worker_id]["busy"] = False
                        workers[worker_id]["completed"] += 1
                with pending_lock:
                    client = pending_clients.pop(task_id, None)
                if client:
                    try:
                        send_json(client, {
                            "status": "SUCCESS",
                            "task_id": task_id,
                            "worker": worker_id,
                            "result": message.get("result")
                        })
                        client.close()
                    except OSError:
                        pass
                print(f"[BROKER] Result received for {task_id} from {worker_id}")
    except (OSError, ValueError, KeyError) as exc:
        print(f"[BROKER] Worker handler error ({worker_id or worker_ip}): {exc}")
    finally:
        with worker_lock:
            current = workers.get(worker_id) if worker_id else None
            if current and current["conn"] is conn:
                workers.pop(worker_id, None)
                notify = True
            else:
                notify = False
        if notify:
            notify_controller("WORKER_DOWN", worker_ip)
            print(f"[BROKER] {worker_id} disconnected; removed from SDN")
        try:
            file_obj.close()
        except OSError:
            pass
        conn.close()


def handle_client(conn, addr):
    file_obj = conn.makefile("r", encoding="utf-8")
    try:
        line = file_obj.readline()
        request = json.loads(line) if line else {}
        task_type = request.get("task")
        allowed = {"sum", "uppercase", "reverse"}
        if task_type not in allowed:
            send_json(conn, {"status": "ERROR", "message": f"Supported tasks: {sorted(allowed)}"})
            return

        task_id = str(uuid.uuid4())[:8]
        task = {
            "task_id": task_id,
            "task": task_type,
            "payload": request.get("payload")
        }
        with pending_lock:
            pending_clients[task_id] = conn
        with queue_lock:
            queue.append(task)
        print(f"[BROKER] Queued task {task_id}: {task_type}")
        # The scheduler/worker thread owns the response and closes the socket.
        conn = None
    except (OSError, ValueError) as exc:
        print(f"[BROKER] Client request error: {exc}")
    finally:
        try:
            file_obj.close()
        except OSError:
            pass
        if conn:
            conn.close()


def start_listener(port, handler):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen(20)
    print(f"[BROKER] Listening on TCP/{port}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handler, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    threading.Thread(target=schedule_tasks, daemon=True).start()
    threading.Thread(target=start_listener, args=(WORKER_PORT, handle_worker), daemon=True).start()
    start_listener(CLIENT_PORT, handle_client)

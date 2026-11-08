import socket
import threading
import json
import time

HOST = '0.0.0.0'
PORT = 5000

active_workers = {}

def handle_worker(conn, addr):
    print(f"\n[+] Connected to worker at {addr}")
    worker_id = None
    try:
        # 1. Receive registration
        data = conn.recv(1024)
        if not data:
            return
        message = json.loads(data.decode('utf-8'))
        if message.get("type") == "REGISTER":
            worker_id = message.get("worker_id")
            active_workers[worker_id] = conn
            print(f"[REGISTRATION] Worker registered: {worker_id}")
            
            # Send welcome response
            response = {"status": "REGISTERED", "message": "Welcome to the broker"}
            conn.sendall((json.dumps(response) + "\n").encode('utf-8'))
        
        # 2. Automatically send a task after 3 seconds
        time.sleep(3)
        if worker_id in active_workers:
            task_msg = {"type": "TASK", "task_id": "task_101", "payload": "Process Data Batch A"}
            conn.sendall((json.dumps(task_msg) + "\n").encode('utf-8'))
            print(f"[*] Automatically sent task_101 to {worker_id}")

        # 3. Listen for results
        while True:
            data = conn.recv(1024)
            if not data:
                break
            res_message = json.loads(data.decode('utf-8'))
            if res_message.get("type") == "RESULT":
                print(f"\n[RESULT] Task {res_message.get('task_id')} completed by {worker_id}: {res_message.get('result')}")
                
    except Exception as e:
        print(f"[-] Error with worker {worker_id}: {e}")
    finally:
        if worker_id and worker_id in active_workers:
            del active_workers[worker_id]
        conn.close()
        print(f"[-] Connection closed for {worker_id}")

def start_broker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[*] Broker listening on {HOST}:{PORT}")
    
    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\n[*] Stopping Broker...")
    finally:
        server.close()

if __name__ == "__main__":
    start_broker()

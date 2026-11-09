import socket
import json
import time
import sys

BROKER_IP = sys.argv[2] if len(sys.argv) > 2 else '10.0.0.10'
PORT = 5000
WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else "worker_1"

def start_worker():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        print(f"[*] Attempting to connect to broker at {BROKER_IP}:{PORT}...")
        client.connect((BROKER_IP, PORT))
        print(f"[*] Connected to broker!")
        
        # Register
        reg_msg = {"type": "REGISTER", "worker_id": WORKER_ID}
        client.sendall((json.dumps(reg_msg) + "\n").encode('utf-8'))
        
        data = client.recv(1024)
        response = json.loads(data.decode('utf-8'))
        print(f"[Server Response]: {response}")
        
        # Listen for tasks from broker
        while True:
            data = client.recv(1024)
            if not data:
                break
            message = json.loads(data.decode('utf-8'))
            if message.get("type") == "TASK":
                task_id = message.get("task_id")
                payload = message.get("payload")
                print(f"\n[RECEIVED TASK] {task_id} with payload: '{payload}'. Processing...")
                
                # Simulate work execution time
                time.sleep(2)
                
                # Send result back
                result_msg = {"type": "RESULT", "task_id": task_id, "result": "SUCCESS - Processed payload"}
                client.sendall((json.dumps(result_msg) + "\n").encode('utf-8'))
                print(f"[SENT RESULT] Task {task_id} completed and reported to broker.")
                
    except ConnectionRefusedError:
        print("[-] Connection failed. Is the broker running?")
    except KeyboardInterrupt:
        print("\n[*] Disconnecting worker...")
    finally:
        client.close()

if __name__ == "__main__":
    start_worker()

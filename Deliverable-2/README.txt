DISTRIBUTED TASK NOTIFICATION AND WORKER SYSTEM
================================================

Files
-----
topology.py   Mininet topology (broker, 3 workers, client, one OVS switch)
controller.py OS-Ken SDN controller (OpenFlow 1.3)
broker.py     TCP broker, task queue, worker registry and scheduler
worker.py     Persistent TCP worker process
client.py     Task submission client

Architecture
------------
1. h1 (10.0.0.1) runs broker.py.
2. h2-h4 (10.0.0.2-4) run worker.py with unique worker IDs.
3. h5 (10.0.0.5) runs client.py to submit tasks.
4. Workers establish persistent TCP connections to the broker on port 5000.
5. Clients submit tasks to the broker on port 6000.
6. The broker queues tasks and assigns them to idle workers.
7. Workers return results over their existing TCP connections.
8. The broker notifies the SDN controller when a worker registers/disconnects.
9. The controller dynamically permits registered worker IPs to reach broker TCP/5000
   and removes their matching OpenFlow entries when they disconnect.

Supported demo tasks
-------------------
sum       payload is a list of numbers
uppercase payload is a string
reverse   payload is a string

Prerequisites (Ubuntu)
----------------------
sudo apt update
sudo apt install mininet openvswitch-switch python3-pip
sudo pip3 install os-ken

Run
---
Terminal 1: start the SDN controller
  cd <project-folder>
  os-ken-manager controller.py

Terminal 2: start Mininet (requires root)
  sudo python3 topology.py

At the Mininet prompt, start the broker:
  h1 python3 broker.py

In separate Mininet prompts/terminals, start workers:
  h2 python3 worker.py --id worker1
  h3 python3 worker.py --id worker2
  h4 python3 worker.py --id worker3

Submit tasks from h5:
  h5 python3 client.py sum 10 20 30
  h5 python3 client.py uppercase "hello mininet"
  h5 python3 client.py reverse "distributed systems"

Testing / demo scenarios
------------------------
A. Normal operation:
   Start all workers and submit several tasks. Show round-robin-like
   least-completed assignment in broker output and returned results.

B. Dynamic worker failure:
   Stop one worker with Ctrl+C. The broker should remove it and send
   WORKER_DOWN to the controller. Submit more tasks; remaining idle workers
   continue processing.

C. No-worker scenario:
   Stop all workers, submit a task, and observe it remain queued. Restart
   a worker and observe the queued task get dispatched.

D. SDN admission:
   The controller logs WORKER_UP/WORKER_DOWN and allows registered worker
   connections to TCP/5000. Unregistered sources attempting that destination
   are dropped by the controller.

Performance evaluation ideas for D2
-----------------------------------
Record task completion time for (for example) 1, 2 and 3 workers.
Run the same batch of tasks for each configuration and compare average
completion time and throughput (completed tasks / total time).
Keep task types and payload sizes constant across trials.

Important notes
---------------
- Start the controller before the Mininet topology.
- The broker must be started before workers and clients.
- This is a clear educational prototype, not a production-grade distributed
  scheduler: it does not persist tasks across broker crashes or authenticate
  worker identities cryptographically.
- The code uses OS-Ken imports. If your lab setup uses Ryu instead, the
  controller imports/launch command may need adaptation.

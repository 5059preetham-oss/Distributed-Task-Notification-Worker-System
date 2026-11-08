# Distributed Task Notification and Worker System - Deliverable 1

## Overview
This project implements a distributed task notification and worker system using low-level TCP socket programming integrated within a custom Mininet network topology. The system consists of a central broker, multiple worker nodes, and a custom network topology running over an Open vSwitch switch.

## System Architecture
* **`broker.py`**: Acts as the central server managing worker registrations, connection tracking, and task dispatching.
* **`worker.py`**: Acts as client worker nodes that connect to the broker, register themselves, receive tasks, process payloads, and report results back.
* **`topo.py`**: Custom Mininet topology script defining the broker (`10.0.0.10`), workers (`10.0.0.1`, `10.0.0.2`), and switch (`s1`).

## Protocol
Communication is performed via a JSON-over-TCP protocol delimited by newline characters (`\n`). Core message types include:
1. `REGISTER`: Worker registration message sent to the broker upon connection.
2. `TASK`: Task assignment dispatched from the broker to a worker.
3. `RESULT`: Completed work report sent back from the worker to the broker.

## Reproduction & Test Instructions
### Prerequisites
* Ubuntu VM with Mininet and Open vSwitch installed.
* Python 3.

### Running the System
1. Clean any existing Mininet states:
   sudo mn -c
2. Run the custom Mininet topology script:
   sudo python3 topo.py
3. Inside the Mininet CLI prompt (mininet>), start the broker in the background:
   broker python3 broker.py &
4. w1 python3 worker.py worker_1 10.0.0.10 &
   w2 python3 worker.py worker_2 10.0.0.10 
5. (cntrl+c) and type "exit"

## Test Cases & Expected Output
1. Worker Connection: [*] Connected to broker! followed by [Server Response]: {'status': 'REGISTERED', ...}
2. Task Processing: [RECEIVED TASK] ... Processing...
3. Result Reporting: [SENT RESULT] Task ... completed and reported to broker.

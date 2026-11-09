# Distributed Task Notification and Worker System

This is the final implementation for Topic 5. A TCP broker registers multiple
workers, assigns queued tasks according to current workload, receives results,
detects worker/task failures, and reassigns unfinished work. The broker also
publishes worker health and load to a Ryu OpenFlow controller over TCP. The
controller installs TCP/5000 forwarding rules for healthy workers and removes
rules for failed workers.

## Architecture and protocol

```text
worker 1 ─┐                         ┌─ Ryu/OpenFlow controller
worker 2 ─┼─ TCP/5000 ─ broker ─────┤   TCP/5001 metrics
           └──── Mininet switch ────┘
```

Messages are compact JSON objects separated by `\n`, so fragmented and
coalesced TCP reads are handled safely.

| Message | Direction | Purpose |
|---|---|---|
| `REGISTER` | worker -> broker | register a unique worker ID |
| `REGISTERED` | broker -> worker | registration acknowledgement |
| `TASK` | broker -> worker | assign a task and attempt number |
| `RESULT` | worker -> broker | return the completed result |
| `HEARTBEAT` | worker -> broker | keep the worker lease alive |
| `WORKER_METRICS` | broker -> controller | publish load and source IP |

## Requirements

The demo requires an Ubuntu VM with Python 3, Mininet, Open vSwitch, Ryu,
and OpenFlow 1.3 support. Run `sudo mn -c` before repeating a demo.

## Reproducible SDN demo

From this directory:

1. Start the controller in one terminal:

   ```bash
   ryu-manager --ofp-tcp-listen-port 6653 controller.py
   ```

2. Start the topology in another terminal:

   ```bash
   sudo python3 topo.py
   ```

3. In the Mininet CLI, start the broker with four demonstration tasks:

   ```text
   mininet> broker python3 broker.py --sdn-host 10.0.0.254 --demo-tasks 4 &
   ```

   If the controller runs on the VM host rather than inside Mininet, use the
   host-reachable controller address in `--sdn-host`. Metrics are sent using
   newline-delimited JSON over TCP/5001.

4. Start two workers:

   ```text
   mininet> w1 python3 worker.py worker-1 10.0.0.10 &
   mininet> w2 python3 worker.py worker-2 10.0.0.10 &
   ```

5. Observe `[DISPATCH]`, `[RESULT]`, and controller flow updates. Use
   `w1 pkill -f worker.py` to demonstrate failure recovery; the broker removes
   the stale worker and requeues its task to the remaining worker.

The application can also be tested without Mininet:

```bash
python3 broker.py --demo-tasks 4
python3 worker.py worker-1 127.0.0.1
python3 worker.py worker-2 127.0.0.1
```

## Testing and evaluation

Run the unit tests from this directory:

```bash
python3 -m unittest -v test_protocol.py
python3 -m py_compile protocol.py broker.py worker.py topo.py controller.py
```

For the performance comparison, repeat the demo with 1 and 2 workers and
record:

* task completion time: broker queue time to the final `[RESULT]`;
* throughput: completed tasks divided by elapsed seconds;
* load balance: tasks completed by each worker;
* recovery time: worker termination to `[DISPATCH]` on the replacement worker;
* failure rate: tasks in `[FAILED]` divided by submitted tasks.

Use the original Deliverable-1 implementation as the static single-worker
baseline. The final implementation should improve throughput with two workers
and preserve completion through a worker failure, while the SDN controller
removes forwarding flows for the failed worker.

## Rubric coverage

* **D1:** architecture, newline-delimited TCP socket implementation, concurrent
  workers, Mininet topology, Ryu/OpenFlow integration, and initial tests.
* **D2 complete functionality:** queueing, least-loaded scheduling, result
  acknowledgements, heartbeats, timeouts, duplicate-ID rejection, and bounded
  input validation.
* **D2 dynamic SDN:** broker metrics cause controller flow installation/removal.
* **D2 robustness:** worker disconnect and task timeout requeue unfinished tasks.
* **D2 performance:** repeatable one-worker/two-worker/failure experiments and
  measurable metrics are specified above.

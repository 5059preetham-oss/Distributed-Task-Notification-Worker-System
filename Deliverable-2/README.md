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

   The topology creates a root-namespace `controller-host` at `10.0.0.254`,
   so metrics are sent using newline-delimited JSON over TCP/5001:

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

For a reproducible local performance comparison, run:

```bash
python3 benchmark.py --tasks 12
```

The benchmark compares the original static one-worker baseline, dynamic
two-worker scheduling, and dynamic scheduling with one worker failing before
its first result. It records:

* task completion time: broker queue time to the final `[RESULT]`;
* throughput: completed tasks divided by elapsed seconds;
* load balance: tasks completed by each worker;
* recovery time: worker termination to `[DISPATCH]` on the replacement worker;
* failure rate: tasks in `[FAILED]` divided by submitted tasks.

Example local loopback run (`python3 benchmark.py --tasks 12`, Windows host,
November 2026):

| Scenario | Workers | Completed | Elapsed (s) | Throughput (tasks/s) | Recovery (s) |
|---|---:|---:|---:|---:|---:|
| Static baseline | 1 | 12 | 0.732 | 16.38 | - |
| Dynamic load balancing | 2 | 12 | 0.489 | 24.55 | - |
| Dynamic + worker failure | 2 | 12 | 0.760 | 15.79 | 0.760 |

These are local loopback measurements, not Mininet measurements. Run the
benchmark in the target Ubuntu VM and include that output in the final report
for environment-specific results.

Use the first row as the static single-worker baseline. The final
implementation should improve throughput with two workers and preserve
completion through a worker failure, while the SDN controller preserves
forwarding for recovery and blocks new TCP connection attempts from workers
whose reported load reaches `MAX_LOAD`.

## Rubric coverage

* **D1:** architecture, newline-delimited TCP socket implementation, concurrent
  workers, Mininet topology, Ryu/OpenFlow integration, and initial tests.
* **D2 complete functionality:** queueing, least-loaded scheduling, result
  acknowledgements, heartbeats, timeouts, duplicate-ID rejection, and bounded
  input validation.
* **D2 dynamic SDN:** broker metrics control higher-priority overload drops.
  Bootstrap forwarding remains installed for registration and failed-worker
  recovery.
  Bootstrap flows cover broker/worker traffic before registration; workload
  metrics install a higher-priority TCP SYN drop for overloaded workers.
* **D2 robustness:** worker disconnect and task timeout requeue unfinished tasks.
* **D2 performance:** `benchmark.py` performs repeatable one-worker,
  two-worker, and failure experiments and prints throughput and recovery data.

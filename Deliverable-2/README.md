# Distributed Task Notification and Worker System

This deliverable implements a closed-loop distributed task notification system.
The broker limits each worker to one in-flight task, requeues timed-out work,
ignores late/duplicate results, reconnects workers, and exchanges policies with
the Ryu controller over one persistent metrics connection.

## Architecture and protocol

```text
workers ──┐                         ┌─ Ryu/OpenFlow controller
          ├─ TCP/5000 ─ broker ─────┤   TCP/5001 metrics + policy
Mininet ──┘                         └─ OpenFlow 1.3 switch
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
| `WORKER_METRICS` | broker -> controller | publish worker IP, load, RTT EWMA/samples, and completed count |
| `WORKER_POLICY` | controller -> broker | HEALTHY/OVERLOADED/FAILED scheduling state |
| `SUBMIT` | client -> broker:5002 | submit a payload |
| `ACCEPTED` / `TASK_DONE` / `TASK_FAILED` | broker -> client | task notification lifecycle |

## Requirements

The demo requires an Ubuntu VM with Python 3, Mininet, Open vSwitch, Ryu,
and OpenFlow 1.3 support. Run `sudo mn -c` before repeating a demo.

```text
HEALTHY ──RTT/traffic threshold──> OVERLOADED
   ▲                                      │
   └────────────── recovered ─────────────┘
   │
   └── heartbeat timeout / missing metrics ──> FAILED
FAILED ──worker reconnect + metrics──> HEALTHY
```

The controller polls flow statistics every two seconds and classifies workers
from RTT EWMA, link status, and broker metrics. It installs a four-second
higher-priority quarantine for overloaded workers, marks failed workers without
deleting bootstrap forwarding, restores forwarding on recovery, and sends
`WORKER_POLICY` back to the broker. Worker ports are deterministic: broker is
port 1, workers are ports 2-9, and the controller host is port 10.

## Reproducible SDN demo

From this directory:

1. Start the controller in one terminal:

   ```bash
   ryu-manager --ofp-tcp-listen-port 6653 controller.py
   ```

2. Start the topology in another terminal:

   ```bash
   sudo python3 topo.py --workers 2
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
   mininet> c1 python3 client.py 10.0.0.10 "hello from client" &
   ```

5. Observe `[DISPATCH]`, `[RESULT]`, `[POLICY]`, and controller flow updates:

   ```text
   mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
   mininet> w1 pkill -f worker.py
   mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
   mininet> link s1 w1 down
   mininet> link s1 w1 up
   ```

   The worker reconnect loop and broker requeue logic should restore service.

The application can also be tested without Mininet:

```bash
python3 broker.py --demo-tasks 4
python3 worker.py worker-1 127.0.0.1
python3 worker.py worker-2 127.0.0.1
python3 client.py 127.0.0.1 "hello from client"
```

`TASK_SDN_MODE=baseline` installs only static forwarding. The default
`closed_loop` mode enables RTT/traffic classification, policy feedback,
quarantine, stale-metrics expiry, and port-status reactions. Broker options
`--scheduling`, `--max-inflight`, and `--client-port` control scheduling,
capacity, and the task client listener; worker `--slowdown` creates
heterogeneous processing speeds.

## Testing and evaluation

Run the unit tests from this directory:

```bash
python3 -m unittest -v test_protocol.py test_broker.py test_policy.py test_integration.py
python3 -m py_compile protocol.py broker.py worker.py topo.py controller.py
```

For a reproducible local performance comparison, run:

```bash
python3 benchmark.py --workers 2 4 8 --tasks 20 50 100 --repeats 3
```

The benchmark writes `results/benchmark-results.csv` and
`results/benchmark-results.png`. It
compares round-robin with blind pre-assignment against least-loaded scheduling
and records:

* task completion time: broker queue time to the final `[RESULT]`;
* throughput: completed tasks divided by elapsed seconds;
* load balance: completed tasks per worker;
* recovery time: first retried task completion after a worker fault, measured from the fault injection;
* failed-task rate: failed tasks divided by submitted tasks.

Run the benchmark inside Mininet for report numbers; loopback output is only a
smoke test and must not be presented as the Mininet result:

```bash
sudo python3 mininet_benchmark.py
```

The Mininet benchmark writes separate broker, worker, and controller logs plus
a CSV and plot under `results/`. It evaluates `baseline_rr`,
`baseline_least`, and `closed_loop` across normal, slow-worker, and link-failure
scenarios. Slow-worker delay is applied before tasks are dispatched; link failure
is injected after registration and held for two seconds. Use `--repeats 1 --tasks 20` for a quick run. `TASK_SDN_MODE` can
also be set directly to `baseline` or `closed_loop`; broker supports
`--scheduling round_robin|least_loaded`, `--max-inflight`, and workers support
`--slowdown`.

The policy thresholds are three RTT samples and at least two qualified workers
to avoid one-sample decisions; overload is `max(1.5 * best, best + 0.3s)`;
hold-down is 4 seconds to prevent oscillation; broker policy expiry is 15
seconds to recover if the controller disappears. The 4-second quarantine
isolates overloaded worker-to-broker TCP traffic and self-expires, while
bootstrap forwarding remains installed for reconnect.

## OpenFlow flow table

| Priority | Match | Action | Purpose |
|---:|---|---|---|
| 250 | worker IP -> broker TCP/5000 | drop, hard timeout 4s | temporary overload quarantine |
| 200 | IPv4 TCP dst broker/5000 | output 1 | worker/task ingress |
| 200 | broker -> worker TCP/5000 | worker port | task dispatch and return traffic |
| 200 | IPv4 TCP dst controller/5001 | output 10 | broker metrics ingress |
| 200 | controller -> broker TCP/5001 | output 1 | policy return path |
| 200 | client/broker TCP/5002 | ports 1/11 | task client path |
| 100 | ARP | flood | address resolution |
| 100 | ICMP IPv4 destination | host port | ping and diagnostics |
| 0 | all other traffic | drop | explicit table-miss behavior |

## Rubric coverage

* **D2 complete functionality:** serial workers, bounded scheduling, result
  acknowledgements, heartbeats, reconnects, timeouts, duplicate-ID rejection,
  late-result handling, and bounded input validation.
* **D2 dynamic SDN:** two-second flow polling, RTT/link-state classification,
  controller-to-broker policy feedback, expiring overload quarantine,
  failed-worker policy, and deterministic port mapping.
* **D2 robustness:** worker disconnect, link failure, and task timeout requeue
  unfinished tasks.
* **D2 performance:** `benchmark.py` runs 2/4/8-worker and 20/50/100-task
  configurations with three repeats, CSV output, and error-bar plots.

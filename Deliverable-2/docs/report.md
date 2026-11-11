# Deliverable 2 report

## Objectives

The system accepts task submissions, schedules work over TCP workers, reports
completion notifications to clients, and adapts forwarding and scheduling to
worker RTT and link state.

## Architecture

`client.py` submits `SUBMIT payload` commands to broker TCP/5002. The broker
queues the task, dispatches it over TCP/5000, and returns `TASK_DONE` or
`TASK_FAILED`. TCP/5001 carries broker metrics to the Ryu controller and
controller policies back to the broker.

## Algorithms and policy

Each worker has one in-flight task by default. The broker measures task
round-trip time using an EWMA. The controller requires at least two workers
with three RTT samples before RTT comparison; overload is RTT greater than
`max(1.5 * best, best + 0.3s)`. A four-second hold-down prevents oscillation,
and broker policies expire after 15 seconds. A four-second OpenFlow quarantine
is used for overload because it is supported by OpenFlow/OVS without requiring
meter configuration; it is intentionally an isolation rule for new worker
connections, while the broker requeues work if a worker becomes unavailable.

## Reproduction

```bash
sudo mn -c
ryu-manager --ofp-tcp-listen-port 6653 controller.py
sudo python3 topo.py --workers 4
mininet> broker python3 broker.py --sdn-host 10.0.0.254 --demo-tasks 20 &
mininet> c1 python3 client.py 10.0.0.10 "hello" &
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
```

Run unit tests from this directory with
`python3 -m unittest -v test_protocol.py test_broker.py test_policy.py test_mininet_benchmark.py test_integration.py`.
Use `sudo python3 mininet_benchmark.py --repeats 1 --tasks 20` for a quick
run, or omit those options for the full 2/4/8-worker and 20/50/100-task sweep.
Raw logs, CSV output, and plots are written under `results/`.

## Test evidence and limitations

The tests cover framing, duplicate results and submissions, dispatch caps,
round-robin, timeouts, policy expiry, state classification, and failure requeue.
A clean Ubuntu VM run must record exact Ubuntu, Python, Mininet, OVS, and Ryu versions
in the final results directory. This Windows development environment cannot
execute Ryu, Open vSwitch, or Mininet; VM-specific flow dumps and timings must
therefore be collected before submission.

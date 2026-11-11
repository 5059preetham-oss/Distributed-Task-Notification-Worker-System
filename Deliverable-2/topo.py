"""Mininet topology for the broker and two TCP workers."""

import argparse

from mininet.cli import CLI
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController


def run_topology(worker_count: int = 2) -> None:
    net = Mininet(
        controller=RemoteController,
        switch=OVSKernelSwitch,
        autoSetMacs=True,
        autoStaticArp=True,
    )
    controller = net.addController(
        "c0", controller=RemoteController, ip="127.0.0.1", port=6653
    )
    switch = net.addSwitch("s1", protocols="OpenFlow13", failMode="secure")
    broker = net.addHost("broker", ip="10.0.0.10/24")
    controller_host = net.addHost(
        "controller-host", ip="10.0.0.254/24", inNamespace=False
    )
    client = net.addHost("c1", ip="10.0.0.20/24")
    net.addLink(broker, switch, port2=1)
    for index in range(worker_count):
        worker = net.addHost(f"w{index + 1}", ip=f"10.0.0.{index + 1}/24")
        net.addLink(worker, switch, port2=index + 2)
    net.addLink(controller_host, switch, port2=10)
    net.addLink(client, switch, port2=11)

    info("*** Starting broker/worker SDN topology\n")
    net.build()
    controller.start()
    switch.start([controller])
    info("*** Hosts: broker=10.0.0.10, workers=10.0.0.1-10.0.0.8, client=10.0.0.20\n")
    info("*** TCP ports: worker=5000, metrics=5001, client=5002\n")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    setLogLevel("info")
    run_topology(max(2, min(8, args.workers)))

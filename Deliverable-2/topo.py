"""Mininet topology for the broker and two TCP workers."""

from mininet.cli import CLI
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController


def run_topology() -> None:
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
    worker1 = net.addHost("w1", ip="10.0.0.1/24")
    worker2 = net.addHost("w2", ip="10.0.0.2/24")
    controller_host = net.addHost(
        "controller-host", ip="10.0.0.254/24", inNamespace=False
    )
    net.addLink(broker, switch)
    net.addLink(worker1, switch)
    net.addLink(worker2, switch)
    net.addLink(controller_host, switch)

    info("*** Starting broker/worker SDN topology\n")
    net.build()
    controller.start()
    switch.start([controller])
    info("*** Hosts: broker=10.0.0.10, w1=10.0.0.1, w2=10.0.0.2\n")
    info("*** TCP application port: 5000; TCP SDN metrics port: 5001\n")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    run_topology()

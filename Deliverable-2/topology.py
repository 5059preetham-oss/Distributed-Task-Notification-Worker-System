"""
Mininet topology:
h1 = broker, h2-h4 = workers, h5 = task client.
The root namespace interface is used by the broker to notify the SDN controller.
"""
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node
from mininet.cli import CLI
from mininet.log import setLogLevel


def main():
    net = Mininet(controller=None, switch=OVSSwitch, autoSetMacs=True)

    net.addController(
        "c0",
        controller=RemoteController,
        ip="127.0.0.1",
        port=6653
    )

    broker = net.addHost("h1", ip="10.0.0.1/24")
    worker1 = net.addHost("h2", ip="10.0.0.2/24")
    worker2 = net.addHost("h3", ip="10.0.0.3/24")
    worker3 = net.addHost("h4", ip="10.0.0.4/24")
    client = net.addHost("h5", ip="10.0.0.5/24")

    s1 = net.addSwitch("s1", protocols="OpenFlow13")
    for host in (broker, worker1, worker2, worker3, client):
        net.addLink(host, s1)

    # Control-channel interface in the root namespace.
    root = Node("root", inNamespace=False)
    intf = net.addLink(root, s1).intf1
    root.setIP("10.0.0.254", 24, intf=intf)

    net.start()
    print("\\nNetwork started:")
    print("h1 10.0.0.1  Broker (TCP 5000) and task scheduler")
    print("h2 10.0.0.2  Worker 1")
    print("h3 10.0.0.3  Worker 2")
    print("h4 10.0.0.4  Worker 3")
    print("h5 10.0.0.5  Task client (TCP 6000)")
    print("root 10.0.0.254 Controller control channel (TCP 9999)")
    print("\\nUse Mininet CLI commands such as:")
    print("  h1 python3 broker.py")
    print("  h2 python3 worker.py --id worker2")
    print("  h5 python3 client.py sum 10 20 30")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    main()

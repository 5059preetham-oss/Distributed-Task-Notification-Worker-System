from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def run_topology():
    # Create network without a controller, using standalone switch mode for native learning
    net = Mininet(switch=OVSKernelSwitch, autoSetMacs=True)

    info("*** Adding OpenFlow Switch (Standalone Mode)\n")
    s1 = net.addSwitch('s1', failMode='standalone')

    info("*** Adding Hosts (Broker and Workers)\n")
    broker = net.addHost('broker', ip='10.0.0.10')
    w1 = net.addHost('w1', ip='10.0.0.1')
    w2 = net.addHost('w2', ip='10.0.0.2')

    info("*** Creating Links\n")
    net.addLink(broker, s1)
    net.addLink(w1, s1)
    net.addLink(w2, s1)

    info("*** Starting Network\n")
    net.start()

    info("\n*** Network is live! Use the mininet prompt below:\n")
    CLI(net)

    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run_topology()

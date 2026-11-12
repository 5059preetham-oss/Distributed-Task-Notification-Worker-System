"""
OS-Ken/OpenFlow 1.3 controller.

The broker sends WORKER_UP <ip> and WORKER_DOWN <ip> messages over TCP/9999.
Only registered workers may initiate connections to the broker's TCP/5000 port.
"""
import socket
import threading

from os_ken.base import app_manager
from os_ken.controller import ofp_event
from os_ken.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from os_ken.ofproto import ofproto_v1_3
from os_ken.lib.packet import packet, ethernet, ether_types, ipv4, tcp

CONTROL_PORT = 9999
BROKER_IP = "10.0.0.1"
BROKER_PORT = 5000
BROKER_CONTROL_IP = "10.0.0.1"


class TaskSDNController(app_manager.OSKenApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}
        self.mac_to_port = {}
        self.active_workers = set()
        self.pending_workers = set()  # temporary TCP registration handshakes
        threading.Thread(target=self.control_server, daemon=True).start()

    def control_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", CONTROL_PORT))
        server.listen(10)
        self.logger.info("Broker control channel listening on TCP/%s", CONTROL_PORT)

        while True:
            conn, addr = server.accept()
            with conn:
                if addr[0] != BROKER_CONTROL_IP:
                    self.logger.warning("Rejected controller command from %s", addr[0])
                    continue
                try:
                    message = conn.recv(256).decode().strip().split()
                except OSError:
                    continue

                if len(message) != 2 or message[0] not in ("WORKER_UP", "WORKER_DOWN"):
                    conn.sendall(b"ERROR\\n")
                    continue

                action, worker_ip = message
                if action == "WORKER_UP":
                    self.active_workers.add(worker_ip)
                    self.pending_workers.discard(worker_ip)
                    self.logger.info("Worker registered: %s", worker_ip)
                else:
                    self.active_workers.discard(worker_ip)
                    self.pending_workers.discard(worker_ip)
                    self.delete_worker_flows(worker_ip)
                    self.logger.info("Worker disconnected: %s", worker_ip)
                conn.sendall(b"OK\\n")

    def add_flow(self, datapath, priority, match, actions):
        parser = datapath.ofproto_parser
        instructions = [parser.OFPInstructionActions(
            datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        datapath.send_msg(parser.OFPFlowMod(
            datapath=datapath, priority=priority,
            match=match, instructions=instructions))

    def delete_worker_flows(self, worker_ip):
        for datapath in self.datapaths.values():
            parser = datapath.ofproto_parser
            ofp = datapath.ofproto
            match = parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_src=worker_ip,
                ipv4_dst=BROKER_IP,
                ip_proto=6,
                tcp_dst=BROKER_PORT)
            datapath.send_msg(parser.OFPFlowMod(
                datapath=datapath, command=ofp.OFPFC_DELETE,
                out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                match=match))

    def packet_out(self, datapath, msg, in_port, actions):
        ofp = datapath.ofproto
        data = msg.data if msg.buffer_id == ofp.OFP_NO_BUFFER else None
        datapath.send_msg(datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id,
            in_port=in_port, actions=actions, data=data))

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features(self, ev):
        dp = ev.msg.datapath
        self.datapaths[dp.id] = dp
        parser = dp.ofproto_parser
        actions = [parser.OFPActionOutput(
            dp.ofproto.OFPP_CONTROLLER, dp.ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, parser.OFPMatch(), actions)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None or eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        self.mac_to_port.setdefault(dp.id, {})
        self.mac_to_port[dp.id][eth.src] = in_port

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)

        # Enforce SDN admission control on worker -> broker registration sockets.
        if (ip_pkt and tcp_pkt and ip_pkt.dst == BROKER_IP
                and tcp_pkt.dst_port == BROKER_PORT):
            # A new worker must be able to complete the TCP connection and
            # send REGISTER before the broker can mark it active. Permit this
            # short bootstrap phase; all other unregistered sources are dropped.
            if ip_pkt.src not in self.active_workers:
                is_new_syn = bool(tcp_pkt.bits & 0x02)  # TCP SYN flag
                if is_new_syn:
                    self.pending_workers.add(ip_pkt.src)
                elif ip_pkt.src not in self.pending_workers:
                    self.logger.warning("DROP unregistered worker %s -> broker", ip_pkt.src)
                    return

            out_port = self.mac_to_port[dp.id].get(eth.dst, ofp.OFPP_FLOOD)
            actions = [parser.OFPActionOutput(out_port)]
            if out_port != ofp.OFPP_FLOOD:
                match = parser.OFPMatch(
                    in_port=in_port, eth_type=ether_types.ETH_TYPE_IP,
                    ipv4_src=ip_pkt.src, ipv4_dst=ip_pkt.dst,
                    ip_proto=6, tcp_dst=BROKER_PORT)
                self.add_flow(dp, 100, match, actions)
            self.packet_out(dp, msg, in_port, actions)
            self.logger.info("ALLOW registered worker %s", ip_pkt.src)
            return

        # Basic L2 learning/forwarding for ARP, client traffic and return packets.
        out_port = self.mac_to_port[dp.id].get(eth.dst, ofp.OFPP_FLOOD)
        self.packet_out(dp, msg, in_port, [parser.OFPActionOutput(out_port)])

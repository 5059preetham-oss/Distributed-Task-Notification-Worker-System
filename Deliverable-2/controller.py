"""Ryu controller that installs worker flows from broker health metrics."""

from __future__ import annotations

import json
import socket
import threading
from typing import Any

from protocol import read_messages

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3


class TaskController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    WORKER_PORTS = {"10.0.0.1": 2, "10.0.0.2": 3}
    TCP_PORT = 5000
    METRICS_PORT = 5001

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.datapaths: dict[int, Any] = {}
        self.listener = threading.Thread(target=self._listen_for_metrics, daemon=True)
        self.listener.start()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, CONFIG_DISPATCHER])
    def state_change_handler(self, ev: Any) -> None:
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[datapath.id] = datapath
        else:
            self.datapaths.pop(datapath.id, None)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev: Any) -> None:
        datapath = ev.msg.datapath
        parser = datapath.ofproto_parser
        self._add_flow(
            datapath,
            0,
            parser.OFPMatch(),
            [parser.OFPActionOutput(datapath.ofproto.OFPP_CONTROLLER)],
        )

    def _add_flow(self, datapath: Any, priority: int, match: Any, actions: list[Any]) -> None:
        parser = datapath.ofproto_parser
        instructions = [
            parser.OFPInstructionActions(datapath.ofproto.OFPIT_APPLY_ACTIONS, actions)
        ]
        datapath.send_msg(
            parser.OFPFlowMod(
                datapath=datapath,
                priority=priority,
                match=match,
                instructions=instructions,
            )
        )

    def _delete_worker_flow(self, datapath: Any, worker_ip: str) -> None:
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(
            eth_type=0x0800, ipv4_dst=worker_ip, ip_proto=6, tcp_dst=self.TCP_PORT
        )
        datapath.send_msg(
            parser.OFPFlowMod(
                datapath=datapath,
                command=datapath.ofproto.OFPFC_DELETE,
                out_port=datapath.ofproto.OFPP_ANY,
                out_group=datapath.ofproto.OFPG_ANY,
                match=match,
            )
        )

    def _install_worker_flow(self, datapath: Any, worker_ip: str) -> None:
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(
            eth_type=0x0800, ipv4_dst=worker_ip, ip_proto=6, tcp_dst=self.TCP_PORT
        )
        self._add_flow(
            datapath,
            100,
            match,
            [parser.OFPActionOutput(self.WORKER_PORTS[worker_ip])],
        )

    def _listen_for_metrics(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", self.METRICS_PORT))
            sock.listen(16)
            while True:
                connection, _ = sock.accept()
                threading.Thread(
                    target=self._handle_metrics_connection,
                    args=(connection,),
                    daemon=True,
                ).start()

    def _handle_metrics_connection(self, connection: socket.socket) -> None:
        buffer = b""
        try:
            with connection:
                while True:
                    data = connection.recv(4096)
                    if not data:
                        return
                    buffer, messages = read_messages(buffer, data)
                    for message in messages:
                        self._apply_metrics(message)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self.logger.warning("invalid broker metrics: %s", exc)

    def _apply_metrics(self, message: dict[str, Any]) -> None:
        if message.get("type") != "WORKER_METRICS":
            return
        active_ips = {
            item["ip"] for item in message.get("workers", []) if item.get("ip")
        }
        for datapath in list(self.datapaths.values()):
            for worker_ip in self.WORKER_PORTS:
                if worker_ip in active_ips:
                    self._install_worker_flow(datapath, worker_ip)
                else:
                    self._delete_worker_flow(datapath, worker_ip)

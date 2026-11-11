"""Ryu controller for closed-loop worker forwarding and health policy."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from typing import Any

from policy import classify
from protocol import encode_message, read_messages

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import (
    CONFIG_DISPATCHER,
    DEAD_DISPATCHER,
    MAIN_DISPATCHER,
    set_ev_cls,
)
from ryu.lib import hub
from ryu.ofproto import ofproto_v1_3


class TaskController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    BROKER_IP = "10.0.0.10"
    CONTROLLER_IP = "10.0.0.254"
    CONTROLLER_PORT = 10
    CLIENT_IP = "10.0.0.20"
    CLIENT_PORT = 11
    CLIENT_TCP_PORT = 5002
    TCP_PORT = 5000
    METRICS_PORT = 5001
    POLL_INTERVAL = 2.0
    WORKER_PORTS = {f"10.0.0.{i}": i + 1 for i in range(1, 9)}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.mode = os.getenv("TASK_SDN_MODE", "closed_loop")
        if self.mode not in {"baseline", "closed_loop"}:
            raise ValueError("TASK_SDN_MODE must be baseline or closed_loop")
        self.datapaths: dict[int, Any] = {}
        self.worker_ports = dict(self.WORKER_PORTS)
        self.port_to_ip = {port: ip for ip, port in self.worker_ports.items()}
        self.worker_ips: dict[str, str] = {}
        self.worker_states: dict[str, str] = {}
        self.worker_metrics: dict[str, dict[str, Any]] = {}
        self.last_metrics_at = 0.0
        self.flow_rates: dict[str, float] = {}
        self.link_down: set[str] = set()
        self.policy_state: dict[str, Any] = {}
        self.previous_flow_bytes: dict[tuple[int, str], tuple[int, float]] = {}
        self.metrics_connections: set[socket.socket] = set()
        self.state_lock = threading.RLock()
        self.listener = hub.spawn(self._listen_for_metrics)
        self.stats_poller = hub.spawn(self._poll_stats)

    @set_ev_cls(
        ofp_event.EventOFPStateChange,
        [MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER],
    )
    def state_change_handler(self, ev: Any) -> None:
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[ev.datapath.id] = ev.datapath
        else:
            self.datapaths.pop(ev.datapath.id, None)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev: Any) -> None:
        datapath = ev.msg.datapath
        parser = datapath.ofproto_parser
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_dst=self.BROKER_IP,
                ip_proto=6,
                tcp_dst=self.TCP_PORT,
            ),
            [parser.OFPActionOutput(1)],
        )
        for worker_ip, port in self.worker_ports.items():
            self._set_worker_forwarding(datapath, worker_ip, port)
        all_hosts = {**self.worker_ports, self.BROKER_IP: 1, self.CONTROLLER_IP: self.CONTROLLER_PORT}
        self._add_flow(
            datapath,
            100,
            parser.OFPMatch(eth_type=0x0806),
            [parser.OFPActionOutput(datapath.ofproto.OFPP_FLOOD)],
        )
        for ip, port in {**all_hosts, self.CLIENT_IP: self.CLIENT_PORT}.items():
            self._add_flow(
                datapath,
                100,
                parser.OFPMatch(eth_type=0x0800, ip_proto=1, ipv4_dst=ip),
                [parser.OFPActionOutput(port)],
            )
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_dst=self.CONTROLLER_IP,
                ip_proto=6,
                tcp_dst=self.METRICS_PORT,
            ),
            [parser.OFPActionOutput(self.CONTROLLER_PORT)],
        )
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=self.CONTROLLER_IP,
                ipv4_dst=self.BROKER_IP,
                ip_proto=6,
                tcp_src=self.METRICS_PORT,
            ),
            [parser.OFPActionOutput(1)],
        )
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_dst=self.BROKER_IP,
                ip_proto=6,
                tcp_dst=self.CLIENT_TCP_PORT,
            ),
            [parser.OFPActionOutput(1)],
        )
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=self.BROKER_IP,
                ipv4_dst=self.CLIENT_IP,
                ip_proto=6,
                tcp_src=self.CLIENT_TCP_PORT,
            ),
            [parser.OFPActionOutput(self.CLIENT_PORT)],
        )
        self._add_flow(
            datapath,
            0,
            parser.OFPMatch(),
            [],
        )

    def _add_flow(
        self,
        datapath: Any,
        priority: int,
        match: Any,
        actions: list[Any],
        hard_timeout: int = 0,
    ) -> None:
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
                hard_timeout=hard_timeout,
            )
        )

    def _set_worker_forwarding(self, datapath: Any, worker_ip: str, port: int) -> None:
        parser = datapath.ofproto_parser
        self._add_flow(
            datapath,
            200,
            parser.OFPMatch(
                eth_type=0x0800,
                ipv4_src=self.BROKER_IP,
                ipv4_dst=worker_ip,
                ip_proto=6,
                tcp_src=self.TCP_PORT,
            ),
            [parser.OFPActionOutput(port)],
        )

    def _set_overload_rule(self, datapath: Any, worker_ip: str, overloaded: bool) -> None:
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(
            eth_type=0x0800,
            ipv4_src=worker_ip,
            ipv4_dst=self.BROKER_IP,
            ip_proto=6,
            tcp_dst=self.TCP_PORT,
        )
        if overloaded:
            self._add_flow(datapath, 250, match, [], hard_timeout=4)
        else:
            datapath.send_msg(
                parser.OFPFlowMod(
                    datapath=datapath,
                    command=datapath.ofproto.OFPFC_DELETE_STRICT,
                    out_port=datapath.ofproto.OFPP_ANY,
                    out_group=datapath.ofproto.OFPG_ANY,
                    match=match,
                    priority=250,
                )
            )

    def _listen_for_metrics(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", self.METRICS_PORT))
            sock.listen(16)
            while True:
                connection, _ = sock.accept()
                with self.state_lock:
                    self.metrics_connections.add(connection)
                    policies = [
                        (worker_id, state)
                        for worker_id, state in self.worker_states.items()
                        if state != "HEALTHY"
                    ]
                for worker_id, state in policies:
                    self._send_policy(connection, worker_id, state)
                hub.spawn(self._handle_metrics_connection, connection)

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
        finally:
            with self.state_lock:
                self.metrics_connections.discard(connection)

    def _apply_metrics(self, message: dict[str, Any]) -> None:
        if message.get("type") != "WORKER_METRICS":
            return
        workers = {
            item["worker_id"]: item
            for item in message.get("workers", [])
            if item.get("worker_id") and item.get("ip")
        }
        with self.state_lock:
            self.worker_metrics = workers
            self.last_metrics_at = time.monotonic()
            self.worker_ips.update(
                {worker_id: str(item["ip"]) for worker_id, item in workers.items()}
            )
        self._reclassify()

    def _reclassify(self) -> None:
        if self.mode == "baseline":
            return
        with self.state_lock:
            states = classify(
                {
                    worker_id: {
                        **item,
                        "byte_rate_bps": self.flow_rates.get(
                            str(item["ip"]), 0.0
                        ),
                    }
                    for worker_id, item in self.worker_metrics.items()
                },
                self.link_down,
                time.monotonic(),
                self.policy_state,
            )
            changes = [
                (worker_id, state, self.worker_states.get(worker_id))
                for worker_id, state in states.items()
                if self.worker_states.get(worker_id) != state
            ]
            self.worker_states = states
        for worker_id, state, _ in changes:
            worker_ip = self.worker_ips.get(worker_id)
            if not worker_ip:
                continue
            port = self.worker_ports.get(worker_ip)
            if port is None:
                continue
            for datapath in list(self.datapaths.values()):
                self._set_overload_rule(
                    datapath, worker_ip, state == "OVERLOADED"
                )
                self._set_worker_forwarding(
                    datapath, worker_ip, port
                )
            with self.state_lock:
                connections = list(self.metrics_connections)
            for connection in connections:
                self._send_policy(connection, worker_id, state)
            self.logger.info(
                "[STATE] worker=%s %s->%s reason=metrics-or-link",
                worker_id, _ or "UNKNOWN", state,
            )
            print(
                f"[{time.time():.3f}] [STATE] worker={worker_id} "
                f"{_ or 'UNKNOWN'}->{state} reason=metrics-or-link",
                flush=True,
            )

    def _send_policy(self, connection: socket.socket, worker_id: str, state: str) -> None:
        try:
            connection.sendall(
                encode_message(
                    {
                        "type": "WORKER_POLICY",
                        "worker_id": worker_id,
                        "state": state,
                    }
                )
            )
        except OSError:
            pass

    def _poll_stats(self) -> None:
        while True:
            hub.sleep(self.POLL_INTERVAL)
            if self.mode == "baseline":
                continue
            with self.state_lock:
                stale = (
                    self.last_metrics_at
                    and time.monotonic() - self.last_metrics_at > 5
                )
                if stale:
                    self.worker_metrics = {}
            if stale:
                self._reclassify()
            for datapath in list(self.datapaths.values()):
                parser = datapath.ofproto_parser
                datapath.send_msg(parser.OFPFlowStatsRequest(datapath))

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev: Any) -> None:
        now = time.monotonic()
        with self.state_lock:
            for stat in ev.msg.body:
                worker_ip = stat.match.get("ipv4_dst")
                if not worker_ip or stat.match.get("tcp_src") != self.TCP_PORT:
                    continue
                key = (ev.msg.datapath.id, worker_ip)
                previous = self.previous_flow_bytes.get(key)
                self.previous_flow_bytes[key] = (stat.byte_count, now)
                if previous:
                    old_bytes, old_time = previous
                    elapsed = now - old_time
                    if elapsed > 0:
                        rate = max(
                            0.0, (stat.byte_count - old_bytes) * 8 / elapsed
                        )
                        self.flow_rates[worker_ip] = rate
                        print(
                            f"[{time.time():.3f}] [STATS] worker={worker_ip} "
                            f"bit_rate_bps={rate:.2f}",
                            flush=True,
                        )
        self._reclassify()

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev: Any) -> None:
        if self.mode == "baseline":
            return
        desc, ofp = ev.msg.desc, ev.msg.datapath.ofproto
        down = bool(desc.state & ofp.OFPPS_LINK_DOWN) or bool(
            desc.config & ofp.OFPPC_PORT_DOWN
        )
        ip = self.port_to_ip.get(desc.port_no)
        if ip:
            with self.state_lock:
                if down:
                    self.link_down.add(ip)
                else:
                    self.link_down.discard(ip)
            self._reclassify()

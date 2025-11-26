"""TCP server that receives messages from peer health checkers."""

import logging
import socket
import threading
import time
import typing
from typing import Callable

from health_checker.leader.election import BullyElection
from health_checker.registry import Registry
from shared.network import Network, NetworkError
from shared.protocol import (
    HCCoordinatorPacket,
    HCElectionPacket,
    HCHeartbeatPacket,
    HCOkPacket,
    Packet,
    PacketType,
)


class PeerServer:
    """TCP server that receives heartbeats and election messages from peers."""

    def __init__(
        self,
        port: int,
        peer_registry: Registry,
        shutdown_event: threading.Event,
    ):
        self._port = port
        self._peer_registry = peer_registry
        self._shutdown_event = shutdown_event

        self._election: BullyElection | None = None
        self._on_election_received: Callable | None = None

        self._server_socket: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._handler_threads: list[threading.Thread] = []
        self._threads_lock = threading.Lock()

    def set_election(self, election: BullyElection) -> None:
        """Set the election handler for dispatching election messages."""
        self._election = election

    def set_on_election_received(self, callback: Callable | None) -> None:
        """Set callback to invoke when ELECTION is received (to clear stale connections)."""
        self._on_election_received = callback

    def start(self) -> None:
        """Start the TCP server."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.settimeout(1.0)
        self._server_socket.bind(("0.0.0.0", self._port))
        self._server_socket.listen(10)

        self._server_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._server_thread.start()
        logging.info(f"action: peer_server_start | result: success | port: {self._port}")

    def stop(self) -> None:
        """Stop the server and clean up resources."""
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
            self._server_socket = None

        if self._server_thread and self._server_thread.is_alive():
            self._server_thread.join(timeout=2.0)

        with self._threads_lock:
            threads_to_join = list(self._handler_threads)
        for thread in threads_to_join:
            thread.join(timeout=1.0)

        logging.info("action: peer_server_stop | result: success")

    def _accept_loop(self) -> None:
        """Accept incoming connections and spawn handlers."""
        while not self._shutdown_event.is_set():
            try:
                conn, _ = self._server_socket.accept()
                handler = threading.Thread(target=self._handle_connection, args=(conn,), daemon=True)
                with self._threads_lock:
                    self._handler_threads = [t for t in self._handler_threads if t.is_alive()]
                    self._handler_threads.append(handler)
                handler.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn: socket.socket) -> None:
        """Handle a single connection, receiving messages until disconnect."""
        conn.settimeout(5.0)
        network = Network(conn)
        try:
            while not self._shutdown_event.is_set():
                packet = network.recv_packet()
                if packet is None:
                    break
                self._dispatch_packet(packet)
        except (NetworkError, socket.timeout, Exception):
            pass
        finally:
            network.close()

    def _dispatch_packet(self, packet: Packet) -> None:
        """Dispatch received packet to appropriate handler based on packet type."""
        match packet.get_message_type():
            case PacketType.HC_HEARTBEAT:
                hb = typing.cast(HCHeartbeatPacket, packet)
                self._peer_registry.update(str(hb.hc_id), hb.timestamp)

            case PacketType.HC_ELECTION:
                el = typing.cast(HCElectionPacket, packet)
                if self._on_election_received:
                    self._on_election_received(el.hc_id)
                if self._election:
                    self._election.handle_election(el.hc_id)

            case PacketType.HC_OK:
                ok = typing.cast(HCOkPacket, packet)
                if self._election:
                    self._election.handle_ok(ok.hc_id)

            case PacketType.HC_COORDINATOR:
                coord = typing.cast(HCCoordinatorPacket, packet)
                self._peer_registry.update(str(coord.hc_id), time.time())
                if self._election:
                    self._election.handle_coordinator(coord.hc_id)

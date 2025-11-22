"""TCP client that sends messages to peer health checkers."""

import logging
import socket
import threading
import time

from shared.network import Network, NetworkError
from shared.protocol import (
    HCCoordinatorPacket,
    HCElectionPacket,
    HCHeartbeatPacket,
    HCOkPacket,
    Packet,
)


class PeerClient:
    """TCP client that sends heartbeats and election messages to peers."""

    def __init__(
        self,
        my_id: int,
        total_replicas: int,
        port: int,
        heartbeat_interval: float,
        shutdown_event: threading.Event,
    ):
        self._my_id = my_id
        self._total_replicas = total_replicas
        self._port = port
        self._heartbeat_interval = heartbeat_interval
        self._shutdown_event = shutdown_event

        self._connections: dict[int, Network] = {}
        self._connections_lock = threading.Lock()
        self._heartbeat_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the heartbeat sender thread."""
        if self._total_replicas <= 1:
            logging.info("action: peer_client_start | result: skipped | reason: single_replica")
            return

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        logging.info(f"action: peer_client_start | result: success | targets: {self._total_replicas - 1}")

    def stop(self) -> None:
        """Stop the client and clean up connections."""
        with self._connections_lock:
            for network in self._connections.values():
                network.close()
            self._connections.clear()

        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=self._heartbeat_interval + 1)

        logging.info("action: peer_client_stop | result: success")

    def clear_connection(self, peer_id: int) -> None:
        """Clear cached connection to a peer, forcing reconnect on next send."""
        with self._connections_lock:
            if peer_id in self._connections:
                try:
                    self._connections[peer_id].close()
                except Exception:
                    pass
                del self._connections[peer_id]

    def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats to all peers."""
        while not self._shutdown_event.wait(timeout=self._heartbeat_interval):
            self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        """Send heartbeat to all peers."""
        packet = HCHeartbeatPacket(self._my_id, time.time())
        for peer_id in range(self._total_replicas):
            if peer_id == self._my_id:
                continue
            self._send_to_peer(peer_id, packet)

    def _get_connection(self, peer_id: int) -> Network | None:
        """Get or create connection to a peer."""
        with self._connections_lock:
            network = self._connections.get(peer_id)
            if network:
                return network

        try:
            host = f"health_checker_{peer_id}"
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, self._port))
            network = Network(sock)
            with self._connections_lock:
                self._connections[peer_id] = network
            return network
        except (socket.error, Exception):
            return None

    def _send_to_peer(self, peer_id: int, packet: Packet) -> bool:
        """Send a packet to a specific peer. Returns True on success."""
        with self._connections_lock:
            network = self._connections.get(peer_id)

        if network:
            try:
                network.send_packet(packet)
                return True
            except NetworkError:
                with self._connections_lock:
                    if peer_id in self._connections:
                        self._connections[peer_id].close()
                        del self._connections[peer_id]

        network = self._get_connection(peer_id)
        if network:
            try:
                network.send_packet(packet)
                return True
            except NetworkError:
                with self._connections_lock:
                    if peer_id in self._connections:
                        self._connections[peer_id].close()
                        del self._connections[peer_id]

        return False

    def send_election(self, peer_id: int) -> None:
        """Send ELECTION message to a specific peer."""
        packet = HCElectionPacket(self._my_id)
        success = self._send_to_peer(peer_id, packet)
        if success:
            logging.debug(f"action: send_election | to: {peer_id}")

    def send_ok(self, peer_id: int) -> None:
        """Send OK message to a specific peer."""
        packet = HCOkPacket(self._my_id)
        success = self._send_to_peer(peer_id, packet)
        if success:
            logging.debug(f"action: send_ok | to: {peer_id}")

    def send_coordinator(self) -> None:
        """Broadcast COORDINATOR message to all peers."""
        packet = HCCoordinatorPacket(self._my_id)
        for peer_id in range(self._total_replicas):
            if peer_id == self._my_id:
                continue
            success = self._send_to_peer(peer_id, packet)
            if success:
                logging.debug(f"action: send_coordinator | to: {peer_id}")

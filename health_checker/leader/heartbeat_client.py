"""TCP client that sends heartbeats to other health checkers."""

import logging
import socket
import threading
import time

from shared.network import Network, NetworkError
from shared.protocol import HCHeartbeatPacket


class HeartbeatClient:
    """TCP client that sends heartbeats to peer health checkers."""

    def __init__(
        self,
        my_id: int,
        total_replicas: int,
        port: int,
        interval: float,
        shutdown_event: threading.Event,
    ):
        self._my_id = my_id
        self._total_replicas = total_replicas
        self._port = port
        self._interval = interval
        self._shutdown_event = shutdown_event
        self._connections: dict[int, Network] = {}
        self._thread = None

    def start(self):
        """Start the heartbeat sender thread."""
        if self._total_replicas <= 1:
            logging.info("action: peer_heartbeat_client_start | result: skipped | reason: single_replica")
            return

        self._thread = threading.Thread(target=self._send_loop, daemon=True)
        self._thread.start()
        logging.info(f"action: peer_heartbeat_client_start | result: success | targets: {self._total_replicas - 1}")

    def stop(self):
        """Stop the client and close all connections."""
        for network in self._connections.values():
            network.close()
        self._connections.clear()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval + 1)

    def _send_loop(self):
        """Periodically send heartbeats to all peer HCs."""
        while not self._shutdown_event.wait(timeout=self._interval):
            self._send_heartbeats()

    def _send_heartbeats(self):
        """Send heartbeat to all peer HCs."""
        packet = HCHeartbeatPacket(self._my_id, time.time())

        for peer_id in range(self._total_replicas):
            if peer_id == self._my_id:
                continue
            self._send_to_peer(peer_id, packet)

    def _send_to_peer(self, peer_id: int, packet: HCHeartbeatPacket):
        """Send packet to a specific peer, reconnecting if needed."""
        network = self._connections.get(peer_id)

        if network:
            try:
                network.send_packet(packet)
                return
            except NetworkError:
                network.close()
                self._connections.pop(peer_id, None)

        try:
            host = f"health_checker_{peer_id}"
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, self._port))
            network = Network(sock)
            network.send_packet(packet)
            self._connections[peer_id] = network
        except (socket.error, NetworkError):
            pass

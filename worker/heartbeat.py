"""
Heartbeat sender for worker health reporting.
"""

import logging
import os
import socket
import threading
import time

from shared.network import Network, NetworkError
from shared.protocol import HeartbeatPacket


class HeartbeatSender:
    """Sends periodic heartbeats to the health checker."""

    def __init__(self, container_name: str, shutdown_event: threading.Event):
        self._container_name = container_name
        self._shutdown_event = shutdown_event
        self._thread = None
        self._host = os.getenv("HEALTH_CHECKER_HOST")
        self._port = int(os.getenv("HEALTH_CHECKER_PORT", "9090"))
        self._interval = float(os.getenv("HEARTBEAT_INTERVAL", "5"))

    @property
    def enabled(self) -> bool:
        # if not set, missing env variables!
        return self._host is not None

    def start(self):
        """Start the heartbeat thread if health checker is configured."""
        if not self.enabled:
            logging.debug(
                f"action: heartbeat_start | container: {self._container_name} | "
                f"status: failed because no health checker configured"
            )
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logging.debug(f"action: heartbeat_start | container: {self._container_name}")

    def stop(self):
        """Stop the heartbeat thread."""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval + 1)

    def _loop(self):
        """Heartbeat loop that sends periodic heartbeats."""
        while not self._shutdown_event.is_set():
            self._send_heartbeat()
            self._shutdown_event.wait(timeout=self._interval)

    def _send_heartbeat(self):
        """Send a single heartbeat to the health checker."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self._host, self._port))
            network = Network(sock)
            packet = HeartbeatPacket(self._container_name, time.time())
            network.send_packet(packet)
            sock.close()
        except (socket.error, NetworkError) as e:
            logging.debug(f"action: heartbeat_send | result: fail | error: {e}")


def build_container_name(stage_name: str, index: int, instances: int) -> str:
    """Build the container name based on stage configuration."""
    if instances > 1:
        return f"{stage_name}_{index}"
    return stage_name

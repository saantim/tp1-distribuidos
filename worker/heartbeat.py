"""
Heartbeat sender for worker health reporting via UDP.
"""

import logging
import os
import socket
import threading
import time

from shared.entity import Heartbeat


class HeartbeatSender:
    """Sends periodic UDP heartbeats to all health checkers."""

    def __init__(self, container_name: str, shutdown_event: threading.Event):
        self._container_name = container_name
        self._shutdown_event = shutdown_event
        self._thread = None
        self._socket = None
        self._port = int(os.getenv("HEALTH_CHECKER_PORT", "9090"))
        self._interval = float(os.getenv("HEARTBEAT_INTERVAL", "5"))
        self._replicas = int(os.getenv("HEALTH_CHECKER_REPLICAS", "0"))
        self._hc_hosts = self._build_hc_hosts()

    def _build_hc_hosts(self) -> list[str]:
        """Build list of health checker hostnames from replica count."""
        if self._replicas <= 0:
            return []
        return [f"health_checker_{i}" for i in range(self._replicas)]

    @property
    def enabled(self) -> bool:
        return len(self._hc_hosts) > 0

    def start(self):
        """Start the heartbeat thread if health checkers are configured."""
        if not self.enabled:
            return
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the heartbeat thread and close socket."""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval + 1)
        if self._socket:
            self._socket.close()
            self._socket = None

    def _loop(self):
        """Heartbeat loop that sends periodic heartbeats."""
        while not self._shutdown_event.is_set():
            self._send_heartbeats()
            self._shutdown_event.wait(timeout=self._interval)

    def _send_heartbeats(self):
        """Send heartbeat to all health checkers."""
        heartbeat = Heartbeat(container_name=self._container_name, timestamp=time.time())
        data = heartbeat.serialize()

        for host in self._hc_hosts:
            try:
                self._socket.sendto(data, (host, self._port))
            except socket.error as e:
                logging.warning(f"action: heartbeat_send | target: {host} | result: fail | error: {e}")


def build_container_name(stage_name: str, index: int, instances: int) -> str:
    """Build the container name based on stage configuration."""
    if instances > 1:
        return f"{stage_name}_{index}"
    return stage_name

"""
Health checker UDP server that receives heartbeats from workers and revives dead containers.
"""

import logging
import socket
import subprocess
import threading

from health_checker.registry.worker_registry import WorkerRegistry
from shared.entity import Heartbeat
from shared.shutdown import ShutdownSignal


UDP_BUFFER_SIZE = 1024


class HealthChecker:
    """
    UDP server that receives worker heartbeats and monitors their health.
    Revives unresponsive workers via docker start.
    """

    def __init__(
        self,
        port: int,
        check_interval: float,
        timeout_threshold: float,
        shutdown_signal: ShutdownSignal,
    ):
        self.port = port
        self.check_interval = check_interval
        self.timeout_threshold = timeout_threshold
        self.shutdown_signal = shutdown_signal
        self.registry = WorkerRegistry()
        self.registered_workers: set[str] = set()
        self.socket = None
        self.health_check_thread = None

    def run(self):
        """Start the health checker server."""
        try:
            self._setup_socket()
            self._start_health_check_loop()
            self._receive_heartbeats()
        except Exception as e:
            logging.error(f"action: health_checker_run | result: fail | error: {e}")
        finally:
            self._cleanup()

    def _setup_socket(self):
        """Setup and bind UDP socket."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", self.port))
        self.socket.settimeout(1.0)
        logging.info(f"action: health_checker_start | result: success | port: {self.port}")

    def _start_health_check_loop(self):
        """Start background thread for checking dead workers."""
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=False)
        self.health_check_thread.start()

    def _health_check_loop(self):
        """Periodically check for dead workers and revive them."""
        while not self.shutdown_signal.wait(timeout=self.check_interval):
            dead_workers = self.registry.get_dead_workers(self.timeout_threshold)
            for worker in dead_workers:
                self._revive_worker(worker.container_name)

    def _revive_worker(self, container_name: str):
        """Attempt to restart a dead worker container."""
        logging.warning(f"action: revive_worker | container: {container_name}")
        try:
            result = subprocess.run(
                ["docker", "start", container_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                logging.info(f"action: revive_worker | result: success | container: {container_name}")
            else:
                logging.error(
                    f"action: revive_worker | result: fail | container: {container_name} | error: {result.stderr}"
                )
        except subprocess.TimeoutExpired:
            logging.error(f"action: revive_worker | result: timeout | container: {container_name}")
        except Exception as e:
            logging.error(f"action: revive_worker | result: fail | container: {container_name} | error: {e}")

    def _receive_heartbeats(self):
        while not self.shutdown_signal.should_shutdown():
            try:
                data, _ = self.socket.recvfrom(UDP_BUFFER_SIZE)
                self._handle_heartbeat(data)
            except socket.timeout:
                continue
            except Exception as e:
                if not self.shutdown_signal.should_shutdown():
                    logging.error(f"action: receive_heartbeat | result: fail | error: {e}")

    def _handle_heartbeat(self, data: bytes):
        try:
            heartbeat = Heartbeat.deserialize(data)
            is_new = heartbeat.container_name not in self.registered_workers
            self.registry.update(heartbeat.container_name, heartbeat.timestamp)
            if is_new:
                self.registered_workers.add(heartbeat.container_name)
                logging.info(f"action: worker_registered | container: {heartbeat.container_name}")
        except Exception as e:
            logging.warning(f"action: heartbeat_parse_error | error: {e}")

    def _cleanup(self):
        logging.info("action: health_checker_cleanup_start")

        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=5.0)

        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logging.error(f"action: health_checker_cleanup | result: fail | error: {e}")

        logging.info("action: health_checker_stop | result: success")

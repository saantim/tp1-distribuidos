"""
Health checker TCP server that receives heartbeats from workers and revives dead containers.
"""

import logging
import socket
import subprocess
import threading
import time

from shared.network import Network, NetworkError
from shared.protocol import HeartbeatPacket
from shared.shutdown import ShutdownSignal

from .pool import ThreadPool
from .registry import WorkerRegistry


class HealthChecker:
    """
    TCP server that receives worker heartbeats and monitors their health.
    Revives unresponsive workers via docker start.
    """

    def __init__(
        self,
        port: int,
        check_interval: float,
        timeout_threshold: float,
        pool_size: int,
        shutdown_signal: ShutdownSignal,
    ):
        self.port = port
        self.check_interval = check_interval
        self.timeout_threshold = timeout_threshold
        self.shutdown_signal = shutdown_signal
        self.registry = WorkerRegistry()
        self.server_socket = None
        self.pool = ThreadPool(pool_size, shutdown_signal)
        self.health_check_thread = None

    def run(self):
        """Start the health checker server."""
        try:
            self._setup_server_socket()
            self.pool.start()
            self._start_health_check_loop()
            self._accept_connections()
        except Exception as e:
            logging.error(f"action: health_checker_run | result: fail | error: {e}")
        finally:
            self._cleanup()

    def _setup_server_socket(self):
        """Setup and bind server socket."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("", self.port))
        self.server_socket.listen(100)
        logging.info(f"action: health_checker_start | result: success | port: {self.port}")

    def _start_health_check_loop(self):
        """Start background thread for checking dead workers."""
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_check_thread.start()

    def _health_check_loop(self):
        """Periodically check for dead workers and revive them."""
        while not self.shutdown_signal.should_shutdown():
            time.sleep(self.check_interval)
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
                    f"action: revive_worker | result: fail | container: {container_name} | " f"error: {result.stderr}"
                )
        except subprocess.TimeoutExpired:
            logging.error(f"action: revive_worker | result: timeout | container: {container_name}")
        except Exception as e:
            logging.error(f"action: revive_worker | result: fail | container: {container_name} | error: {e}")

    def _accept_connections(self):
        """Accept heartbeat connections from workers."""
        while not self.shutdown_signal.should_shutdown():
            try:
                self.server_socket.settimeout(1.0)
                client_socket, _ = self.server_socket.accept()
                self.pool.submit(lambda sock=client_socket: self._handle_heartbeat(sock))
            except socket.timeout:
                continue
            except Exception as e:
                if not self.shutdown_signal.should_shutdown():
                    logging.error(f"action: accept_heartbeat | result: fail | error: {e}")

    def _handle_heartbeat(self, client_socket: socket.socket):
        """Handle a single heartbeat connection."""
        try:
            network = Network(client_socket, self.shutdown_signal)
            packet = network.recv_packet()
            if isinstance(packet, HeartbeatPacket):
                self.registry.update(packet.container_name, packet.timestamp)
                logging.debug(f"action: heartbeat_received | container: {packet.container_name}")
        except NetworkError as e:
            logging.debug(f"action: heartbeat_error | error: {e}")
        finally:
            client_socket.close()

    def _cleanup(self):
        """Cleanup server resources."""
        logging.info("action: health_checker_cleanup_start")

        self.pool.stop()

        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=5.0)

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logging.error(f"action: health_checker_cleanup | result: fail | error: {e}")

        logging.info("action: health_checker_stop | result: success")

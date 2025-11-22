"""
Health checker server that receives heartbeats and revives dead containers.
Supports leader election for fault-tolerant operation with multiple replicas.
"""

import logging
import socket
import subprocess
import threading

from health_checker.leader.election import LeaderElection
from health_checker.leader.heartbeat_client import HeartbeatClient
from health_checker.leader.heartbeat_server import HeartbeatServer
from health_checker.registry import Registry
from shared.entity import Heartbeat
from shared.shutdown import ShutdownSignal


UDP_BUFFER_SIZE = 1024


class HealthChecker:
    """
    UDP server that receives worker heartbeats and monitors their health.
    Only the leader performs container revivals.
    """

    def __init__(
        self,
        replica_id: int,
        replicas: int,
        worker_port: int,
        peer_port: int,
        check_interval: float,
        worker_timeout: float,
        peer_heartbeat_interval: float,
        peer_timeout: float,
        persist_path: str | None,
        shutdown_signal: ShutdownSignal,
    ):
        self._replica_id = replica_id
        self._replicas = replicas
        self._check_interval = check_interval
        self._shutdown_signal = shutdown_signal

        # Worker tracking
        self._worker_port = worker_port
        self._worker_timeout = worker_timeout
        self._worker_registry = Registry(persist_path)
        self._worker_socket = None
        self._registered_workers: set[str] = set()

        # Peer tracking (leader election)
        self._peer_port = peer_port
        self._peer_timeout = peer_timeout
        self._peer_registry = Registry()
        self._leader_election = LeaderElection(replica_id, self._peer_registry, peer_timeout)
        self._peer_heartbeat_server = HeartbeatServer(replica_id, peer_port, self._peer_registry, shutdown_signal.event)
        self._peer_heartbeat_client = HeartbeatClient(
            replica_id, replicas, peer_port, peer_heartbeat_interval, shutdown_signal.event
        )

        self._health_check_thread = None

    def run(self):
        """Start the health checker server."""
        try:
            self._worker_registry.load()
            self._setup_worker_socket()
            self._peer_heartbeat_server.start()
            self._peer_heartbeat_client.start()
            self._start_health_check_loop()
            self._receive_worker_heartbeats()
        except Exception as e:
            logging.error(f"action: health_checker_run | result: fail | error: {e}")
        finally:
            self._cleanup()

    def _setup_worker_socket(self):
        """Setup and bind UDP socket for worker heartbeats."""
        self._worker_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._worker_socket.bind(("", self._worker_port))
        self._worker_socket.settimeout(1.0)
        logging.info(
            f"action: health_checker_start | result: success | "
            f"replica_id: {self._replica_id} | worker_port: {self._worker_port}"
        )

    def _start_health_check_loop(self):
        """Start background thread for checking dead containers."""
        self._health_check_thread = threading.Thread(target=self._health_check_loop, daemon=False)
        self._health_check_thread.start()

    def _health_check_loop(self):
        """Periodically check for dead containers and revive them if leader."""
        while not self._shutdown_signal.wait(timeout=self._check_interval):
            self._worker_registry.persist()

            if not self._leader_election.am_i_leader():
                continue

            for worker in self._worker_registry.get_dead(self._worker_timeout):
                self._revive_container(worker)

            for peer_id in self._peer_registry.get_dead(self._peer_timeout):
                self._revive_container(f"health_checker_{peer_id}")
                self._peer_registry.remove(peer_id)

    def _revive_container(self, name: str):
        """Attempt to restart a dead container."""
        logging.warning(f"action: revive_container | container: {name}")
        try:
            result = subprocess.run(
                ["docker", "start", name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                logging.info(f"action: revive_container | result: success | container: {name}")
            else:
                logging.error(f"action: revive_container | result: fail | container: {name} | error: {result.stderr}")
        except subprocess.TimeoutExpired:
            logging.error(f"action: revive_container | result: timeout | container: {name}")
        except Exception as e:
            logging.error(f"action: revive_container | result: fail | container: {name} | error: {e}")

    def _receive_worker_heartbeats(self):
        """Receive worker heartbeats via UDP."""
        while not self._shutdown_signal.should_shutdown():
            try:
                data, _ = self._worker_socket.recvfrom(UDP_BUFFER_SIZE)
                self._handle_worker_heartbeat(data)
            except socket.timeout:
                continue
            except Exception as e:
                if not self._shutdown_signal.should_shutdown():
                    logging.error(f"action: receive_worker_heartbeat | result: fail | error: {e}")

    def _handle_worker_heartbeat(self, data: bytes):
        """Process a worker heartbeat."""
        try:
            heartbeat = Heartbeat.deserialize(data)
            is_new = heartbeat.container_name not in self._registered_workers
            self._worker_registry.update(heartbeat.container_name, heartbeat.timestamp)
            if is_new:
                self._registered_workers.add(heartbeat.container_name)
                logging.info(f"action: worker_registered | container: {heartbeat.container_name}")
        except Exception as e:
            logging.warning(f"action: worker_heartbeat_parse_error | error: {e}")

    def _cleanup(self):
        """Clean up resources on shutdown."""
        logging.info("action: health_checker_cleanup_start")

        self._peer_heartbeat_client.stop()
        self._peer_heartbeat_server.stop()

        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=5.0)

        if self._worker_socket:
            try:
                self._worker_socket.close()
            except Exception as e:
                logging.error(f"action: health_checker_cleanup | result: fail | error: {e}")

        logging.info("action: health_checker_stop | result: success")

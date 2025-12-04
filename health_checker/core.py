"""
Health checker server that receives heartbeats and revives dead containers.
Supports leader election via Bully algorithm for fault-tolerant operation.
"""

import logging
import socket
import subprocess
import threading

from health_checker.leader.election import BullyElection
from health_checker.leader.peer_client import PeerClient
from health_checker.leader.peer_server import PeerServer
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
        election_timeout: float,
        coordinator_timeout: float,
        persist_path: str | None,
        shutdown_signal: ShutdownSignal,
    ):
        self._replica_id = replica_id
        self._replicas = replicas
        self._check_interval = check_interval
        self._shutdown_signal = shutdown_signal

        self._worker_port = worker_port
        self._worker_timeout = worker_timeout
        self._worker_registry = Registry(persist_path)
        self._worker_socket = None
        self._registered_workers: set[str] = set()

        self._peer_port = peer_port
        self._peer_timeout = peer_timeout
        self._peer_registry = Registry()

        self._peer_client = PeerClient(
            my_id=replica_id,
            total_replicas=replicas,
            port=peer_port,
            heartbeat_interval=peer_heartbeat_interval,
            shutdown_event=shutdown_signal.event,
        )

        self._peer_server = PeerServer(
            port=peer_port,
            peer_registry=self._peer_registry,
            shutdown_event=shutdown_signal.event,
        )

        self._election = BullyElection(
            my_id=replica_id,
            total_replicas=replicas,
            election_timeout=election_timeout,
            coordinator_timeout=coordinator_timeout,
            send_election_fn=self._peer_client.send_election,
            send_ok_fn=self._peer_client.send_ok,
            send_coordinator_fn=self._peer_client.send_coordinator,
        )

        self._peer_server.set_election(self._election)
        self._peer_server.set_on_election_received(self._peer_client.clear_connection)

        self._health_check_thread = None
        self._leader_monitor_thread = None

    def run(self):
        """Start the health checker server."""
        try:
            self._worker_registry.load()
            self._registered_workers = set(self._worker_registry.get_all().keys())
            self._setup_worker_socket()
            self._peer_server.start()
            self._peer_client.start()
            self._start_health_check_loop()
            self._start_leader_monitor()

            self._election.start_election()

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

    def _start_leader_monitor(self):
        """Start background thread for monitoring leader liveness."""
        self._leader_monitor_thread = threading.Thread(target=self._leader_monitor_loop, daemon=True)
        self._leader_monitor_thread.start()

    def _leader_monitor_loop(self):
        """Monitor leader liveness and trigger election if leader dies."""
        while not self._shutdown_signal.wait(timeout=self._peer_timeout):
            current_leader = self._election.get_current_leader()

            if current_leader is None or current_leader == self._replica_id:
                continue

            dead_peers = self._peer_registry.get_dead(self._peer_timeout)
            if str(current_leader) in dead_peers:
                logging.warning(f"action: leader_dead_detected | leader: {current_leader}")
                self._election.start_election()

    def _health_check_loop(self):
        """Periodically check for dead containers and revive them if leader."""
        while not self._shutdown_signal.wait(timeout=self._check_interval):
            self._worker_registry.persist()

            if not self._election.am_i_leader():
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

        self._peer_client.stop()
        self._peer_server.stop()

        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=5.0)

        if self._worker_socket:
            try:
                self._worker_socket.close()
            except Exception as e:
                logging.error(f"action: health_checker_cleanup | result: fail | error: {e}")

        logging.info("action: health_checker_stop | result: success")

"""TCP server that receives heartbeats from other health checkers."""

import logging
import socket
import threading

from health_checker.registry.peer import PeerRegistry
from shared.network import Network, NetworkError
from shared.protocol import HCHeartbeatPacket


class HeartbeatServer:
    """TCP server that receives heartbeats from peer health checkers."""

    def __init__(
        self,
        my_id: int,
        port: int,
        peer_registry: PeerRegistry,
        shutdown_event: threading.Event,
    ):
        self._my_id = my_id
        self._port = port
        self._peer_registry = peer_registry
        self._shutdown_event = shutdown_event
        self._socket = None
        self._accept_thread = None
        self._handler_threads: list[threading.Thread] = []
        self._threads_lock = threading.Lock()

    def start(self):
        """Start the TCP server in a background thread."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(1.0)
        self._socket.bind(("0.0.0.0", self._port))
        self._socket.listen(10)

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        logging.info(f"action: peer_heartbeat_server_start | result: success | port: {self._port}")

    def stop(self):
        """Stop the server and wait for all handler threads."""
        if self._socket:
            self._socket.close()
            self._socket = None

        if self._accept_thread and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=2.0)

        with self._threads_lock:
            threads_to_join = list(self._handler_threads)

        for thread in threads_to_join:
            thread.join(timeout=1.0)

        logging.info("action: peer_heartbeat_server_stop | result: success")

    def _accept_loop(self):
        """Accept incoming connections and spawn handlers."""
        while not self._shutdown_event.is_set():
            try:
                conn, _ = self._socket.accept()
                handler = threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True,
                )
                with self._threads_lock:
                    self._handler_threads = [t for t in self._handler_threads if t.is_alive()]
                    self._handler_threads.append(handler)
                handler.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn: socket.socket):
        """Handle a single connection, receiving heartbeats until disconnect."""
        conn.settimeout(5.0)
        network = Network(conn)
        try:
            while not self._shutdown_event.is_set():
                packet = network.recv_packet()
                if packet is None:
                    break
                if isinstance(packet, HCHeartbeatPacket):
                    self._peer_registry.update(packet.hc_id, packet.timestamp)
        except (NetworkError, socket.timeout, Exception):
            pass
        finally:
            network.close()

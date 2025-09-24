"""
gateway tcp server that accepts client connections and routes packets to middleware.
currently handles single client connection.
"""

import logging
import socket
from typing import Optional

from handler import ClientHandler
from router import PacketRouter

from shared.shutdown import ShutdownSignal


class Server:
    """
    tcp server that accepts client connections and handles data processing sessions.
    routes incoming packets to appropriate middleware queues.
    """

    def __init__(self, port: int, listen_backlog: int, router: PacketRouter, shutdown_signal: ShutdownSignal):
        """
        create server instance.

        args:
            port: tcp port to listen on
            router: packet router for middleware publishing
            shutdown_signal: shutdown signal handler
        """
        self.port = port
        self.router = router
        self.shutdown_signal = shutdown_signal
        self.backlog = listen_backlog
        self.server_socket: Optional[socket.socket] = None

    def run(self):
        """
        start server and handle client connections.
        """
        try:
            self._setup_server_socket()
            self._accept_connections()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
        finally:
            self._cleanup()

    def _setup_server_socket(self):
        """setup and bind server socket."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("", self.port))
        self.server_socket.listen(self.backlog)

        logging.info(f"action: server_start | result: success | port: {self.port}")

    def _accept_connections(self):
        """accept and handle client connections."""
        while not self.shutdown_signal.should_shutdown():
            try:
                self.server_socket.settimeout(1.0)  # esto para responsive shutdown.
                client_socket, client_address = self.server_socket.accept()

                logging.info(f"action: client_connect | client: {client_address}")

                handler = ClientHandler(client_socket, self.router, self.shutdown_signal)
                handler.handle_session()

                logging.info(f"action: client_disconnect | client: {client_address}")

            except socket.timeout:
                continue
            except Exception as e:
                if not self.shutdown_signal.should_shutdown():
                    logging.error(f"action: accept_connection | result: fail | error: {e}")

    def _cleanup(self):
        """cleanup server resources."""
        if self.server_socket:
            try:
                self.server_socket.close()
                logging.info("action: server_stop | result: success")
            except Exception as e:
                logging.error(f"action: server_cleanup | result: fail | error: {e}")
            finally:
                self.server_socket = None

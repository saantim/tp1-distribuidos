"""
gateway tcp server that accepts client connections and routes packets to middleware.
currently handles single client connection.
"""

import logging
import socket
from typing import Optional

from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import PacketType
from shared.shutdown import ShutdownSignal

from .handler import ClientHandler


class Server:
    """
    tcp server that accepts client connections and handles data processing sessions.
    routes incoming packets to appropriate middleware queues.
    """

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        middleware_host: str,
        demux_queues: dict,
        shutdown_signal: ShutdownSignal,
    ):
        self.port = port
        self.middleware_host = middleware_host
        self.demux_queues = demux_queues
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
                self.server_socket.settimeout(1.0)
                client_socket, client_address = self.server_socket.accept()

                logging.info(f"action: client_connect | client: {client_address}")

                publishers = {
                    PacketType.STORE_BATCH: MessageMiddlewareQueueMQ(self.middleware_host, self.demux_queues["stores"]),
                    PacketType.USERS_BATCH: MessageMiddlewareQueueMQ(self.middleware_host, self.demux_queues["users"]),
                    PacketType.TRANSACTIONS_BATCH: MessageMiddlewareQueueMQ(
                        self.middleware_host, self.demux_queues["transactions"]
                    ),
                    PacketType.TRANSACTION_ITEMS_BATCH: MessageMiddlewareQueueMQ(
                        self.middleware_host, self.demux_queues["transaction_items"]
                    ),
                    PacketType.MENU_ITEMS_BATCH: MessageMiddlewareQueueMQ(
                        self.middleware_host, self.demux_queues["menu_items"]
                    ),
                }

                handler = ClientHandler(client_socket, publishers, self.middleware_host, self.shutdown_signal)
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

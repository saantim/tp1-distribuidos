"""
gateway tcp server that accepts MULTIPLE concurrent client connections.
spawns thread per client and routes packets to middleware with session_id tagging.
"""

import logging
import socket
import threading
from typing import Optional

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from shared.protocol import EntityType
from shared.shutdown import ShutdownSignal

from .handler import ClientHandler
from .results import ResultCollector
from .session import SessionManager


class Server:
    """
    tcp server that accepts MULTIPLE concurrent client connections.
    each client gets unique session_id for tracking throughout pipeline.
    """

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        middleware_host: str,
        batch_exchanges: dict,
        shutdown_signal: ShutdownSignal,
    ):
        self.port = port
        self.middleware_host = middleware_host
        self.batch_exchanges = batch_exchanges
        self.shutdown_signal = shutdown_signal
        self.backlog = listen_backlog
        self.server_socket: Optional[socket.socket] = None

        self.session_manager = SessionManager()
        self.publishers = self._create_publishers()
        self.result_collector = ResultCollector(middleware_host, self.session_manager, shutdown_signal)
        self.client_threads = []
        self.client_threads_lock = threading.Lock()

    def run(self):
        """
        start server and handle multiple concurrent client connections.
        """
        try:
            self._setup_server_socket()
            self.result_collector.start()
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

        logging.info(f"action: server_start | result: success | port: {self.port} | " f"backlog: {self.backlog}")

    def _accept_connections(self):
        """accept and handle multiple concurrent client connections."""
        while not self.shutdown_signal.should_shutdown():
            try:
                self.server_socket.settimeout(1.0)
                client_socket, client_address = self.server_socket.accept()

                logging.info(
                    f"action: client_accept | client: {client_address} | "
                    f"active_sessions: {self.session_manager.get_active_session_count()}"
                )

                session_id = self.session_manager.create_session(client_socket, client_address)
                client_thread = threading.Thread(
                    target=self._handle_client_thread,
                    args=(client_socket, client_address, session_id, self.publishers),
                    name=f"client-{session_id}",
                    daemon=False,
                )
                client_thread.start()

                with self.client_threads_lock:
                    self.client_threads.append(client_thread)

            except socket.timeout:
                continue
            except Exception as e:
                if not self.shutdown_signal.should_shutdown():
                    logging.error(f"action: accept_connection | result: fail | error: {e}")

    def _handle_client_thread(self, client_socket, client_address, session_id, publishers):
        """Thread target for handling individual client."""
        try:
            handler = ClientHandler(
                client_socket, client_address, session_id, publishers, self.session_manager, self.shutdown_signal
            )
            handler.handle_session()

            logging.info(f"action: client_handler_complete | session_id: {session_id} | " f"client: {client_address}")

        except Exception as e:
            logging.exception(f"action: client_thread_error | session_id: {session_id} | error: {e}")
        finally:
            for publisher in publishers.values():
                try:
                    publisher.close()
                except Exception:
                    pass

    def _create_publishers(self) -> dict:
        """Create middleware publishers for a client (thread-local connections)."""
        return {
            EntityType.STORE: MessageMiddlewareExchangeRMQ(self.middleware_host, self.batch_exchanges["STORE"]),
            EntityType.USER: MessageMiddlewareExchangeRMQ(self.middleware_host, self.batch_exchanges["USER"]),
            EntityType.TRANSACTION: MessageMiddlewareExchangeRMQ(
                self.middleware_host, self.batch_exchanges["TRANSACTION"]
            ),
            EntityType.TRANSACTION_ITEM: MessageMiddlewareExchangeRMQ(
                self.middleware_host, self.batch_exchanges["TRANSACTION_ITEM"]
            ),
            EntityType.MENU_ITEM: MessageMiddlewareExchangeRMQ(self.middleware_host, self.batch_exchanges["MENU_ITEM"]),
        }

    def _cleanup(self):
        """cleanup server resources and wait for client threads."""
        logging.info("action: server_cleanup_start")

        with self.client_threads_lock:
            active_threads = [t for t in self.client_threads if t.is_alive()]

        if active_threads:
            logging.info(f"action: waiting_for_clients | count: {len(active_threads)}")
            for thread in active_threads:
                thread.join(timeout=5.0)

        if self.server_socket:
            try:
                self.server_socket.close()
                logging.info("action: server_stop | result: success")
            except Exception as e:
                logging.error(f"action: server_cleanup | result: fail | error: {e}")
            finally:
                self.server_socket = None

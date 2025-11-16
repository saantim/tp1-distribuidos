"""
result collector that listens to multiple query result queues and streams to client.
"""

import logging
import threading
from uuid import UUID

from gateway.core.session import SessionData
from shared.entity import EOF
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ, MessageMiddlewareQueueMQ
from shared.network import Network, NetworkError
from shared.protocol import ResultPacket, SESSION_ID
from shared.shutdown import ShutdownSignal


class ResultCollector:
    """
    Listens to all query result queues in separate threads.
    Multiplexes results to correct client sessions based on session_id header.
    """

    EXCHANGE_NAME = "results"

    def __init__(self, middleware_host: str, session_manager, shutdown_signal: ShutdownSignal):
        self.middleware_host = middleware_host
        self.session_manager = session_manager
        self.shutdown_signal = shutdown_signal

        # For now, only Q1 is enabled
        self.query_keys = ["q1"]

        self._listener_thread = None

    def start(self):
        """Start listening to results exchange."""
        thread = threading.Thread(
            target=self._listen_exchange,
            name="result-collector",
            daemon=False,
        )
        thread.start()
        self._listener_thread = thread
        logging.info(f"action: global_result_collector_started | queries: {self.query_keys}")

    def _listen_exchange(self):
        """Listen to results exchange and route messages to correct sessions."""
        exchange = MessageMiddlewareExchangeRMQ(
            host=self.middleware_host,
            exchange_name=self.EXCHANGE_NAME,
            route_keys=self.query_keys,
        )

        def on_message(channel, method, properties, body):
            if self.shutdown_signal.should_shutdown():
                exchange.stop_consuming()
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Extract query ID from routing key (e.g., "q1", "q2", etc.)
            query_id = method.routing_key

            session_id = None
            if properties.headers:
                session_id = properties.headers.get(SESSION_ID)

            if not session_id:
                logging.error(f"action: result_no_session | query: {query_id} | result: dropping_message")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            session_id = UUID(hex=session_id)
            session: SessionData = self.session_manager.get_session(session_id)

            if not session:
                logging.warning(
                    f"action: result_unknown_session | query: {query_id} | "
                    f"session_id: {session_id} | result: session_closed"
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Store result and send to client
            self.session_manager.add_result(session_id, query_id.upper(), body)

            try:
                network = Network(session.socket, self.shutdown_signal)

                result_packet = ResultPacket(query_id.upper(), body)
                network.send_packet(result_packet)

                eof_packet = ResultPacket(query_id.upper(), EOF().serialize())
                network.send_packet(eof_packet)
                channel.basic_ack(delivery_tag=method.delivery_tag)

                if self.session_manager.is_session_complete(session_id):
                    logging.info(f"action: session_results_complete | session_id: {session_id}")
                    # Close session after 5 seconds to ensure everything is sent
                    threading.Timer(5.0, self.session_manager.close_session, args=[session_id]).start()

            except NetworkError as err:
                logging.warning(
                    f"action: result_send_fail | session_id: {session_id} | query: {query_id} | error: {err}"
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.session_manager.close_session(session_id)

        try:
            logging.info(f"action: start_listening | exchange: {self.EXCHANGE_NAME} | routing_keys: {self.query_keys}")
            exchange.start_consuming(on_message)
        except Exception as e:
            logging.error(f"action: result_listener_error | exchange: {self.EXCHANGE_NAME} | error: {e}")
        finally:
            try:
                exchange.close()
            except Exception:
                pass

    def _listen_to_queue(self, query_id: str, queue_name: str):
        """Listen to a specific query result queue and route to correct session."""
        queue = MessageMiddlewareQueueMQ(self.middleware_host, queue_name)

        def on_message(channel, method, properties, body):
            if self.shutdown_signal.should_shutdown():
                queue.stop_consuming()
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            session_id = None
            if properties.headers:
                session_id = properties.headers.get(SESSION_ID)

            if not session_id:
                logging.error(f"action: result_no_session | query: {query_id} | " f"result: dropping_message")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            session_id = UUID(hex=session_id)
            session: SessionData = self.session_manager.get_session(session_id)

            is_final_data = properties.headers.get("FINAL") is not None
            if not is_final_data:
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logging.debug(
                    f"action: ignore_eof | query: {query_id} |"
                    f" session_id: {session_id} | gateway uses new protocol."
                )
                return

            if not session:
                logging.warning(
                    f"action: result_unknown_session | query: {query_id} | "
                    f"session_id: {session_id} | result: session_closed"
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.session_manager.add_result(session_id, query_id, body)

            try:
                network = Network(session.socket, self.shutdown_signal)

                result_packet = ResultPacket(query_id, body)
                network.send_packet(result_packet)

                eof_packet = ResultPacket(query_id, EOF().serialize())
                network.send_packet(eof_packet)
                channel.basic_ack(delivery_tag=method.delivery_tag)

                if self.session_manager.is_session_complete(session_id):
                    logging.info(f"action: session_results_complete | session_id: {session_id}")
                    # todo: ver de mejorar esto, cerramos luego de 5 segundo para que se envie todo.
                    threading.Timer(5.0, self.session_manager.close_session, args=[session_id]).start()

            except NetworkError as err:
                logging.warning(
                    f"action: result_send_fail | session_id: {session_id} | " f"query: {query_id} | error: {err}"
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.session_manager.close_session(session_id)

        try:
            logging.info(f"action: start_listening | query: {query_id} | queue: {queue_name}")
            queue.start_consuming(on_message)
        except Exception as e:
            logging.error(f"action: result_listener_error | query: {query_id} | error: {e}")
        finally:
            try:
                queue.close()
            except Exception as e:
                _ = e
                pass

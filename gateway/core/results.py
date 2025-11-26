"""
result collector that listens to multiple query result queues and streams to client.
"""

import logging
import threading
from uuid import UUID

from gateway.core.session import SessionData
from shared.entity import EOF, RawMessage
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from shared.network import Network, NetworkError
from shared.protocol import ResultPacket, SESSION_ID
from shared.shutdown import ShutdownSignal
from shared.utils import unpack_result_batch


class ResultCollector:
    """
    Listens to all query result queues in separate threads.
    Multiplexes results to correct client sessions based on session_id header.
    """

    EXCHANGE_NAME = "results"

    def __init__(self, middleware_host: str, session_manager, enabled_queries: list, shutdown_signal: ShutdownSignal):
        self.middleware_host = middleware_host
        self.session_manager = session_manager
        self.shutdown_signal = shutdown_signal
        self.query_keys = enabled_queries
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
        route_keys = self.query_keys + ["common"]
        exchange = MessageMiddlewareExchangeRMQ(
            host=self.middleware_host,
            exchange_name=self.EXCHANGE_NAME,
            route_keys=route_keys,
            queue_name=self.EXCHANGE_NAME,
        )

        def on_message(channel, method, properties, body):
            if self.shutdown_signal.should_shutdown():
                exchange.stop_consuming()
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

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
                    f"action: result_unknown_session | routing_key: {query_id} | "
                    f"session_id: {session_id} | result: session_closed"
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            is_eof = EOF.is_type(body)

            if query_id == "common" and is_eof:
                self.session_manager.increment_query_eof(session_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                if self.session_manager.is_session_complete(session_id):
                    logging.info(f"action: session_complete | session_id: {session_id}")
                    self.session_manager.close_session(session_id)
                return

            unpacked_results = unpack_result_batch(body)

            if not unpacked_results:
                logging.warning(f"action: empty_result_batch | session_id: {session_id} | query: {query_id}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                network = Network(session.socket, self.shutdown_signal)

                for result_data in unpacked_results:
                    raw_message = RawMessage.deserialize(result_data)
                    actual_data = raw_message.raw_data

                    self.session_manager.add_result(session_id, query_id.upper(), actual_data)
                    result_packet = ResultPacket(query_id.upper(), actual_data)
                    network.send_packet(result_packet)

                eof_packet = ResultPacket(query_id.upper(), EOF().serialize())
                network.send_packet(eof_packet)
                channel.basic_ack(delivery_tag=method.delivery_tag)

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

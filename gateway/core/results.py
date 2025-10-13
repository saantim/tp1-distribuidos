"""
result collector that listens to multiple query result queues and streams to client.
"""

import logging
import threading
from uuid import UUID

from gateway.core.session import SessionData
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.network import Network, NetworkError
from shared.protocol import ResultPacket, SESSION_ID
from shared.shutdown import ShutdownSignal


class ResultCollector:
    """
    Listens to all query result queues in separate threads.
    Multiplexes results to correct client sessions based on session_id header.
    """

    def __init__(self, middleware_host: str, session_manager, shutdown_signal: ShutdownSignal):
        self.middleware_host = middleware_host
        self.session_manager = session_manager
        self.shutdown_signal = shutdown_signal

        self.query_queues = {
            "Q1": "results_q1",
            "Q2": "results_q2",
            "Q3": "results_q3",
            "Q4": "results_q4",
        }

        self.listener_threads = []

    def start(self):
        """Start listening to all result queues in separate threads."""
        for query_id, queue_name in self.query_queues.items():
            thread = threading.Thread(
                target=self._listen_to_queue,
                args=(query_id, queue_name),
                name=f"result-collector-{query_id}",
                daemon=False,
            )
            thread.start()
            self.listener_threads.append(thread)

        logging.info("action: global_result_collector_started | queries: 4")

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
            if not session:
                logging.warning(
                    f"action: result_unknown_session | query: {query_id} | "
                    f"session_id: {session_id} | result: session_closed"
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.session_manager.add_result(session_id, query_id, body)

            try:
                result_packet = ResultPacket(query_id, body)
                network = Network(session.socket, self.shutdown_signal)
                network.send_packet(result_packet)

                logging.debug(f"action: result_sent | session_id: {session_id} | query: {query_id}")

                channel.basic_ack(delivery_tag=method.delivery_tag)

                if self.session_manager.is_session_complete(session_id):
                    logging.info(f"action: session_results_complete | session_id: {session_id}")
                    # todo: ver de mejorar esto, cerramos luego de 1 segundo para que se envie todo.
                    threading.Timer(1.0, self.session_manager.close_session, args=[session_id]).start()

            except NetworkError as err:
                logging.error(
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

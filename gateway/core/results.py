"""
result collector that listens to multiple query result queues and streams to client.
"""

import logging
import threading

from shared.entity import EOF
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.network import Network, NetworkError
from shared.protocol import ResultPacket
from shared.shutdown import ShutdownSignal


class ResultCollector:
    """
    Listens to multiple result queues and streams results back to client.
    Each query has its own result queue, results are streamed as they arrive.
    """

    def __init__(self, network: Network, middleware_host: str, shutdown_signal: ShutdownSignal):
        self.network = network
        self.middleware_host = middleware_host
        self.shutdown_signal = shutdown_signal
        self.result_queues = {}
        self.eof_received = {}
        self.lock = threading.Lock()
        self.ready_to_send = threading.Event()

    def add_query(self, query_id: str, queue_name: str):
        """Register a query result queue to listen to."""
        self.result_queues[query_id] = MessageMiddlewareQueueMQ(self.middleware_host, queue_name)
        self.eof_received[query_id] = False
        logging.debug(f"action: register_result_queue | query: {query_id} | queue: {queue_name}")

    def signal_ready(self):
        """Signal that client is ready to receive results."""
        self.ready_to_send.set()
        logging.info("action: ready_for_results")

    def start_listening(self):
        """Start listening to all result queues in separate threads."""
        threads = []
        for query_id, queue in self.result_queues.items():
            thread = threading.Thread(target=self._listen_to_queue, args=(query_id, queue), name=f"results-{query_id}")
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        logging.info("action: all_results_sent")

    def _listen_to_queue(self, query_id: str, queue: MessageMiddlewareQueueMQ):
        """Listen to a specific query's result queue and stream to client."""

        def on_message(channel, method, properties, body):
            if self.shutdown_signal.should_shutdown():
                queue.stop_consuming()
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if self._is_eof(body):
                logging.info(f"action: query_complete | query: {query_id}")

                eof_packet = ResultPacket(query_id, body)
                self.network.send_packet(eof_packet)

                with self.lock:
                    self.eof_received[query_id] = True

                queue.stop_consuming()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                result_packet = ResultPacket(query_id, body)
                with self.lock:
                    self.network.send_packet(result_packet)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except NetworkError as error:
                logging.error(f"action: send_result | query: {query_id} | error: {error}")
                queue.stop_consuming()
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        try:
            self.ready_to_send.wait()
            logging.info(f"action: listen_results | query: {query_id}")
            queue.start_consuming(on_message)
        except Exception as e:
            logging.error(f"action: result_listener_error | query: {query_id} | error: {e}")
        finally:
            try:
                queue.close()
            except Exception:
                pass

    @staticmethod
    def _is_eof(data: bytes) -> bool:
        """
        Check if data represents EOF marker.

        TODO: refactor this when redesigning EOF handling.
        Current implementation uses exception-based detection.
        """
        try:
            EOF.deserialize(data)
            return True
        except Exception:
            return False

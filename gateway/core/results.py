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

    def __init__(
        self, network: Network, middleware_host: str, user_cond: threading.Condition, shutdown_signal: ShutdownSignal
    ):
        self.network = network
        self.middleware_host = middleware_host
        self.shutdown_signal = shutdown_signal
        self.user_ready_var = user_cond
        self.can_send = False
        self.result_queues = {}
        self.eof_received = {}
        self.lock = threading.Lock()

    def add_query(self, query_id: str, queue_name: str):
        """Register a query result queue to listen to."""
        self.result_queues[query_id] = MessageMiddlewareQueueMQ(self.middleware_host, queue_name)
        self.eof_received[query_id] = False
        logging.info(f"Registered result queue for {query_id}: {queue_name}")

    def start_listening(self):
        """Start listening to all result queues in separate threads."""
        threads = []
        for query_id, queue in self.result_queues.items():
            thread = threading.Thread(target=self._listen_to_queue, args=(query_id, queue), name=f"results-{query_id}")
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        logging.info("All query results collected and sent to client")

    def _listen_to_queue(self, query_id: str, queue: MessageMiddlewareQueueMQ):
        """Listen to a specific query's result queue and stream to client."""

        def on_message(channel, method, properties, body):
            if self.shutdown_signal.should_shutdown():
                queue.stop_consuming()
                return

            try:
                EOF.deserialize(body)
                logging.info(f"Query {query_id} complete, sending EOF to client")

                eof_packet = ResultPacket(query_id, body)
                self.network.send_packet(eof_packet)

                with self.lock:
                    self.eof_received[query_id] = True

                queue.stop_consuming()
                return
            except Exception:
                pass

            try:
                result_packet = ResultPacket(query_id, body)
                with self.lock:
                    self.network.send_packet(result_packet)
            except NetworkError as e:
                logging.error(f"Failed to send result for {query_id}: {e}")
                queue.stop_consuming()

        try:
            with self.user_ready_var:
                while not self.can_send:
                    self.user_ready_var.wait()
                    self.can_send = True
            logging.info(f"Starting to listen for {query_id} results")
            queue.start_consuming(on_message)
        except Exception as e:
            logging.error(f"Error in result listener for {query_id}: {e}")
        finally:
            try:
                queue.close()
            except Exception:
                pass

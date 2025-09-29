import json
import logging
import threading
from typing import Any, Dict

from shared.middleware.interface import MessageMiddleware
from shared.network import Network
from shared.protocol import PacketType, ResultPacket


class ResultListener:
    """subscribes to result queue and aggregates results."""

    EXPECTED_RESULTS = 4

    def __init__(self, consumer: MessageMiddleware, network: Network):
        self.consumer = consumer
        self.network = network
        self.results: Dict[int, Any] = {}
        self.listener_thread = None
        self.shutdown_event = threading.Event()

    def start(self):
        """start the result listener in a separate thread."""
        self.listener_thread = threading.Thread(target=self.listen, daemon=True)
        self.listener_thread.start()
        logging.info("action: start_result_listener | result: success")

    def listen(self):
        """main listening loop that runs in the thread."""
        try:
            self.consumer.start_consuming(self.store_result)
        except Exception as e:
            logging.error(f"action: listen_results | result: fail | error: {e}")

    def stop(self):
        """stop the result listener and close the consumer."""
        try:
            self.shutdown_event.set()

            if self.consumer:
                self.consumer.stop_consuming()
                self.consumer.close()

            if self.listener_thread and self.listener_thread.is_alive():
                self.listener_thread.join(timeout=5.0)

            logging.info("action: stop_result_listener | result: success")
        except Exception as e:
            logging.error(f"action: stop_result_listener | result: fail | error: {e}")

    def store_result(self, channel, method, properties, body: bytes):
        """callback for storing received results."""
        if self.shutdown_event.is_set():
            return

        try:
            res_n = len(self.results) + 1
            if res_n > self.EXPECTED_RESULTS:
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            res = json.loads(body)
            logging.info(f"Received result nº{res_n}: {res}")

            self.results[res_n] = res

            if res_n == self.EXPECTED_RESULTS:
                self.send()

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logging.error(f"action: store_result | result: json_decode_error | error: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logging.error(f"action: store_result | result: fail | error: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def send(self):
        """send aggregated results to client."""
        if self.shutdown_event.is_set():
            return

        packet = ResultPacket(self.results)
        try:
            self.network.send_packet(packet)
            self._wait_for_ack()
            logging.info("action: result_send | result: success")
        except Exception as e:
            logging.exception("failed while sending results", e)

    def _wait_for_ack(self):
        """wait for ACK packet if not already received."""
        packet = self.network.recv_packet()
        if packet and packet.get_message_type() == PacketType.ACK:
            return
        else:
            raise Exception("result packet not acked by client")

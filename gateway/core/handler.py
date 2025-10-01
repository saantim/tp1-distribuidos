"""
client session handler that processes data transfer sessions.
handles FileSendStart -> batches -> FileSendEnd protocol -> results streaming.
"""

import logging
import threading

from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.network import Network, NetworkError
from shared.protocol import AckPacket, ErrorPacket, PacketType
from shared.shutdown import ShutdownSignal

from .results import ResultCollector


class ClientHandler:
    """
    handles individual client data transfer sessions.
    processes packet stream and routes data packets to middleware.
    """

    def __init__(
        self,
        client_socket,
        publisher: MessageMiddlewareQueueMQ,
        middleware_host: str,
        shutdown_signal: ShutdownSignal,
    ):
        self.network = Network(client_socket, shutdown_signal)
        self.publisher = publisher
        self.middleware_host = middleware_host
        self.shutdown_signal = shutdown_signal

    def handle_session(self):
        """
        handle complete client session.
        expects FileSendStart -> data batches -> FileSendEnd protocol -> results.
        """
        try:
            if not self._wait_for_session_start():
                return

            result_collector = ResultCollector(self.network, self.middleware_host, self.shutdown_signal)
            result_collector.add_query("Q1", "results_q1")
            result_collector.add_query("Q2", "results_q2")
            result_collector.add_query("Q3", "results_q3")
            # result_collector.add_query("Q4", "results_q4")
            results_thread = threading.Thread(target=result_collector.start_listening, name="results-collector")
            results_thread.start()
            self._process_data_batches()
            result_collector.signal_ready()

            results_thread.join()
            logging.info("Session complete, all results sent to client")

        except NetworkError as e:
            logging.exception(f"action: handle_session | result: network_error | error: {e}")
            self._send_error_packet(500, str(e))
        except Exception as e:
            logging.exception(f"action: handle_session | result: fail | error: {e}")
            self._send_error_packet(500, "internal server error")
        finally:
            self.network.close()

    def _wait_for_session_start(self) -> bool:
        packet = self.network.recv_packet()
        if packet is None:
            logging.warning("action: session_start | result: connection_closed")
            return False

        if packet.get_message_type() != PacketType.FILE_SEND_START:
            logging.error(f"action: session_start | result: invalid_packet | type: {packet.get_message_type()}")
            self._send_error_packet(400, "expected FileSendStart")
            return False

        logging.info("action: session_start | result: success")
        self._send_ack_packet()
        return True

    def _process_data_batches(self):
        batch_count = 0

        while not self.shutdown_signal.should_shutdown():
            packet = self.network.recv_packet()
            if packet is None:
                logging.warning("action: process_batches | result: connection_closed")
                break

            packet_type = packet.get_message_type()

            if packet_type == PacketType.FILE_SEND_END:
                logging.info(f"action: process_batches | result: session_end | batches: {batch_count}")
                self._send_ack_packet()
                break

            if self._is_batch_packet(packet_type):
                try:
                    self.publisher.send(packet.serialize())
                    batch_count += 1
                except Exception as e:
                    logging.error(f"action: route_packet | result: fail | error: {e}")
                    self._send_error_packet(500, "routing error")
                    break
            else:
                logging.warning(f"action: process_batches | result: unexpected_packet | type: {packet_type}")
                self._send_error_packet(400, f"unexpected packet type: {packet_type}")
                break

    def _wait_for_session_end(self):
        packet = self.network.recv_packet()
        if packet and packet.get_message_type() == PacketType.FILE_SEND_END:
            logging.info("action: session_end | result: success")
            self._send_ack_packet()

    @staticmethod
    def _is_batch_packet(packet_type: int) -> bool:
        data_packet_types = {
            PacketType.STORE_BATCH,
            PacketType.USERS_BATCH,
            PacketType.TRANSACTIONS_BATCH,
            PacketType.TRANSACTION_ITEMS_BATCH,
            PacketType.MENU_ITEMS_BATCH,
        }
        return packet_type in data_packet_types

    def _send_ack_packet(self):
        try:
            ack_packet = AckPacket()
            self.network.send_packet(ack_packet)
        except NetworkError as e:
            logging.error(f"action: send_ack | result: fail | error: {e}")

    def _send_error_packet(self, error_code: int, message: str):
        try:
            error_packet = ErrorPacket(error_code, message)
            self.network.send_packet(error_packet)
        except NetworkError as e:
            logging.error(f"action: send_error | result: fail | error: {e}")

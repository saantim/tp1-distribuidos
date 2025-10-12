"""
client session handler that processes data transfer sessions.
handles FileSendStart -> batches -> FileSendEnd protocol -> results streaming.
"""

import logging
import threading
import time
from typing import cast

from shared.entity import EOF
from shared.network import Network, NetworkError
from shared.protocol import AckPacket, Batch, ErrorPacket, PacketType
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
        publishers: dict,
        middleware_host: str,
        shutdown_signal: ShutdownSignal,
    ):
        self.network = Network(client_socket, shutdown_signal)
        self.publishers = publishers
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
            result_collector.add_query("Q4", "results_q4")

            results_thread = threading.Thread(target=result_collector.start_listening, name="results-collector")
            results_thread.start()

            batch_start = time.time()
            self._process_data_batches()
            result_collector.signal_ready()
            batch_end = time.time()
            logging.info(f"action: batch_forward | duration: {(batch_end - batch_start):.2f}s")

            results_thread.join()
            logging.info("action: session_complete")

        except NetworkError as e:
            logging.exception(f"action: handle_session | error: {e}")
            self._send_error_packet(500, str(e))
        except Exception as e:
            logging.exception(f"action: handle_session | error: {e}")
            self._send_error_packet(500, "internal server error")
        finally:
            self.network.close()

    def _wait_for_session_start(self) -> bool:
        """Wait for FileSendStart packet."""
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
        """
        Process incoming batch packets and route to appropriate queues.
        Sends EOF marker when batch.eof is True.
        """
        batch_count = 0
        eof_count = 0

        while not self.shutdown_signal.should_shutdown():
            packet = self.network.recv_packet()
            if packet is None:
                logging.warning("action: process_batches | result: connection_closed")
                break

            packet_type = packet.get_message_type()

            if packet_type == PacketType.FILE_SEND_END:
                logging.info(
                    f"action: process_batches | result: session_end |" f" batches: {batch_count} | eofs: {eof_count}"
                )
                self._send_ack_packet()
                break

            if packet_type == PacketType.BATCH:
                batch = cast(Batch, packet)
                try:
                    publisher = self.publishers.get(batch.entity_type)
                    if not publisher:
                        logging.error(f"action: route_batch | result: no_publisher | entity_type: {batch.entity_type}")
                        self._send_error_packet(500, f"no publisher for entity type {batch.entity_type}")
                        break

                    if batch.eof:
                        publisher.send(EOF().serialize())
                        eof_count += 1
                        logging.debug(f"action: route_eof | entity_type: {batch.entity_type.name}")
                    else:
                        publisher.send(batch.serialize())
                        batch_count += 1

                        if batch_count % 100 == 0:
                            logging.debug(f"action: route_progress | batches: {batch_count}")

                except Exception as e:
                    logging.error(f"action: route_batch | result: fail | error: {e}")
                    self._send_error_packet(500, "routing error")
                    break
            else:
                logging.warning(f"action: process_batches | result: unexpected_packet | type: {packet_type}")
                self._send_error_packet(400, f"unexpected packet type: {packet_type}")
                break

    def _send_ack_packet(self):
        """Send ACK packet to client."""
        try:
            ack_packet = AckPacket()
            self.network.send_packet(ack_packet)
        except NetworkError as e:
            logging.error(f"action: send_ack | error: {e}")

    def _send_error_packet(self, error_code: int, message: str):
        """Send error packet to client."""
        try:
            error_packet = ErrorPacket(error_code, message)
            self.network.send_packet(error_packet)
        except NetworkError as e:
            logging.error(f"action: send_error | error: {e}")

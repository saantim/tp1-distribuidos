"""
client session handler that processes data transfer sessions.
handles FileSendStart -> batches -> FileSendEnd protocol -> results streaming.
now supports multi-client sessions.
"""

import logging
import time
import uuid
from typing import cast
from uuid import UUID

from shared.entity import EOF
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from shared.network import Network, NetworkError
from shared.protocol import AckPacket, Batch, EntityType, ErrorPacket, MESSAGE_ID, PacketType, SESSION_ID
from shared.shutdown import ShutdownSignal


class ClientHandler:
    """
    handles individual client data transfer sessions with session_id tagging.
    processes packet stream and routes data packets to middleware with session headers.
    """

    def __init__(
        self,
        client_socket,
        client_address,
        session_id: UUID,
        publishers: dict,
        transformer_configs: dict,
        session_manager,
        shutdown_signal: ShutdownSignal,
    ):
        self.network = Network(client_socket, shutdown_signal)
        self.client_address = client_address
        self.session_id = session_id
        self.publishers = publishers
        self.transformer_configs = transformer_configs
        self.session_manager = session_manager
        self.shutdown_signal = shutdown_signal

    def handle_session(self):
        """
        handle complete client session.
        expects FileSendStart -> data batches -> FileSendEnd protocol.
        results are handled by separate result collector thread.
        """
        try:
            # TODO: Mejorar el chequeo de cuando esta listo el pipeline para empezar a aceptar clientes.
            time.sleep(5)
            if not self._wait_for_session_start():
                return

            batch_start = time.time()
            self._process_data_batches()
            batch_end = time.time()

            logging.info(
                f"action: batch_forward | session_id: {self.session_id} | "
                f"duration: {(batch_end - batch_start):.2f}s"
            )

        except NetworkError as e:
            logging.exception(f"action: handle_session | session_id: {self.session_id} | error: {e}")
            self._send_error_packet(500, str(e))
        except Exception as e:
            logging.exception(f"action: handle_session | session_id: {self.session_id} | error: {e}")
            self._send_error_packet(500, "internal server error")
        finally:
            # Clean up session if still in UPLOADING state (client disconnected early)
            from gateway.core.session import SessionState

            session_state = self.session_manager.get_session_state(self.session_id)
            if session_state == SessionState.UPLOADING:
                logging.info(f"action: cleanup_incomplete_session | session_id: {self.session_id}")
                self.session_manager.close_session(self.session_id)

    def _wait_for_session_start(self) -> bool:
        """Wait for FileSendStart packet."""
        packet = self.network.recv_packet()
        if packet is None:
            logging.warning(f"action: session_start | session_id: {self.session_id} | " f"result: connection_closed")
            return False

        if packet.get_message_type() != PacketType.FILE_SEND_START:
            logging.error(
                f"action: session_start | session_id: {self.session_id} | "
                f"result: invalid_packet | type: {packet.get_message_type()}"
            )
            self._send_error_packet(400, "expected FileSendStart")
            return False

        logging.info(f"action: session_start | session_id: {self.session_id} | result: success")
        self._send_session_id()
        self._send_ack_packet()
        return True

    def _process_data_batches(self):
        """
        Process incoming batch packets and route to appropriate queues.
        Tags all messages with session_id in RabbitMQ headers.
        """
        batch_count = 0
        eof_count = 0

        headers = {SESSION_ID: self.session_id.hex}

        while not self.shutdown_signal.should_shutdown():
            packet = self.network.recv_packet()
            if packet is None:
                logging.warning(
                    f"action: process_batches | session_id: {self.session_id} | " f"result: connection_closed"
                )
                break

            packet_type = packet.get_message_type()

            if packet_type == PacketType.FILE_SEND_END:
                logging.info(
                    f"action: process_batches | session_id: {self.session_id} | "
                    f"result: session_end | batches: {batch_count} | eofs: {eof_count}"
                )

                # CRITICAL: Transition state and get buffered results + queries needing EOF
                buffered, queries_needing_eof = self.session_manager.transition_to_ready_for_results(self.session_id)

                # Send ACK (protocol requires ACK after FILE_SEND_END)
                self._send_ack_packet()

                # Flush buffered results to client (lock-free)
                if buffered or queries_needing_eof:
                    self._flush_buffered_results(buffered, queries_needing_eof)

                break

            if packet_type == PacketType.BATCH:
                batch = cast(Batch, packet)
                try:
                    publisher = self.publishers.get(batch.entity_type)
                    if not publisher:
                        logging.debug(
                            f"action: route_batch | session_id: {self.session_id} | "
                            f"result: no_publisher | entity_type: {batch.entity_type.name} | "
                            f"transformer disabled"
                        )
                        continue

                    transformer_config = self.transformer_configs.get(batch.entity_type)
                    if not transformer_config:
                        logging.debug(
                            f"action: route_batch | session_id: {self.session_id} | "
                            f"result: no_config | entity_type: {batch.entity_type.name} | "
                            f"transformer disabled"
                        )
                        continue

                    if batch.eof:
                        self._route_batch_packet(EOF().serialize(), publisher, batch.entity_type, headers, eof=True)
                        eof_count += 1

                        self.session_manager.track_eof_received(self.session_id, batch.entity_type.name)

                        logging.debug(
                            f"action: route_eof | session_id: {self.session_id} | "
                            f"entity_type: {batch.entity_type.name}"
                        )
                    else:
                        self._route_batch_packet(batch.serialize(), publisher, batch.entity_type, headers)
                        batch_count += 1

                        if batch_count % 100 == 0:
                            logging.debug(
                                f"action: route_progress | session_id: {self.session_id} | " f"batches: {batch_count}"
                            )

                except Exception as e:
                    logging.error(
                        f"action: route_batch | session_id: {self.session_id} | " f"result: fail | error: {e}"
                    )
                    self._send_error_packet(500, "routing error")
                    break
            else:
                logging.warning(
                    f"action: process_batches | session_id: {self.session_id} | "
                    f"result: unexpected_packet | type: {packet_type}"
                )
                self._send_error_packet(400, f"unexpected packet type: {packet_type}")
                break

    def _route_batch_packet(
        self, batch: bytes, exchange: MessageMiddlewareExchangeRMQ, entity_type: EntityType, headers, eof=False
    ):
        batch_id = uuid.uuid4()

        if eof:
            key = "common"
        else:
            config = self.transformer_configs[entity_type]
            index = batch_id.int % config["replicas"]
            key = f"{config['downstream_stage']}_{index}"

        exchange.send(batch, key, headers | {MESSAGE_ID: batch_id.hex})

    def _send_ack_packet(self):
        """Send ACK packet to client."""
        try:
            ack_packet = AckPacket()
            self.network.send_packet(ack_packet)
        except NetworkError as e:
            logging.error(f"action: send_ack | session_id: {self.session_id} | error: {e}")

    def _send_session_id(self):
        """Send session_id to client."""
        try:
            from shared.protocol import SessionIdPacket

            session_packet = SessionIdPacket(self.session_id.int)
            self.network.send_packet(session_packet)
            logging.info(f"action: send_session_id | session_id: {self.session_id}")
        except NetworkError as e:
            logging.error(f"action: send_session_id | session_id: {self.session_id} | error: {e}")
            raise

    def _send_error_packet(self, error_code: int, message: str):
        """Send error packet to client."""
        try:
            error_packet = ErrorPacket(error_code, message)
            self.network.send_packet(error_packet)
        except NetworkError as e:
            logging.error(f"action: send_error | session_id: {self.session_id} | error: {e}")

    def _flush_buffered_results(self, buffered_results, queries_needing_eof):
        """
        Flush buffered results to client socket, then send EOF for each query.
        Called after state transition to READY_FOR_RESULTS.

        Args:
            buffered_results: List of (query_id, result_body) to send
            queries_needing_eof: Set of query_ids that need EOF packet sent
        """
        from shared.protocol import ResultPacket

        logging.info(
            f"action: flush_buffered_results | session_id: {self.session_id} | "
            f"results: {len(buffered_results)} | queries_needing_eof: {queries_needing_eof}"
        )

        try:
            # Send all buffered result packets
            for query_id, result_body in buffered_results:
                result_packet = ResultPacket(query_id, result_body)
                self.network.send_packet(result_packet)

            # Send EOF for each query that had buffered results
            for query_id in queries_needing_eof:
                eof_packet = ResultPacket(query_id, EOF().serialize())
                self.network.send_packet(eof_packet)
                logging.debug(f"action: flush_eof | session_id: {self.session_id} | query: {query_id}")

        except NetworkError as e:
            logging.error(f"action: flush_error | session_id: {self.session_id} | error: {e}")
            raise  # Propagate to handle_session error handling

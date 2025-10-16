"""
session management for gateway communication.
handles connection lifecycle and ACK protocol.
"""

import logging
import socket

from shared.network import Network
from shared.protocol import FileSendEnd, FileSendStart, PacketType
from shared.shutdown import ShutdownSignal


class Session:
    """manages tcp session with gateway."""

    def __init__(self, host: str, port: int, shutdown_signal: ShutdownSignal):
        self.host = host
        self.port = port
        self.shutdown_signal = shutdown_signal
        self.network = None
        self.session_id = None

    def connect(self):
        """establish tcp connection to gateway."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        self.network = Network(sock, self.shutdown_signal)
        logging.info(f"action: connect | gateway: {self.host}:{self.port}")

    def start(self):
        """send session start and wait for acknowledgment."""
        self.network.send_packet(FileSendStart())
        self._receive_session_id()
        self._wait_for_ack("session start")

    def end(self):
        """send session end and wait for acknowledgment."""
        self.network.send_packet(FileSendEnd())
        self._wait_for_ack("session end")

    def close(self):
        """close network connection."""
        if self.network:
            self.network.close()

    def _wait_for_ack(self, stage: str):
        """wait for ACK packet and handle errors."""
        ack_packet = self.network.recv_packet()
        if not ack_packet:
            raise Exception(f"connection lost while waiting for ACK for {stage}")
        elif ack_packet.get_message_type() == PacketType.ERROR:
            raise Exception(f"server error for {stage}: {ack_packet.message}")
        elif ack_packet.get_message_type() != PacketType.ACK:
            raise Exception(f"did not receive ACK for {stage}")

    def _receive_session_id(self):
        """receive session_id from gateway."""
        from uuid import UUID

        packet = self.network.recv_packet()
        if not packet:
            raise Exception("connection lost while waiting for session_id")

        if packet.get_message_type() != PacketType.SESSION_ID_PACKET:
            raise Exception(f"expected SESSION_ID_PACKET, got {packet.get_message_type()}")

        session_packet = packet
        session_uuid = UUID(int=session_packet.session_id_int)
        self.session_id = str(session_uuid)
        logging.info(f"action: session_id_received | session_id: {self.session_id}")

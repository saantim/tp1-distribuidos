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

    def connect(self):
        """establish tcp connection to gateway."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        self.network = Network(sock, self.shutdown_signal)
        logging.info(f"action: connect | gateway: {self.host}:{self.port}")

    def start(self):
        """send session start and wait for acknowledgment."""
        self.network.send_packet(FileSendStart())
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

"""
network transport layer for reliable packet transmission.
handles socket communication with automatic retry for short reads/writes.
supports graceful shutdown signaling for responsive network operations.
"""

import socket
from typing import Optional

from protocol import Header, Packet
from shutdown import ShutdownSignal


class NetworkError(Exception):
    """raised when network operations fail."""

    pass


class Network:
    """
    reliable packet transport over tcp sockets.
    handles short reads/writes and provides clean packet-based interface.
    supports graceful shutdown via signal handler.
    """

    def __init__(self, sock: socket.socket, signal: Optional[ShutdownSignal] = None):
        self.sock = sock
        self.signal = signal

    def send_packet(self, packet: Packet) -> None:
        """
        send a complete packet, handling short writes.
        raises NetworkError on failure or shutdown signal.
        """
        data = packet.serialize()
        self._send_all(data)

    def recv_packet(self) -> Optional[Packet]:
        """
        receive a complete packet, handling short reads.
        returns None on clean connection close.
        raises NetworkError on failure, malformed data, or shutdown signal.
        """
        # read header first
        header_data = self._recv_exact(Header.SIZE)
        if header_data is None:
            return None

        try:
            header = Header.deserialize(header_data)
        except Exception as e:
            raise NetworkError(f"invalid header: {e}")

        if header.payload_length > 0:
            payload_data = self._recv_exact(header.payload_length)
            if payload_data is None:
                raise NetworkError("connection closed while reading payload")
        else:
            payload_data = b""

        try:
            return Packet.deserialize(header, payload_data)
        except Exception as e:
            raise NetworkError(f"invalid packet: {e}")

    def _send_all(self, data: bytes) -> None:
        """send all bytes, handling short writes and shutdown signals."""
        total_sent = 0
        data_len = len(data)

        while total_sent < data_len:

            if self.signal and self.signal.should_shutdown():
                raise NetworkError("operation cancelled due to shutdown signal")

            try:
                sent = self.sock.send(data[total_sent:])
                if sent == 0:
                    raise NetworkError("connection closed during send")
                total_sent += sent
            except socket.error as e:
                raise NetworkError(f"send failed: {e}")

    def _recv_exact(self, size: int) -> Optional[bytes]:
        """receive exactly size bytes, handling short reads and shutdown signals."""
        if size == 0:
            return b""

        buffer = b""

        while len(buffer) < size:

            if self.signal and self.signal.should_shutdown():
                raise NetworkError("operation cancelled due to shutdown signal")

            try:
                chunk = self.sock.recv(size - len(buffer))
                if not chunk:
                    return None if len(buffer) == 0 else buffer  # partial read
                buffer += chunk
            except socket.error as e:
                raise NetworkError(f"recv failed: {e}")

        return buffer

    def close(self) -> None:
        """close the underlying socket."""
        try:
            self.sock.close()
        except socket.error:
            pass

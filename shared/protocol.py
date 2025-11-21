"""
binary serialization using ByteWriter and Reader utils.
"""

from abc import ABC, abstractmethod
from enum import IntEnum

from .utils import ByteReader, ByteWriter


SESSION_ID = "session_id"
MESSAGE_ID = "message_id"


class PacketType(IntEnum):
    ERROR = 0
    ACK = 1
    FILE_SEND_START = 2
    FILE_SEND_END = 3
    BATCH = 4
    SESSION_ID_PACKET = 5
    RESULT = 6
    HEARTBEAT = 7


class EntityType(IntEnum):
    STORE = 0
    USER = 1
    TRANSACTION = 2
    TRANSACTION_ITEM = 3
    MENU_ITEM = 4


class Header:
    """Packet header: message_type (1 byte) + payload_length (4 bytes)"""

    SIZE = 5

    def __init__(self, message_type: int, payload_length: int):
        self.message_type = message_type
        self.payload_length = payload_length

    def serialize(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint8(self.message_type)
        writer.write_uint32(self.payload_length)
        return writer.get_bytes()

    @classmethod
    def deserialize(cls, data: bytes) -> "Header":
        if len(data) != cls.SIZE:
            raise ValueError("Invalid header size")
        reader = ByteReader(data)
        message_type = reader.read_uint8()
        payload_length = reader.read_uint32()
        return cls(message_type, payload_length)


class Packet(ABC):
    """Base packet class"""

    @abstractmethod
    def get_message_type(self) -> int:
        pass

    @abstractmethod
    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    @abstractmethod
    def deserialize_payload(cls, data: bytes) -> "Packet":
        pass

    def serialize(self) -> bytes:
        payload = self.serialize_payload()
        header = Header(self.get_message_type(), len(payload))
        return header.serialize() + payload

    @classmethod
    def deserialize(cls, header: Header, payload: bytes) -> "Packet":
        packet_classes = {
            PacketType.FILE_SEND_START: FileSendStart,
            PacketType.FILE_SEND_END: FileSendEnd,
            PacketType.BATCH: Batch,
            PacketType.SESSION_ID_PACKET: SessionIdPacket,
            PacketType.RESULT: ResultPacket,
            PacketType.ACK: AckPacket,
            PacketType.ERROR: ErrorPacket,
            PacketType.HEARTBEAT: HeartbeatPacket,
        }

        packet_class = packet_classes.get(PacketType(header.message_type))
        if packet_class is None:
            raise ValueError(f"Unknown message type: {header.message_type}")

        return packet_class.deserialize_payload(payload)


class FileSendStart(Packet):
    def get_message_type(self) -> int:
        return PacketType.FILE_SEND_START

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "FileSendStart":
        return cls()


class FileSendEnd(Packet):
    def get_message_type(self) -> int:
        return PacketType.FILE_SEND_END

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "FileSendEnd":
        return cls()


class AckPacket(Packet):
    def get_message_type(self) -> int:
        return PacketType.ACK

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "AckPacket":
        return cls()


class ResultPacket(Packet):
    def __init__(self, query_id: str, data: bytes):
        """
        Result packet that can stream data for a specific query.
        """
        self.query_id = query_id
        self.data = data

    def get_message_type(self) -> int:
        return PacketType.RESULT

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_string(self.query_id)
        writer.write_uint32(len(self.data))
        writer.write_bytes(self.data)
        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ResultPacket":
        reader = ByteReader(data)
        query_id = reader.read_string()
        data_length = reader.read_uint32()
        result_data = reader.read_bytes(data_length)
        return cls(query_id, result_data)


class ErrorPacket(Packet):
    def __init__(self, error_code: int, message: str):
        self.error_code = error_code
        self.message = message

    def get_message_type(self) -> int:
        return PacketType.ERROR

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint32(self.error_code)
        writer.write_string(self.message)
        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ErrorPacket":
        reader = ByteReader(data)
        error_code = reader.read_uint32()
        message = reader.read_string()
        return cls(error_code, message)


class Batch(Packet):
    """
    Generic batch packet containing CSV row strings.
    Each row is a comma-separated string of values.
    """

    def __init__(self, entity_type: EntityType, csv_rows: list[str], eof: bool = False):
        """
        Args:
            entity_type: Type of entities in this batch
            csv_rows: List of CSV row strings (comma-separated values)
            eof: True if this is the last batch for this entity type
        """
        self.entity_type = entity_type
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.BATCH

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint8(self.entity_type)
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            writer.write_string(row)

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "Batch":
        reader = ByteReader(data)
        entity_type = EntityType(reader.read_uint8())
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        csv_rows = []
        for _ in range(row_count):
            csv_rows.append(reader.read_string())

        return cls(entity_type, csv_rows, eof)


class SessionIdPacket(Packet):
    """Packet that sends session_id from gateway to client as 128-bit integer."""

    def __init__(self, session_id_int: int):
        """
        Args:
            session_id_int: UUID as 128-bit integer (use uuid.int to convert)
        """
        self.session_id_int = session_id_int

    def get_message_type(self) -> int:
        return PacketType.SESSION_ID_PACKET

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint128(self.session_id_int)
        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "SessionIdPacket":
        reader = ByteReader(data)
        session_id_int = reader.read_uint128()
        return cls(session_id_int)


class HeartbeatPacket(Packet):
    """Heartbeat packet sent from workers to health checker."""

    def __init__(self, stage_name: str, worker_id: int, timestamp: float):
        self.stage_name = stage_name
        self.worker_id = worker_id
        self.timestamp = timestamp

    def get_message_type(self) -> int:
        return PacketType.HEARTBEAT

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_string(self.stage_name)
        writer.write_uint32(self.worker_id)
        writer.write_float64(self.timestamp)
        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "HeartbeatPacket":
        reader = ByteReader(data)
        stage_name = reader.read_string()
        worker_id = reader.read_uint32()
        timestamp = reader.read_float64()
        return cls(stage_name, worker_id, timestamp)

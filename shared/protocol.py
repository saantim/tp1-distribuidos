from abc import ABC, abstractmethod

from utils import ByteReader, ByteWriter


class MessageType:
    # batches
    STORE_BATCH = 1
    USERS_BATCH = 2
    TRANSACTIONS_BATCH = 3
    TRANSACTION_ITEMS_BATCH = 4
    MENU_ITEMS_BATCH = 5
    # control
    ACK = 20
    ERROR = 21


class Header:
    """
    Packet Header structure (5 Bytes total):
        - 1 Byte: message_type (uint8) - Identifies the packet type
        - 4 Bytes: payload_length (uint32) - Length of the following payload
    """

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
        if len(data) != Header.SIZE:
            raise ValueError("Invalid header size")

        reader = ByteReader(data)
        message_type = reader.read_uint8()
        payload_length = reader.read_uint32()

        return cls(message_type, payload_length)


class Packet(ABC):
    """Base Abstract Class for Packet Types."""

    @abstractmethod
    def get_message_type(self) -> int:
        """Gets the message type."""
        pass

    @abstractmethod
    def serialize_payload(self) -> bytes:
        """Serializes payload into Bytes."""
        pass

    @classmethod
    @abstractmethod
    def deserialize_payload(cls, data: bytes) -> "Packet":
        """Parses the bytes into a concrete Packet object."""
        pass

    def serialize(self) -> bytes:
        """Complete packet serialization including header and payload."""
        payload = self.serialize_payload()
        header = Header(self.get_message_type(), len(payload))
        return header.serialize() + payload

    @classmethod
    def deserialize(cls, header: Header, payload: bytes) -> "Packet":
        """Based on the header, deserializes it into a concrete Packet object."""
        packet_classes = {
            MessageType.STORE_BATCH: StoreBatch,
            MessageType.USERS_BATCH: UsersBatch,
            MessageType.TRANSACTIONS_BATCH: TransactionsBatch,
            MessageType.TRANSACTION_ITEMS_BATCH: TransactionItemsBatch,
            MessageType.MENU_ITEMS_BATCH: MenuItemsBatch,
            MessageType.ACK: AckPacket,
            MessageType.ERROR: ErrorPacket,
        }

        packet_class = packet_classes.get(header.message_type)
        if packet_class is None:
            raise ValueError(f"Unknown message type: {header.message_type}")

        return packet_class.deserialize_payload(payload)


class StoreBatch(Packet):
    """
    Store batch packet.
    Payload: row_count (4 bytes) + serialized store rows
    """

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_message_type(self) -> int:
        return MessageType.STORE_BATCH

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "StoreBatch":
        pass


class UsersBatch(Packet):
    """
    Users batch packet.
    Payload: row_count (4 bytes) + serialized user rows
    """

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_message_type(self) -> int:
        return MessageType.USERS_BATCH

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "UsersBatch":
        pass


class TransactionsBatch(Packet):
    """
    Transactions batch packet.
    Payload: row_count (4 bytes) + serialized transaction rows
    """

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_message_type(self) -> int:
        return MessageType.TRANSACTIONS_BATCH

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionsBatch":
        pass


class TransactionItemsBatch(Packet):
    """
    Transaction Items batch packet.
    Payload: row_count (4 bytes) + serialized transaction item rows
    """

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_message_type(self) -> int:
        return MessageType.TRANSACTION_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionItemsBatch":
        pass


class MenuItemsBatch(Packet):
    """
    Menu Items batch packet.
    Payload: row_count (4 bytes) + serialized menu item rows
    """

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_message_type(self) -> int:
        return MessageType.MENU_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "MenuItemsBatch":
        pass


class AckPacket(Packet):
    """
    Acknowledgment packet.
    Payload: empty (0 bytes)
    """

    def __init__(self):
        pass

    def get_message_type(self) -> int:
        return MessageType.ACK

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "AckPacket":
        pass


class ErrorPacket(Packet):
    """
    Error packet.
    Payload: error_code (4 bytes) + message_length (4 bytes) + message (N bytes)
    """

    def __init__(self, error_code: int, message: str):
        self.error_code = error_code
        self.message = message

    def get_message_type(self) -> int:
        return MessageType.ERROR

    def serialize_payload(self) -> bytes:
        pass

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ErrorPacket":
        pass

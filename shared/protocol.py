"""
binary serialization using python's struct module.

struct format strings used:
- ">B" = unsigned 8-bit integer, big endian
- ">I" = unsigned 32-bit integer, big endian
- ">Q" = unsigned 64-bit integer, big endian
- ">d" = 64-bit double precision float, big endian

the ">" prefix forces big endian byte order which ensures consistent
serialization across different platforms and architectures.
"""

import struct
from abc import ABC, abstractmethod
from datetime import datetime
from enum import IntEnum


class PacketType(IntEnum):
    # file control
    FILE_SEND_START = 0
    FILE_SEND_END = 1
    # batches
    STORE_BATCH = 2
    USERS_BATCH = 3
    TRANSACTIONS_BATCH = 4
    TRANSACTION_ITEMS_BATCH = 5
    MENU_ITEMS_BATCH = 6
    # control
    ACK = 20
    ERROR = 21


class Header:
    """
    packet header structure (5 bytes total):
    - message_type (>B) + payload_length (>I)
    """

    FORMAT = ">BI"
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, message_type: int, payload_length: int):
        self.message_type = message_type
        self.payload_length = payload_length

    def serialize(self) -> bytes:
        return struct.pack(self.FORMAT, self.message_type, self.payload_length)

    @classmethod
    def deserialize(cls, data: bytes) -> "Header":
        if len(data) != cls.SIZE:
            raise ValueError("Invalid header size")
        message_type, payload_length = struct.unpack(cls.FORMAT, data)
        return cls(message_type, payload_length)


class Packet(ABC):
    """base abstract class for packet types."""

    @abstractmethod
    def get_message_type(self) -> int:
        """gets the message type."""
        pass

    @abstractmethod
    def serialize_payload(self) -> bytes:
        """serializes payload into bytes."""
        pass

    @classmethod
    @abstractmethod
    def deserialize_payload(cls, data: bytes) -> "Packet":
        """parses the bytes into a concrete packet object."""
        pass

    def serialize(self) -> bytes:
        """complete packet serialization including header and payload."""
        payload = self.serialize_payload()
        header = Header(self.get_message_type(), len(payload))
        return header.serialize() + payload

    @classmethod
    def deserialize(cls, header: Header, payload: bytes) -> "Packet":
        """based on the header, deserializes it into a concrete packet object."""
        packet_classes = {
            PacketType.FILE_SEND_START: FileSendStart,
            PacketType.FILE_SEND_END: FileSendEnd,
            PacketType.STORE_BATCH: StoreBatch,
            PacketType.USERS_BATCH: UsersBatch,
            PacketType.TRANSACTIONS_BATCH: TransactionsBatch,
            PacketType.TRANSACTION_ITEMS_BATCH: TransactionItemsBatch,
            PacketType.MENU_ITEMS_BATCH: MenuItemsBatch,
            PacketType.ACK: AckPacket,
            PacketType.ERROR: ErrorPacket,
        }

        packet_class = packet_classes.get(PacketType(header.message_type))
        if packet_class is None:
            raise ValueError(f"Unknown message type: {header.message_type}")

        return packet_class.deserialize_payload(payload)


class FileSendStart(Packet):
    """file send start packet - empty payload"""

    def get_message_type(self) -> int:
        return PacketType.FILE_SEND_START

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "FileSendStart":
        return cls()


class FileSendEnd(Packet):
    """file send end packet - empty payload"""

    def get_message_type(self) -> int:
        return PacketType.FILE_SEND_END

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "FileSendEnd":
        return cls()


class StoreBatch(Packet):
    """
    store batch packet.

    header: row_count (>I) + eof (>B)
    per row: store_id (>B) + postal_code (>I) + latitude (>d) + longitude (>d)
             + variable strings: store_name, street, city, state
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BIdd"

    def __init__(self, rows: list[dict], eof: bool = False):
        self.rows = rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.STORE_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.rows), 1 if self.eof else 0)

        for row in self.rows:
            # fixed fields
            data += struct.pack(
                self.ROW_FIXED_FORMAT, row["store_id"], row["postal_code"], row["latitude"], row["longitude"]
            )
            # variable strings
            data += pack_string(row["store_name"])
            data += pack_string(row["street"])
            data += pack_string(row["city"])
            data += pack_string(row["state"])

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "StoreBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        rows = []
        for _ in range(row_count):
            # fixed fields
            store_id, postal_code, latitude, longitude = struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            # variable strings
            store_name, offset = unpack_string(data, offset)
            street, offset = unpack_string(data, offset)
            city, offset = unpack_string(data, offset)
            state, offset = unpack_string(data, offset)

            rows.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "street": street,
                    "postal_code": postal_code,
                    "city": city,
                    "state": state,
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )

        return cls(rows, eof == 1)


class UsersBatch(Packet):
    """
    users batch packet.

    header: row_count (>I) + eof (>B)
    per row: user_id (>I) + birthdate (>Q) + registered_at (>Q) + gender (variable string)
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">IQQ"

    def __init__(self, rows: list[dict], eof: bool = False):
        self.rows = rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.USERS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.rows), 1 if self.eof else 0)

        for row in self.rows:
            birthdate_ts = int(row["birthdate"].timestamp())
            registered_at_ts = int(row["registered_at"].timestamp())

            data += struct.pack(self.ROW_FIXED_FORMAT, row["user_id"], birthdate_ts, registered_at_ts)
            data += pack_string(row["gender"])

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "UsersBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        rows = []
        for _ in range(row_count):
            user_id, birthdate_ts, registered_at_ts = struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            gender, offset = unpack_string(data, offset)

            rows.append(
                {
                    "user_id": user_id,
                    "gender": gender,
                    "birthdate": datetime.fromtimestamp(birthdate_ts),
                    "registered_at": datetime.fromtimestamp(registered_at_ts),
                }
            )

        return cls(rows, eof == 1)


class TransactionsBatch(Packet):
    """
    transactions batch packet.

    header: row_count (>I) + eof (>B)
    per row: store_id (>B) + payment_method_id (>I) + voucher_flag (>B) + user_flag (>B)
             + amounts (>ddd) + created_at (>Q)
             + variable: transaction_id, optional voucher_id, optional user_id
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BIBB"  # store_id, payment_method_id, voucher_flag, user_flag
    AMOUNTS_FORMAT = ">dddQ"  # original, discount, final, created_at

    def __init__(self, rows: list[dict], eof: bool = False):
        self.rows = rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTIONS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.rows), 1 if self.eof else 0)

        for row in self.rows:
            has_voucher = 1 if row.get("voucher_id") else 0
            has_user = 1 if row.get("user_id") else 0

            data += struct.pack(self.ROW_FIXED_FORMAT, row["store_id"], row["payment_method_id"], has_voucher, has_user)

            data += pack_string(row["transaction_id"])

            if has_voucher:
                data += pack_string(row["voucher_id"])
            if has_user:
                data += struct.pack(">I", row["user_id"])

            created_at_ts = int(row["created_at"].timestamp())
            data += struct.pack(
                self.AMOUNTS_FORMAT, row["original_amount"], row["discount_applied"], row["final_amount"], created_at_ts
            )

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        rows = []
        for _ in range(row_count):
            store_id, payment_method_id, has_voucher, has_user = struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            transaction_id, offset = unpack_string(data, offset)

            voucher_id = None
            if has_voucher:
                voucher_id, offset = unpack_string(data, offset)

            user_id = None
            if has_user:
                user_id = struct.unpack_from(">I", data, offset)[0]
                offset += 4

            original, discount, final, created_at_ts = struct.unpack_from(cls.AMOUNTS_FORMAT, data, offset)
            offset += struct.calcsize(cls.AMOUNTS_FORMAT)

            rows.append(
                {
                    "transaction_id": transaction_id,
                    "store_id": store_id,
                    "payment_method_id": payment_method_id,
                    "voucher_id": voucher_id,
                    "user_id": user_id,
                    "original_amount": original,
                    "discount_applied": discount,
                    "final_amount": final,
                    "created_at": datetime.fromtimestamp(created_at_ts),
                }
            )

        return cls(rows, eof == 1)


class TransactionItemsBatch(Packet):
    """
    transaction items batch packet.

    header: row_count (>I) + eof (>B)
    per row: item_id (>B) + quantity (>I) + unit_price (>d) + subtotal (>d) + created_at (>Q)
             + variable: transaction_id
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BIddQ"

    def __init__(self, rows: list[dict], eof: bool = False):
        self.rows = rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTION_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.rows), 1 if self.eof else 0)

        for row in self.rows:
            created_at_ts = int(row["created_at"].timestamp())

            data += struct.pack(
                self.ROW_FIXED_FORMAT,
                row["item_id"],
                row["quantity"],
                row["unit_price"],
                row["subtotal"],
                created_at_ts,
            )
            data += pack_string(row["transaction_id"])

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionItemsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        rows = []
        for _ in range(row_count):
            item_id, quantity, unit_price, subtotal, created_at_ts = struct.unpack_from(
                cls.ROW_FIXED_FORMAT, data, offset
            )
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            transaction_id, offset = unpack_string(data, offset)

            rows.append(
                {
                    "transaction_id": transaction_id,
                    "item_id": item_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "subtotal": subtotal,
                    "created_at": datetime.fromtimestamp(created_at_ts),
                }
            )

        return cls(rows, eof == 1)


class MenuItemsBatch(Packet):
    """
    menu items batch packet.

    header: row_count (>I) + eof (>B)
    per row: item_id (>B) + price (>d) + is_seasonal (>B) + available_from (>d) + available_to (>d)
             + variable: item_name, category
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BdBdd"

    def __init__(self, rows: list[dict], eof: bool = False):
        self.rows = rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.MENU_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.rows), 1 if self.eof else 0)

        for row in self.rows:
            data += struct.pack(
                self.ROW_FIXED_FORMAT,
                row["item_id"],
                row["price"],
                1 if row["is_seasonal"] else 0,
                row["available_from"],
                row["available_to"],
            )
            data += pack_string(row["item_name"])
            data += pack_string(row["category"])

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "MenuItemsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        rows = []
        for _ in range(row_count):
            item_id, price, is_seasonal, available_from, available_to = struct.unpack_from(
                cls.ROW_FIXED_FORMAT, data, offset
            )
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            item_name, offset = unpack_string(data, offset)
            category, offset = unpack_string(data, offset)

            rows.append(
                {
                    "item_id": item_id,
                    "item_name": item_name,
                    "category": category,
                    "price": price,
                    "is_seasonal": is_seasonal == 1,
                    "available_from": available_from,
                    "available_to": available_to,
                }
            )

        return cls(rows, eof == 1)


class AckPacket(Packet):
    """acknowledgment packet - empty payload"""

    def get_message_type(self) -> int:
        return PacketType.ACK

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "AckPacket":
        return cls()


class ErrorPacket(Packet):
    """
    error packet.

    format: error_code (>I) + message (variable string)
    """

    FORMAT = ">I"

    def __init__(self, error_code: int, message: str):
        self.error_code = error_code
        self.message = message

    def get_message_type(self) -> int:
        return PacketType.ERROR

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.FORMAT, self.error_code)
        data += pack_string(self.message)
        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ErrorPacket":
        error_code = struct.unpack_from(cls.FORMAT, data, 0)[0]
        offset = struct.calcsize(cls.FORMAT)
        message, _ = unpack_string(data, offset)
        return cls(error_code, message)


def pack_string(s: str) -> bytes:
    """pack string as length (>B) + utf8_bytes"""
    encoded = s.encode("utf-8")
    return struct.pack(">B", len(encoded)) + encoded


def unpack_string(data: bytes, offset: int) -> tuple[str, int]:
    """unpack string and return (string, new_offset)"""
    length = struct.unpack_from(">B", data, offset)[0]
    offset += 1
    string = data[offset : offset + length].decode("utf-8")
    return string, offset + length

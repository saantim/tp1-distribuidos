"""
binary serialization using python's struct module.

struct format strings used:
- ">B" = unsigned 8-bit integer, big endian
- ">I" = unsigned 32-bit integer, big endian
- ">Q" = unsigned 64-bit integer, big endian
- ">q" = signed 64-bit integer, big endian
- ">d" = 64-bit double precision float, big endian

the ">" prefix forces big endian byte order which ensures consistent
serialization across different platforms and architectures.
"""

import struct
from abc import ABC, abstractmethod
from datetime import datetime
from enum import IntEnum
from typing import List

from .entity import MenuItem, Store, Transaction, TransactionItem, User


class PacketType(IntEnum):
    FILE_SEND_START = 0
    FILE_SEND_END = 1
    STORE_BATCH = 2
    USERS_BATCH = 3
    TRANSACTIONS_BATCH = 4
    TRANSACTION_ITEMS_BATCH = 5
    MENU_ITEMS_BATCH = 6
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

    def __init__(self, stores: List[Store], eof: bool = False):
        self.stores = stores
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.STORE_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.stores), 1 if self.eof else 0)

        for store in self.stores:
            data += struct.pack(
                self.ROW_FIXED_FORMAT, store.store_id, store.postal_code, store.latitude, store.longitude
            )
            data += _pack_string(store.store_name)
            data += _pack_string(store.street)
            data += _pack_string(store.city)
            data += _pack_string(store.state)

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "StoreBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        stores = []
        for _ in range(row_count):
            store_id, postal_code, latitude, longitude = struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            store_name, offset = _unpack_string(data, offset)
            street, offset = _unpack_string(data, offset)
            city, offset = _unpack_string(data, offset)
            state, offset = _unpack_string(data, offset)

            stores.append(
                Store(
                    store_id=store_id,
                    store_name=store_name,
                    street=street,
                    postal_code=postal_code,
                    city=city,
                    state=state,
                    latitude=latitude,
                    longitude=longitude,
                )
            )

        return cls(stores, eof == 1)


class UsersBatch(Packet):
    """
    users batch packet.

    header: row_count (>I) + eof (>B)
    per row: user_id (>I) + birthdate (>q) + registered_at (>Q) + gender (variable string)
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">IqQ"

    def __init__(self, users: List[User], eof: bool = False):
        self.users = users
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.USERS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.users), 1 if self.eof else 0)

        for user in self.users:
            birthdate_ts = int(user.birthdate.timestamp())
            registered_at_ts = int(user.registered_at.timestamp())

            data += struct.pack(self.ROW_FIXED_FORMAT, user.user_id, birthdate_ts, registered_at_ts)
            data += _pack_string(user.gender)

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "UsersBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        users = []
        for _ in range(row_count):
            user_id, birthdate_ts, registered_at_ts = struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            gender, offset = _unpack_string(data, offset)

            users.append(
                User(
                    user_id=user_id,
                    gender=gender,
                    birthdate=datetime.fromtimestamp(birthdate_ts),
                    registered_at=datetime.fromtimestamp(registered_at_ts),
                )
            )

        return cls(users, eof == 1)


class TransactionsBatch(Packet):
    """
    Transactions batch packet.

    header: row_count (>I) + eof (>B)
    per row: store_id (>B) + payment_method_id (>I) + voucher_id (>I) + user_id (>I)
             + amounts (>ddd) + created_at (>Q) + variable: transaction_id
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BIIIdddq"

    def __init__(self, transactions: List[Transaction], eof: bool = False):
        self.transactions = transactions
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTIONS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.transactions), 1 if self.eof else 0)

        for txn in self.transactions:
            created_at_ts = int(txn.created_at.timestamp())
            data += struct.pack(
                self.ROW_FIXED_FORMAT,
                txn.store_id,
                txn.payment_method_id,
                txn.voucher_id,
                txn.user_id,
                txn.original_amount,
                txn.discount_applied,
                txn.final_amount,
                created_at_ts,
            )
            data += _pack_string(txn.transaction_id)

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        transactions = []
        for _ in range(row_count):
            (store_id, payment_method_id, voucher_id, user_id, original, discount, final, created_at_ts) = (
                struct.unpack_from(cls.ROW_FIXED_FORMAT, data, offset)
            )
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            transaction_id, offset = _unpack_string(data, offset)

            transactions.append(
                Transaction(
                    transaction_id=transaction_id,
                    store_id=store_id,
                    payment_method_id=payment_method_id,
                    voucher_id=voucher_id,
                    user_id=user_id,
                    original_amount=original,
                    discount_applied=discount,
                    final_amount=final,
                    created_at=datetime.fromtimestamp(created_at_ts),
                )
            )

        return cls(transactions, eof == 1)


class TransactionItemsBatch(Packet):
    """
    transaction items batch packet.

    header: row_count (>I) + eof (>B)
    per row: item_id (>B) + quantity (>I) + unit_price (>d) + subtotal (>d) + created_at (>Q)
             + variable: transaction_id
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BIddq"

    def __init__(self, items: List[TransactionItem], eof: bool = False):
        self.items = items
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTION_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.items), 1 if self.eof else 0)

        for item in self.items:
            created_at_ts = int(item.created_at.timestamp())
            data += struct.pack(
                self.ROW_FIXED_FORMAT, item.item_id, item.quantity, item.unit_price, item.subtotal, created_at_ts
            )
            data += _pack_string(item.transaction_id)

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionItemsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        items = []
        for _ in range(row_count):
            item_id, quantity, unit_price, subtotal, created_at_ts = struct.unpack_from(
                cls.ROW_FIXED_FORMAT, data, offset
            )
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            transaction_id, offset = _unpack_string(data, offset)

            items.append(
                TransactionItem(
                    transaction_id=transaction_id,
                    item_id=item_id,
                    quantity=quantity,
                    unit_price=unit_price,
                    subtotal=subtotal,
                    created_at=datetime.fromtimestamp(created_at_ts),
                )
            )

        return cls(items, eof == 1)


class MenuItemsBatch(Packet):
    """
    menu items batch packet.

    header: row_count (>I) + eof (>B)
    per row: item_id (>B) + price (>d) + is_seasonal (>B) + available_from (>Q) + available_to (>Q)
             + variable strings: item_name, category
    """

    HEADER_FORMAT = ">IB"
    ROW_FIXED_FORMAT = ">BdBQQ"

    def __init__(self, items: List[MenuItem], eof: bool = False):
        self.items = items
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.MENU_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.items), 1 if self.eof else 0)

        for item in self.items:
            is_seasonal = 1 if item.is_seasonal else 0
            available_from_ts = int(item.available_from.timestamp())
            available_to_ts = int(item.available_to.timestamp())

            data += struct.pack(
                self.ROW_FIXED_FORMAT, item.item_id, item.price, is_seasonal, available_from_ts, available_to_ts
            )
            data += _pack_string(item.item_name)
            data += _pack_string(item.category)

        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "MenuItemsBatch":
        row_count, eof = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        offset = struct.calcsize(cls.HEADER_FORMAT)

        items = []
        for _ in range(row_count):
            (item_id, price, is_seasonal, available_from_ts, available_to_ts) = struct.unpack_from(
                cls.ROW_FIXED_FORMAT, data, offset
            )
            offset += struct.calcsize(cls.ROW_FIXED_FORMAT)

            item_name, offset = _unpack_string(data, offset)
            category, offset = _unpack_string(data, offset)

            items.append(
                MenuItem(
                    item_id=item_id,
                    item_name=item_name,
                    category=category,
                    price=price,
                    is_seasonal=is_seasonal == 1,
                    available_from=datetime.fromtimestamp(available_from_ts),
                    available_to=datetime.fromtimestamp(available_to_ts),
                )
            )

        return cls(items, eof == 1)


class AckPacket(Packet):
    """Acknowledgment packet - empty payload"""

    def get_message_type(self) -> int:
        return PacketType.ACK

    def serialize_payload(self) -> bytes:
        return b""

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "AckPacket":
        return cls()


class ErrorPacket(Packet):
    """Error packet with error code and message"""

    FORMAT = ">I"

    def __init__(self, error_code: int, message: str):
        self.error_code = error_code
        self.message = message

    def get_message_type(self) -> int:
        return PacketType.ERROR

    def serialize_payload(self) -> bytes:
        data = struct.pack(self.FORMAT, self.error_code)
        data += _pack_string(self.message)
        return data

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ErrorPacket":
        error_code = struct.unpack_from(cls.FORMAT, data, 0)[0]
        offset = struct.calcsize(cls.FORMAT)
        message, _ = _unpack_string(data, offset)
        return cls(error_code, message)


def _pack_string(s: str) -> bytes:
    """pack string with length variable (1 byte length limit: 255 chars max)"""
    encoded = s.encode("utf-8")
    if len(encoded) > 255:
        raise ValueError(f"String too long: {len(encoded)} bytes (max 255)")
    return struct.pack(">B", len(encoded)) + encoded


def _unpack_string(data: bytes, offset: int) -> tuple[str, int]:
    """unpack string with length variable"""
    length = struct.unpack_from(">B", data, offset)[0]
    offset += 1
    string = data[offset : offset + length].decode("utf-8")
    return string, offset + length

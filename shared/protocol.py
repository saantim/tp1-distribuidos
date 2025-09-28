"""
binary serialization using ByteWriter and Reader utils.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import IntEnum
from typing import Any, Dict, List

from .utils import ByteReader, ByteWriter


class PacketType(IntEnum):
    FILE_SEND_START = 0
    FILE_SEND_END = 1
    STORE_BATCH = 2
    USERS_BATCH = 3
    TRANSACTIONS_BATCH = 4
    TRANSACTION_ITEMS_BATCH = 5
    MENU_ITEMS_BATCH = 6
    RESULT = 19
    ACK = 20
    ERROR = 21


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
    def __init__(self, data: dict):
        self.data = data

    def get_message_type(self) -> int:
        return PacketType.RESULT

    def serialize_payload(self) -> bytes:
        return json.dumps(self.data).encode("utf-8")

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "ResultPacket":
        data = json.loads(data.decode("utf-8"))
        return cls(data)


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


class StoreBatch(Packet):

    # Estimated size:
    # Fixed: store_id(1) + postal_code(4) + lat(8) + lon(8) = 21
    # Variable strings with 1-byte length prefix:
    #   - store_name: ~25 chars avg + 1 = 26
    #   - street: ~20 chars avg + 1 = 21
    #   - city: ~15 chars avg + 1 = 16
    #   - state: ~18 chars avg + 1 = 19
    UNIT_SIZE = 21 + 26 + 21 + 16 + 19  # = 103

    def __init__(self, csv_rows: List[Dict[str, Any]], eof: bool = False):
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.STORE_BATCH

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            store_id = _safe_int(row.get("store_id"))
            postal_code = _safe_int(row.get("postal_code"))
            latitude = _safe_float(row.get("latitude"))
            longitude = _safe_float(row.get("longitude"))

            writer.write_uint8(store_id if 0 <= store_id <= 255 else 0)
            writer.write_uint32(postal_code)
            writer.write_float64(latitude)
            writer.write_float64(longitude)

            writer.write_string(_safe_str(row.get("store_name")))
            writer.write_string(_safe_str(row.get("street")))
            writer.write_string(_safe_str(row.get("city")))
            writer.write_string(_safe_str(row.get("state")))

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "StoreBatch":
        reader = ByteReader(data)
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        stores = []
        for _ in range(row_count):
            store_id = reader.read_uint8()

            if store_id == 0:
                store_id = None

            postal_code = reader.read_uint32()
            latitude = reader.read_float64()
            longitude = reader.read_float64()

            store_name = reader.read_string()

            if not store_name:
                store_name = None

            street = reader.read_string()
            city = reader.read_string()
            state = reader.read_string()

            stores.append(
                {
                    "store_id": store_id,
                    "postal_code": postal_code,
                    "latitude": latitude,
                    "longitude": longitude,
                    "store_name": store_name,
                    "street": street,
                    "city": city,
                    "state": state,
                }
            )

        return cls(stores, eof)


class UsersBatch(Packet):

    # Estimated size calculation:
    # Fixed: user_id(4) + birthdate(8) + registered_at(8) = 20
    # Variable strings with 1-byte length prefix:
    #   - gender: ~6 chars avg + 1 = 7
    UNIT_SIZE = 20 + 7  # = 27

    def __init__(self, csv_rows: List[Dict[str, Any]], eof: bool = False):
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.USERS_BATCH

    def serialize_payload(self) -> bytes:

        writer = ByteWriter()
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            user_id = _safe_int(row.get("user_id"))
            gender = _safe_str(row.get("gender"))
            birthdate_ts = _parse_date_timestamp(row.get("birthdate"))
            registered_ts = _parse_datetime_timestamp(row.get("registered_at"))

            writer.write_uint32(user_id)
            writer.write_int64(birthdate_ts)
            writer.write_uint64(registered_ts)
            writer.write_string(gender)

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "UsersBatch":
        reader = ByteReader(data)
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        users = []
        for _ in range(row_count):
            user_id = reader.read_uint32()

            if user_id == 0:
                user_id = None

            birthdate_ts = reader.read_int64()

            if birthdate_ts == 0:
                birthdate_ts = None

            registered_ts = reader.read_uint64()
            gender = reader.read_string()

            users.append(
                {
                    "user_id": user_id,
                    "gender": gender,
                    "birthdate": None if birthdate_ts is None else datetime.fromtimestamp(birthdate_ts),
                    "registered_at": None if registered_ts is None else datetime.fromtimestamp(registered_ts),
                }
            )

        return cls(users, eof)


class TransactionsBatch(Packet):

    # Estimated size calculation:
    # Fixed: store_id(1) + payment_id(4) + voucher_id(4) + user_id(4) + amounts(24) + created_at(8) = 45
    # Variable strings with 1-byte length prefix:
    #   - transaction_id: ~36 chars UUID + 1 = 37
    UNIT_SIZE = 45 + 37  # = 82

    def __init__(self, csv_rows: List[Dict[str, Any]], eof: bool = False):
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTIONS_BATCH

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            store_id = _safe_int(row.get("store_id"))
            payment_method_id = _safe_int(row.get("payment_method_id"))
            voucher_id = _safe_int(row.get("voucher_id"))
            user_id = _safe_int(row.get("user_id"))
            original_amount = _safe_float(row.get("original_amount"))
            discount_applied = _safe_float(row.get("discount_applied"))
            final_amount = _safe_float(row.get("final_amount"))
            created_ts = _parse_datetime_timestamp(row.get("created_at"))
            transaction_id = _safe_str(row.get("transaction_id"))

            writer.write_uint8(store_id if 0 <= store_id <= 255 else 0)
            writer.write_uint32(payment_method_id)
            writer.write_uint32(voucher_id)
            writer.write_uint32(user_id)
            writer.write_float64(original_amount)
            writer.write_float64(discount_applied)
            writer.write_float64(final_amount)
            writer.write_int64(created_ts)
            writer.write_string(transaction_id)

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionsBatch":
        reader = ByteReader(data)
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        transactions = []
        for _ in range(row_count):
            store_id = reader.read_uint8()
            if store_id == 0:
                store_id = None

            payment_method_id = reader.read_uint32()
            voucher_id = reader.read_uint32()

            user_id = reader.read_uint32()
            if user_id == 0:
                user_id = None

            original_amount = reader.read_float64()
            discount_applied = reader.read_float64()
            final_amount = reader.read_float64()
            created_ts = reader.read_int64()

            transaction_id = reader.read_string()
            if not transaction_id:
                transaction_id = None

            transactions.append(
                {
                    "transaction_id": transaction_id,
                    "store_id": store_id,
                    "payment_method_id": payment_method_id,
                    "voucher_id": voucher_id,
                    "user_id": user_id,
                    "original_amount": original_amount,
                    "discount_applied": discount_applied,
                    "final_amount": final_amount,
                    "created_at": datetime.fromtimestamp(created_ts),
                }
            )

        return cls(transactions, eof)


class TransactionItemsBatch(Packet):

    # Estimated size calculation:
    # Fixed: item_id(1) + quantity(4) + unit_price(8) + subtotal(8) + created_at(8) = 29
    # Variable strings with 1-byte length prefix:
    #   - transaction_id: ~36 chars UUID + 1 = 37
    UNIT_SIZE = 29 + 37  # = 66

    def __init__(self, csv_rows: List[Dict[str, Any]], eof: bool = False):
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.TRANSACTION_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            item_id = _safe_int(row.get("item_id"))
            quantity = _safe_int(row.get("quantity"))
            unit_price = _safe_float(row.get("unit_price"))
            subtotal = _safe_float(row.get("subtotal"))
            created_ts = _parse_datetime_timestamp(row.get("created_at"))
            transaction_id = _safe_str(row.get("transaction_id"))

            writer.write_uint8(item_id if 0 <= item_id <= 255 else 0)
            writer.write_uint32(quantity)
            writer.write_float64(unit_price)
            writer.write_float64(subtotal)
            writer.write_int64(created_ts)
            writer.write_string(transaction_id)

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "TransactionItemsBatch":
        reader = ByteReader(data)
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        items = []
        for _ in range(row_count):
            item_id = reader.read_uint8()
            quantity = reader.read_uint32()
            unit_price = reader.read_float64()
            subtotal = reader.read_float64()
            created_ts = reader.read_int64()
            transaction_id = reader.read_string()

            items.append(
                {
                    "transaction_id": transaction_id,
                    "item_id": item_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "subtotal": subtotal,
                    "created_at": datetime.fromtimestamp(created_ts),
                }
            )

        return cls(items, eof)


class MenuItemsBatch(Packet):

    # Estimated size calculation:
    # Fixed: item_id(1) + price(8) + is_seasonal(1) + available_from(8) + available_to(8) = 26
    # Variable strings with 1-byte length prefix:
    #   - item_name: ~12 chars avg + 1 = 13
    #   - category: ~8 chars avg + 1 = 9
    UNIT_SIZE = 26 + 13 + 9  # = 48

    def __init__(self, csv_rows: List[Dict[str, Any]], eof: bool = False):
        self.csv_rows = csv_rows
        self.eof = eof

    def get_message_type(self) -> int:
        return PacketType.MENU_ITEMS_BATCH

    def serialize_payload(self) -> bytes:
        writer = ByteWriter()
        writer.write_uint32(len(self.csv_rows))
        writer.write_uint8(1 if self.eof else 0)

        for row in self.csv_rows:
            item_id = _safe_int(row.get("item_id"))
            item_name = _safe_str(row.get("item_name"))
            category = _safe_str(row.get("category"))
            price = _safe_float(row.get("price"))
            is_seasonal = _safe_bool(row.get("is_seasonal"))
            available_from_ts = _parse_datetime_timestamp(row.get("available_from"))
            available_to_ts = _parse_datetime_timestamp(row.get("available_to"))

            writer.write_uint8(item_id if 0 <= item_id <= 255 else 0)
            writer.write_float64(price)
            writer.write_uint8(1 if is_seasonal else 0)
            writer.write_int64(available_from_ts)
            writer.write_int64(available_to_ts)
            writer.write_string(item_name)
            writer.write_string(category)

        return writer.get_bytes()

    @classmethod
    def deserialize_payload(cls, data: bytes) -> "MenuItemsBatch":
        reader = ByteReader(data)
        row_count = reader.read_uint32()
        eof = reader.read_uint8() == 1

        items = []
        for _ in range(row_count):
            item_id = reader.read_uint8()
            price = reader.read_float64()
            is_seasonal = reader.read_uint8() == 1
            available_from_ts = reader.read_uint64()
            available_to_ts = reader.read_uint64()
            item_name = reader.read_string()
            category = reader.read_string()

            items.append(
                {
                    "item_id": item_id,
                    "item_name": item_name,
                    "category": category,
                    "price": price,
                    "is_seasonal": is_seasonal,
                    "available_from": datetime.fromtimestamp(available_from_ts),
                    "available_to": datetime.fromtimestamp(available_to_ts),
                }
            )

        return cls(items, eof)


def _safe_str(value, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _safe_int(value, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_bool(value, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1")
    return bool(value)


def _parse_date_timestamp(date_str) -> int:
    """Parse date string (YYYY-MM-DD) to timestamp"""
    if not date_str:
        return 0  # EPOCH

    if isinstance(date_str, datetime):
        return int(date_str.timestamp())

    try:
        dt = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        return int(dt.timestamp())
    except ValueError:
        return 0  # EPOCH


def _parse_datetime_timestamp(datetime_str) -> int:
    """Parse datetime string (YYYY-MM-DD HH:MM:SS) to timestamp"""
    if not datetime_str:
        return 0  # EPOCH

    if isinstance(datetime_str, datetime):
        return int(datetime_str.timestamp())

    try:
        dt = datetime.strptime(str(datetime_str).strip(), "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except ValueError:
        return 0  # EPOCH

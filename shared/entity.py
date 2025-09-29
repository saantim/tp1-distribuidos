import json
from abc import ABC
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Dict, Set


_DATETIME_FIELDS: Dict[type, Set[str]] = {}


def _get_datetime_fields(cls) -> Set[str]:
    """cache which fields are datetime to avoid repeated introspection."""
    if cls not in _DATETIME_FIELDS:
        _DATETIME_FIELDS[cls] = {field.name for field in fields(cls) if field.type == datetime}
    return _DATETIME_FIELDS[cls]


@dataclass
class Message(ABC):

    def serialize(self) -> bytes:
        """Optimized serialization."""

        data = self.__dict__.copy()

        datetime_fields = _get_datetime_fields(self.__class__)
        for field_name in datetime_fields:
            if field_name in data and data[field_name] is not None:
                data[field_name] = data[field_name].isoformat()

        return json.dumps(data).encode()

    @classmethod
    def from_dict(cls, data: dict):
        """deserialization with cached field lookup."""
        datetime_fields = _get_datetime_fields(cls)

        for field_name in datetime_fields:
            if field_name in data:
                value = data[field_name]
                if isinstance(value, str):
                    data[field_name] = datetime.fromisoformat(value)
        return cls(**data)

    @classmethod
    def deserialize(cls, payload: bytes):
        data = json.loads(payload)
        return cls.from_dict(data)


@dataclass
class EOF(Message):
    pass


@dataclass
class Store(Message):
    store_id: int
    latitude: float
    longitude: float
    postal_code: int
    store_name: str
    street: str
    city: str
    state: str


@dataclass
class TransactionItem(Message):
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: datetime
    transaction_id: str


@dataclass
class MenuItem(Message):
    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: datetime
    available_to: datetime


@dataclass
class User(Message):
    user_id: int
    gender: str
    birthdate: datetime
    registered_at: datetime


@dataclass
class Transaction(Message):
    store_id: int
    payment_method_id: int
    voucher_id: int
    user_id: int
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: datetime
    transaction_id: str

import json
from abc import ABC
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Dict, NewType, Set


_DATETIME_FIELDS: Dict[type, Set[str]] = {}


def _get_datetime_fields(cls) -> Set[str]:
    """cache which fields are datetime to avoid repeated introspection."""
    if cls not in _DATETIME_FIELDS:
        _DATETIME_FIELDS[cls] = {field.name for field in fields(cls) if field.type in [Birthdate, CreatedAt, datetime]}
    return _DATETIME_FIELDS[cls]


@dataclass
class Message(ABC):

    def __str__(self):
        return asdict(self)

    def serialize(self) -> bytes:
        """Optimized serialization."""

        data = asdict(self)

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

        for field in fields(cls):
            if hasattr(field, "from_dict"):
                data[field.name] = field.from_dict(data[field.name])

        return cls(**data)

    @classmethod
    def deserialize(cls, payload: bytes):
        data = json.loads(payload)
        return cls.from_dict(data)

    @classmethod
    def is_type(cls, payload: bytes):
        try:
            data = json.loads(payload)
            cls.from_dict(data)
            return True
        except Exception as e:
            _ = e
            return False


@dataclass
class EOF(Message):
    pass


ItemId = NewType("ItemId", int)
ItemName = NewType("ItemName", str)


@dataclass
class MenuItem(Message):
    item_id: ItemId
    item_name: ItemName


StoreId = NewType("StoreId", int)
StoreName = NewType("StoreName", str)


@dataclass
class Store(Message):
    store_id: StoreId
    store_name: StoreName


Quantity = NewType("Quantity", int)
CreatedAt = NewType("CreatedAt", datetime)


@dataclass
class TransactionItem(Message):
    item_id: ItemId
    quantity: Quantity
    created_at: CreatedAt


UserId = NewType("UserId", int)
Birthdate = NewType("Birthdate", datetime)


@dataclass
class User(Message):
    user_id: UserId
    birthdate: Birthdate


TransactionId = NewType("TransactionId", str)
FinalAmount = NewType("FinalAmount", float)


@dataclass
class Transaction(Message):
    id: TransactionId
    store_id: StoreId
    user_id: UserId
    final_amount: FinalAmount
    created_at: CreatedAt

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, fields
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class Message(ABC):

    @abstractmethod
    def serialize(self) -> bytes:
        return json.dumps(asdict(self), cls=DateTimeEncoder).encode()

    @classmethod
    def from_dict(cls, data: dict):
        for field in fields(cls):
            if field.type == datetime and field.name in data:
                value = data[field.name]
                if isinstance(value, str):
                    data[field.name] = datetime.fromisoformat(value)
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

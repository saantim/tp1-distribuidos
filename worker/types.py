import json
from abc import ABC
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Message(ABC):

    @classmethod
    def from_dict(cls, data: dict):
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)

    @classmethod
    def deserialize(cls, payload: bytes):
        data = json.loads(payload)
        return cls.from_dict(data)


@dataclass
class EOF(Message):
    pass


# SOURCES


@dataclass
class Store(Message):
    store_id: int
    latitude: float
    longitude: float
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
    available_from_ts: datetime
    available_to_ts: datetime


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


# USER PURCHASE AGGREGATOR
@dataclass
class UserPurchasesOnStore(Message):
    user: User
    purchases: int


@dataclass
class UserPurchasesByStore(Message):
    """int being the store_id"""

    user_purchases_by_store: dict[int, list[UserPurchasesOnStore]]


# TOP 3 USERS AGGREGATOR
@dataclass
class Top3UsersPurchasesOnStore(Message):
    top_3: UserPurchasesByStore


# PERIOD AGGREGATOR
@dataclass
class TransactionItemByPeriod(Message):
    transaction_item_per_period: dict[str, int]

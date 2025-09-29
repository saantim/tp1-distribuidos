import json
from abc import ABC
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Dict, Set, NewType


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


StoreId = NewType("StoreId", int)
Latitude = NewType("Latitude", float)
Longitude = NewType("Longitude", float)
StoreName = NewType("StoreName", str)
Street = NewType("Street", str)
City = NewType("City", str)
State = NewType("State", str)


@dataclass
class Store(Message):
    store_id: StoreId
    latitude: Latitude
    longitude: Longitude
    store_name: StoreName
    street: Street
    city: City
    state: State


ItemId = NewType("ItemId", int)
Quantity = NewType("Quantity", int)
UnitPrice = NewType("UnitPrice", float)
Subtotal = NewType("Subtotal", float)
CreatedAt = NewType("CreatedAt", datetime)
TransactionId = NewType("TransactionId", str)


@dataclass
class TransactionItem(Message):
    item_id: ItemId
    quantity: Quantity
    unit_price: UnitPrice
    subtotal: Subtotal
    created_at: CreatedAt
    transaction_id: TransactionId


ItemName = NewType("ItemName", str)
Category = NewType("Category", str)
Price = NewType("Price", float)
IsSeasonal = NewType("IsSeasonal", bool)
AvailableFromTs = NewType("AvailableFromTs", datetime)
AvailableToTs = NewType("AvailableToTs", datetime)


@dataclass
class MenuItem(Message):
    item_id: ItemId
    item_name: ItemName
    category: Category
    price: Price
    is_seasonal: IsSeasonal
    available_from_ts: AvailableFromTs
    available_to_ts: AvailableToTs


# Strong types for each field
UserId = NewType("UserId", int)
Gender = NewType("Gender", str)
Birthdate = NewType("Birthdate", datetime)
RegisteredAt = NewType("RegisteredAt", datetime)


@dataclass
class User(Message):
    user_id: UserId
    gender: Gender
    birthdate: Birthdate
    registered_at: RegisteredAt


PaymentMethodId = NewType("PaymentMethodId", int)
VoucherId = NewType("VoucherId", int)
OriginalAmount = NewType("OriginalAmount", float)
DiscountApplied = NewType("DiscountApplied", float)
FinalAmount = NewType("FinalAmount", float)


@dataclass
class Transaction(Message):
    store_id: StoreId
    payment_method_id: PaymentMethodId
    voucher_id: VoucherId
    user_id: UserId
    original_amount: OriginalAmount
    discount_applied: DiscountApplied
    final_amount: FinalAmount
    created_at: CreatedAt
    transaction_id: TransactionId

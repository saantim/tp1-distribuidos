import json
import logging
from abc import ABC
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from typing import NewType


@dataclass
class Message(ABC):
    def serialize(self) -> bytes:
        representation = asdict(self)
        for field in fields(self):
            if field.type in [CreatedAt, AvailableFromTs, AvailableToTs]:
                representation[field.name] = representation[field.name].isoformat()
        return json.dumps(representation).encode()

    @classmethod
    def from_dict(cls, data: dict):
        for field in fields(cls):
            logging.info(f"Field {field} - name: {field.name} - type: {field.type} from dict {data}")
            if field.type in [CreatedAt, AvailableFromTs, AvailableToTs]:
                try:
                    data[field.name] = datetime.fromisoformat(data[field.name])
                except Exception:
                    logging.info(f"Field {field} - name: {field.name} - type: {field.type} from dict {data}")
        return cls(**data)

    @classmethod
    def deserialize(cls, payload: bytes) -> "Message":
        data = json.loads(payload)
        return cls.from_dict(data)


@dataclass
class EOF(Message):
    pass


# SOURCES

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


# USER PURCHASE AGGREGATOR
@dataclass
class UserPurchasesOnStore(Message):
    user: User
    purchases: int


@dataclass
class UserPurchasesByStore(Message):

    user_purchases_by_store: dict[int, list[UserPurchasesOnStore]]


# TOP 3 USERS AGGREGATOR
@dataclass
class Top3UsersPurchasesOnStore(Message):
    top_3: UserPurchasesByStore

TransactionAmount = NewType('TransactionAmount', int)
Period = NewType('Period', str)


# PERIOD AGGREGATOR (Q2)
@dataclass
class ItemSold(Message):
    amount: float
    quantity: int

@dataclass(unsafe_hash=True)
class ItemInfo(Message):
    item_id: ItemId
    item_name: ItemName

@dataclass
class TransactionItemByPeriod(Message):
    transaction_item_per_period: dict[Period, dict[ItemInfo, ItemSold]]
from dataclasses import dataclass
from typing import NewType

from shared.entity import Message, User, ItemId, ItemName



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
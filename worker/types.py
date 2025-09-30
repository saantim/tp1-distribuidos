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
class ItemInfo(Message):
    amount: float
    quantity: int
    item_name: ItemName

@dataclass
class TransactionItemByPeriod(Message):
    transaction_item_per_period: dict[Period, dict[ItemId, ItemInfo]]

    @classmethod
    def from_dict(cls, data: dict) -> "TransactionItemByPeriod":
        raw = data["transaction_item_per_period"]
        parsed: dict[Period, dict[ItemId, ItemInfo]] = {}
        for period_str, items_dict in raw.items():
            period = Period(period_str)
            parsed_items = {}
            for item_id_str, item_info_dict in items_dict.items():
                item_id = ItemId(item_id_str)
                item_info = ItemInfo(item_info_dict["amount"], item_info_dict["quantity"], ItemName(item_info_dict["item_name"]))
                parsed_items[item_id] = item_info
            parsed[period] = parsed_items
        return cls(transaction_item_per_period=parsed)

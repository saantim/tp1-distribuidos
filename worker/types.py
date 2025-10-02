from dataclasses import dataclass
from typing import NewType

from shared.entity import ItemId, ItemName, Message, StoreId, StoreName, UserId


# USER PURCHASE AGGREGATOR
@dataclass
class UserPurchasesInfo(Message):
    user: UserId
    birthday: str
    purchases: int
    store_name: StoreName


@dataclass
class UserPurchasesByStore(Message):
    user_purchases_by_store: dict[StoreId, dict[UserId, UserPurchasesInfo]]


# TOP 3 USERS AGGREGATOR
@dataclass
class Top3UsersPurchasesOnStore(Message):
    top_3: UserPurchasesByStore


TransactionAmount = NewType("TransactionAmount", int)
Period = NewType("Period", str)


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
                item_info = ItemInfo(
                    item_info_dict["amount"], item_info_dict["quantity"], ItemName(item_info_dict["item_name"])
                )
                parsed_items[item_id] = item_info
            parsed[period] = parsed_items
        return cls(transaction_item_per_period=parsed)


# SEMESTER AGGREGATOR (Q3)
@dataclass
class StoreInfo(Message):
    store_name: StoreName
    amount: float


Semester = NewType("Semester", str)


@dataclass
class SemesterTPVByStore(Message):
    semester_tpv_by_store: dict[Semester, dict[StoreId, StoreInfo]]

    @classmethod
    def from_dict(cls, data: dict) -> "SemesterTPVByStore":
        raw = data["semester_tpv_by_store"]
        parsed: dict[Semester, dict[StoreId, StoreInfo]] = {}
        for semester_str, stores_dict in raw.items():
            semester = Semester(semester_str)
            parsed_stores: dict[StoreId, StoreInfo] = {}
            for store_id_str, store_info_dict in stores_dict.items():
                store_id = StoreId(store_id_str)
                store_info = StoreInfo(
                    store_name=StoreName(store_info_dict["store_name"]),
                    amount=store_info_dict["amount"],
                )
                parsed_stores[store_id] = store_info
            parsed[semester] = parsed_stores
        return cls(semester_tpv_by_store=parsed)

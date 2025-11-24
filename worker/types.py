from typing import NewType
from shared.entity import ItemId, ItemName, Message, StoreId, StoreName, UserId


# USER PURCHASE AGGREGATOR
class UserPurchasesInfo(Message):
    user: UserId
    birthday: str
    purchases: int
    store_name: StoreName


class UserPurchasesByStore(Message):
    user_purchases_by_store: dict[StoreId, dict[UserId, UserPurchasesInfo]]

# PERIOD AGGREGATOR (Q2)
class ItemInfo(Message):
    amount: float
    quantity: int
    item_name: ItemName

Period = NewType("Period", str)

class TransactionItemByPeriod(Message):
    transaction_item_per_period: dict[Period, dict[ItemId, ItemInfo]]

# SEMESTER AGGREGATOR (Q3)
class StoreInfo(Message):
    store_name: StoreName
    amount: float


Semester = NewType("Semester", str)

class SemesterTPVByStore(Message):
    semester_tpv_by_store: dict[Semester, dict[StoreId, StoreInfo]]

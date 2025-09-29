from dataclasses import dataclass

from shared.entity import Message, User


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

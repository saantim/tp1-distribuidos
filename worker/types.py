from typing import NewType

from pydantic import model_serializer, model_validator

from shared.entity import ItemId, ItemName, Message, StoreId, StoreName, UserId


# USER PURCHASE AGGREGATOR
class UserPurchasesInfo(Message):
    user: UserId
    birthday: str
    purchases: int
    store_name: StoreName

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.user, self.birthday, self.purchases, self.store_name]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 4:
                raise ValueError
            return {"user": value[0], "birthday": value[1], "purchases": value[2], "store_name": value[3]}
        return value


class UserPurchasesByStore(Message):
    user_purchases_by_store: dict[StoreId, dict[UserId, UserPurchasesInfo]]

    @classmethod
    def build_from_v2(cls, v2:"UserPurchasesByStoreV2") -> "UserPurchasesByStore":
        v1 = UserPurchasesByStore(user_purchases_by_store={})
        for store_id in v2.user_purchases_by_store.keys():
            user_purchases_info = {}
            for user_id, purchases in v2.user_purchases_by_store[store_id].items():
                user_purchases_info[user_id] = UserPurchasesInfo(
                    user=user_id,
                    birthday = "",
                    purchases = purchases,
                    store_name=StoreName(""),
                )
            v1.user_purchases_by_store[store_id] = user_purchases_info
        return v1

Purchases = NewType("Purchases", int)

class UserPurchasesByStoreV2(Message):
    user_purchases_by_store: dict[StoreId, dict[UserId, Purchases]]

# PERIOD AGGREGATOR (Q2)
class ItemInfo(Message):
    amount: float
    quantity: int
    item_name: ItemName

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.amount, self.quantity, self.item_name]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 3:
                raise ValueError
            return {"amount": value[0], "quantity": value[1], "item_name": value[2]}
        return value

Period = NewType("Period", str)

class TransactionItemByPeriod(Message):
    transaction_item_per_period: dict[Period, dict[ItemId, ItemInfo]]

# SEMESTER AGGREGATOR (Q3)
class StoreInfo(Message):
    store_name: StoreName
    amount: float

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.store_name, self.amount]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 2:
                raise ValueError
            return {"store_name": value[0], "amount": value[1]}
        return value


Semester = NewType("Semester", str)

class SemesterTPVByStore(Message):
    semester_tpv_by_store: dict[Semester, dict[StoreId, StoreInfo]]

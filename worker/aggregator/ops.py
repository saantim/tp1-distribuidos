"""
Operations for aggregator workers using WAL storage.
"""

from typing import Literal

from shared.entity import ItemId, StoreId, UserId
from worker.storage.ops import BaseOp


class AggregateItemOp(BaseOp):
    """
    Aggregate item sales for a specific period.
    """

    type: Literal["aggregate_item"] = "aggregate_item"
    period: str
    item_id: ItemId
    quantity_delta: int
    amount_delta: float


class AggregateSemesterOp(BaseOp):
    """
    Aggregate store TPV (Total Purchase Value) for a specific semester.
    """

    type: Literal["aggregate_semester"] = "aggregate_semester"
    semester: str
    store_id: StoreId
    amount_delta: float


class IncrementUserPurchaseOp(BaseOp):
    """
    Increment user purchase count for a specific store.
    """

    type: Literal["increment_user_purchase"] = "increment_user_purchase"
    store_id: StoreId
    user_id: UserId
    increment: int = 1

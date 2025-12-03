"""
Typed operations for merger workers using WAL storage.

Each merger has its own operation type with a typed message field,
eliminating the need for string-based type resolution.
"""

from typing import Literal

from worker.session import BaseOp
from worker.types import (
    SemesterTPVByStore,
    TransactionItemByPeriod,
    UserPurchasesByStore,
)


class PeriodMergeOp(BaseOp):
    """Merge transaction items by period from upstream replica."""

    type: Literal["period_merge"] = "period_merge"
    message: TransactionItemByPeriod


class SemesterMergeOp(BaseOp):
    """Merge semester TPV by store from upstream replica."""

    type: Literal["semester_merge"] = "semester_merge"
    message: SemesterTPVByStore


class Top3MergeOp(BaseOp):
    """Merge top 3 user purchases by store from upstream replica."""

    type: Literal["top3_merge"] = "top3_merge"
    message: UserPurchasesByStore

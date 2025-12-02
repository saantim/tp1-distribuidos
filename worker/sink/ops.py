"""
Operations for sink workers using WAL storage.
"""

from typing import Literal

from worker.storage.ops import BaseOp


class AppendTransactionOp(BaseOp):
    """
    Append a transaction to Q1 sink results.
    """

    type: Literal["append_tx"] = "append_tx"
    transaction_id: str
    final_amount: float

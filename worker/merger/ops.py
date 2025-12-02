"""
Operations for merger workers using WAL storage.
"""

from typing import Any, Literal

import worker.types as worker_types
from shared import entity
from worker.storage.ops import BaseOp


class MergeOp(BaseOp):
    """
    Generic merge operation - stores full aggregated result from upstream.
    """

    type: Literal["merge"] = "merge"
    message_data: dict[str, Any]
    message_type: str


def message_from_op(op: MergeOp):
    """
    Reconstruct aggregated Message from operation data.
    """
    msg_cls = getattr(entity, op.message_type, None)
    if msg_cls is None:
        msg_cls = getattr(worker_types, op.message_type, None)

    if msg_cls is None:
        raise ValueError(f"Unknown message type: {op.message_type}")

    return msg_cls.model_validate(op.message_data)

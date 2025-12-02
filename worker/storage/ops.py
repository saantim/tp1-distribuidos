"""
Base operation types for WAL storage.
"""

from typing import Literal

from pydantic import BaseModel


class BaseOp(BaseModel):
    """Base class for all WAL operations."""

    type: str


class SysEofOp(BaseOp):
    """System operation marking EOF from a worker."""

    type: Literal["__sys_eof"] = "__sys_eof"
    worker_id: str


class SysMsgOp(BaseOp):
    """System operation marking message received (for duplicate detection)."""

    type: Literal["__sys_msg"] = "__sys_msg"
    msg_id: str

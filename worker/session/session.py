import uuid
from typing import Any, Literal, Optional, Type, TypeVar

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class BaseOp(BaseModel):
    """Base class for all WAL operations."""

    type: str

    @classmethod
    def get_type(cls) -> str:
        return cls.model_fields["type"].default


class SysEofOp(BaseOp):
    """System operation marking EOF from a worker."""

    type: Literal["__sys_eof"] = "__sys_eof"
    worker_id: str


class SysMsgOp(BaseOp):
    """System operation marking message received (for duplicate detection)."""

    type: Literal["__sys_msg"] = "__sys_msg"
    msg_id: str


class SysCommitOp(BaseOp):
    """System operation marking successful batch commit (for atomicity)."""

    type: Literal["__sys_commit"] = "__sys_commit"
    batch_id: str


class Session(BaseModel):
    """
    Session state for a worker.

    Tracks EOF markers from upstream workers, message IDs for duplicate detection,
    and application-specific storage data.
    """

    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: Optional[Any] = None

    def get_storage(self, data_type: Type[T]) -> T:
        """
        Deserialize storage field into a typed Pydantic model.

        If storage is already the correct type, returns it directly.
        Otherwise validates and converts from dict.
        """
        raw = self.storage

        if isinstance(raw, data_type):
            return raw

        if raw is None:
            obj = data_type()

        elif isinstance(raw, dict):
            obj = data_type.model_validate(raw)

        else:
            obj = data_type.model_validate(raw)

        self.storage = obj
        return obj

    def set_storage(self, storage: BaseModel) -> None:
        """Replace storage with a new Pydantic model."""
        self.storage = storage

    def add_eof(self, worker_id: str) -> None:
        """Mark that a worker has sent EOF for this session."""
        self.eof_collected.add(worker_id)

    def get_eof_collected(self) -> set[str]:
        """Return worker IDs that have sent EOF."""
        return self.eof_collected

    def add_msg_received(self, msg_id: str) -> None:
        """Register that a message has been processed (for duplicate detection)."""
        self.msgs_received.add(msg_id)

    def is_duplicated_msg(self, msg_id: str) -> bool:
        """Check if message has already been processed."""
        return msg_id in self.msgs_received

    def apply(self, op: Any) -> None:
        """
        Apply an operation to update session state.

        Handles system operations (EOF, message tracking).
        WAL storage uses this to record operations for persistence.
        """
        if isinstance(op, SysEofOp):
            self.add_eof(op.worker_id)
        elif isinstance(op, SysMsgOp):
            self.add_msg_received(op.msg_id)

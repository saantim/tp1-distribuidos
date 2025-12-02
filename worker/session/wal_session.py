from typing import Any, Callable, Optional

from pydantic import Field

from worker.session.base import Session
from worker.storage.ops import BaseOp, SysEofOp, SysMsgOp


class WALSession(Session):
    """
    Session subclass with Write-Ahead Log support via reducer pattern.

    Extends the base Session class with WAL-specific functionality:
    - pending_ops: Tracks operations since last persistence
    - reducer: Function that applies operations to storage state
    - bind_reducer(): Registers a reducer function for operation replay
    - apply(): Applies an operation via reducer and tracks for persistence

    The reducer pattern enables efficient event sourcing where only deltas
    (incremental changes) are persisted to the WAL instead of full state snapshots.
    """

    pending_ops: list[BaseOp] = Field(default_factory=list, exclude=True)
    reducer: Optional[Callable[[Any, BaseOp], Any]] = Field(default=None, exclude=True)

    def bind_reducer(self, reducer: Callable[[Any, BaseOp], Any]) -> None:
        """
        Bind a reducer function for applying operations to storage.
        """
        self.reducer = reducer

    def apply(self, op: BaseOp) -> None:
        """
        Apply operation to session via reducer and track for persistence.
        """
        if isinstance(op, SysEofOp):
            self.add_eof(op.worker_id)
        elif isinstance(op, SysMsgOp):
            self.add_msg_received(op.msg_id)
        else:
            # Apply custom op via reducer
            if self.reducer:
                self.storage = self.reducer(self.storage, op)

        # Track for persistence
        self.pending_ops.append(op)

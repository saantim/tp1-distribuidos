from abc import ABC, abstractmethod
from typing import Any, Optional

from shared.entity import Message
from worker.base import WorkerBase
from worker.session import BaseOp
from worker.session.ops import MergeOp, message_from_op
from worker.session.session import Session


class MergerBase(WorkerBase, ABC):
    @abstractmethod
    def merger_fn(self, merged: Optional[Message], payload: Message, session: Session) -> Message:
        pass

    def _start_of_session(self, session: Session):
        session_type = self.get_session_data_type()
        session.set_storage(session_type())

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(self.get_session_data_type())
        if session_data.merged is not None:
            self._send_message(messages=[session_data.merged], session_id=session.session_id)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data = session.get_storage(self.get_session_data_type())
        session_data.merged = self.merger_fn(session_data.merged, message, session)

    def _after_batch_processed(self, session: Session) -> None:
        pass

    def _merge_logic(self, merged: Optional[Message], message: Message) -> Message:
        """Domain merge logic without session dependency. Override in subclasses."""
        return message if merged is None else self._do_merge(merged, message)

    @abstractmethod
    def _do_merge(self, merged: Message, message: Message) -> Message:
        """Implement the actual merging logic. Called by _merge_logic."""
        pass

    def create_merge_reducer(self):
        """Generic factory to create reducer with bound merge logic. Works for all mergers."""

        def reducer(storage: Any, op: BaseOp) -> Any:
            if isinstance(op, MergeOp):
                if storage is None:
                    storage = self.get_session_data_type()()

                message = message_from_op(op)
                storage.merged = self._merge_logic(storage.merged, message)
                storage.message_count += 1

            return storage

        return reducer

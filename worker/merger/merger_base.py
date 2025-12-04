import uuid
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

from shared.entity import Message
from worker.base import Session, WorkerBase



class MergerBase(WorkerBase, ABC):
    @abstractmethod
    def merger_fn(self, merged: Optional[Message], payload: Message) -> None:
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
        session_data.merged = self.merger_fn(session_data.merged, message)

import uuid
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

from shared.entity import Message
from worker.base import Session, WorkerBase


TypedMSG = TypeVar("TypedMSG", bound=Message)


class SessionData(GenericModel, Generic[TypedMSG]):
    merged: Optional[TypedMSG] = None
    message_count: int = 0


class MergerBase(WorkerBase, ABC):
    @abstractmethod
    def merger_fn(self, merged: Optional[Message], payload: Message) -> None:
        pass

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage(SessionData)
        if session_data.merged is not None:
            self._send_message(messages=[session_data.merged], session_id=session.session_id, message_id=uuid.uuid4())

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage(SessionData)
        session_data.merged = self.merger_fn(session_data.merged, message)

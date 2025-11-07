import uuid
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from worker.base import WorkerBase, Session
from worker.packer import pack_entity_batch
from dataclasses import dataclass

@dataclass
class SessionData:
    merged: Optional[Message] = None
    message_count: int = 0

class MergerBase(WorkerBase, ABC):
    @abstractmethod
    def merger_fn(self, merged: Optional[Message], payload: Message) -> None:
        pass

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage()
        if session_data.merged is not None:
            packed: bytes = pack_entity_batch([session_data.merged])
            self._send_message(message=packed, session_id=session.session_id, message_id=uuid.uuid4())

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage()
        session_data.merged = self.merger_fn(session_data.merged, message)

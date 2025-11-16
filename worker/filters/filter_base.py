# worker/filters/filter_main.py
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from shared.entity import Message
from shared.middleware.interface import MessageMiddlewareExchange
from worker.base import Session, WorkerBase
from worker.packer import pack_entity_batch


@dataclass
class SessionData:
    buffer: list[Message] = field(default_factory=list)
    received: int = 0
    passed: int = 0


class FilterBase(WorkerBase, ABC):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        downstream_worker_quantity: int,
        source: MessageMiddlewareExchange,
        output: MessageMiddlewareExchange,
        batch_size: int = 500,
    ):
        super().__init__(instances, index, stage_name, downstream_worker_quantity, source, output)
        self.buffer_size = batch_size

    @abstractmethod
    def filter_fn(self, entity: Message) -> bool: ...

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage()
        logging.info(
            f"[{self._stage_name}] end_of_session: received_per_session={session_data.received},"
            f" pass={session_data.passed} session_id={session.session_id}"
        )
        self._flush_buffer(session)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage()
        session_data.received += 1

        if self.filter_fn(message):
            session_data.passed += 1
            session_data.buffer.append(message)

        if len(session_data.buffer) >= self.buffer_size:
            self._flush_buffer(session)

        if session_data.received % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] checkpoint: received_per_session={session_data.received},"
                f" pass={session_data.passed} session_id={session.session_id}"
            )

    def _flush_buffer(self, session: Session) -> None:
        """Flush buffer to all queues"""
        session_data: SessionData = session.get_storage()
        packed: bytes = pack_entity_batch(session_data.buffer)
        self._send_message(message=packed, session_id=session.session_id, message_id=uuid.uuid4())

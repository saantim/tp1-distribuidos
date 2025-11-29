# worker/filter/filter_main.py
import logging
from abc import ABC, abstractmethod

from pydantic.generics import GenericModel

from shared.entity import Message
from shared.middleware.interface import MessageMiddlewareExchange
from worker.base import Session, WorkerBase

class FilterBase(WorkerBase, ABC):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddlewareExchange,
        outputs: list,
        batch_size: int = 10_000,
    ):
        super().__init__(instances, index, stage_name, source, outputs)
        self.buffer_size = batch_size

    @abstractmethod
    def filter_fn(self, entity: Message) -> bool: ...

    def _start_of_session(self, session: Session):
        session_type = self.get_session_data_type()
        session.set_storage(session_type())

    def _end_of_session(self, session: Session):
        session_data = session.get_storage(self.get_session_data_type())
        logging.info(
            f"[{self._stage_name}] end_of_session: received_per_session={session_data.received}, "
            f"pass={session_data.passed} session_id={session.session_id.hex[:8]}"
        )
        self._flush_buffer(session)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data = session.get_storage(self.get_session_data_type())
        session_data.received += 1

        if self.filter_fn(message):
            session_data.passed += 1
            session_data.buffer.append(message)

        if len(session_data.buffer) >= self.buffer_size:
            self._flush_buffer(session)

        if session_data.received % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] {session_data.received//1000}k: pass={session_data.passed} | "
                f"session: {session.session_id.hex[:8]}"
            )

    def _flush_buffer(self, session: Session) -> None:
        """Flush buffer and send messages"""
        session_data = session.get_storage(self.get_session_data_type())
        if session_data.buffer:
            self._send_message(messages=session_data.buffer, session_id=session.session_id)
            session_data.buffer.clear()

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

from shared.entity import Message
from worker.base import Session, WorkerBase


TypedMSG = TypeVar("TypedMSG", bound=Message)


class SessionData(GenericModel, Generic[TypedMSG]):
    aggregated: Optional[TypedMSG] = None
    message_count: int = 0


class AggregatorBase(WorkerBase, ABC):
    def _start_of_session(self, session: Session):
        session.set_storage(self.get_session_data_type()())

    def _end_of_session(self, session: Session) -> None:
        session_data: SessionData = session.get_storage(self.get_session_data_type())

        if session_data.aggregated is None:
            return

        self._send_message(messages=[session_data.aggregated], session_id=session.session_id)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage(self.get_session_data_type())
        session_data.aggregated = self.aggregator_fn(session_data.aggregated, message)
        session_data.message_count += 1
        session.set_storage(session_data)
        if session_data.message_count % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] {session_data.message_count//1000}k aggregated | "
                f"session: {session.session_id.hex[:8]}"
            )

    @abstractmethod
    def aggregator_fn(self, aggregated: Optional[TypedMSG], message: TypedMSG) -> Message:
        pass

import logging
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from worker.base import Session, WorkerBase


class AggregatorBase(WorkerBase, ABC):
    def _start_of_session(self, session: Session):
        session_type = self.get_session_data_type()
        session.set_storage(session_type())

    def _end_of_session(self, session: Session) -> None:
        session_data = session.get_storage(self.get_session_data_type())

        if session_data.aggregated is None:
            return

        self._send_message(messages=[session_data.aggregated], session_id=session.session_id)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data = session.get_storage(self.get_session_data_type())
        session_data.aggregated = self.aggregator_fn(session_data.aggregated, message)
        session_data.message_count += 1
        if session_data.message_count % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] {session_data.message_count//1000}k aggregated | "
                f"session: {session.session_id.hex[:8]}"
            )

    @abstractmethod
    def aggregator_fn(self, aggregated: Optional[Message], message: Message) -> Message:
        pass

    def _after_batch_processed(self, session: Session) -> None:
        pass

import logging
import uuid
from abc import ABC, abstractmethod
from pydantic import BaseModel

from shared.entity import Message, RawMessage
from shared.middleware.interface import MessageMiddlewareExchange
from worker.base import Session, WorkerBase


class SessionData(BaseModel):
    result: list[Message] = []
    message_count: int = 0


class SinkBase(WorkerBase, ABC):
    """
    Collects results from pipeline, formats them using a query-specific function,
    and sends formatted results to the results exchange with by_stage_name routing.
    """

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddlewareExchange,
        outputs: list,
    ):
        super().__init__(instances, index, stage_name, source, outputs)
        self._results_per_session: dict[uuid.UUID, list[Message]] = {}

    @abstractmethod
    def format_fn(self, results_collected: list[Message]) -> RawMessage: ...

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage(SessionData)
        formatted_results: list[RawMessage] = [self.format_fn(session_data.result)]
        if formatted_results:
            self._send_message(formatted_results, session_id=session.session_id, message_id=uuid.uuid4())
            logging.info(
                f"action: sent_final_results | size: {len(formatted_results)} | session: {session.session_id.hex[:8]}"
            )
            session_data.result.clear()

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage(SessionData)
        session_data.result.append(message)

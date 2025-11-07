import logging
import uuid
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase, Session
from dataclasses import dataclass, field

@dataclass
class SessionData:
    result: list[Message] = field(default_factory=list)
    message_count: int = 0

class SinkBase(WorkerBase, ABC):
    """
    Collects results from pipeline, formats them using a query-specific function,
    and sends formatted results to the query-specific results queue.
    """

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        downstream_worker_quantity: int,
        source: MessageMiddlewareExchange,
        output: MessageMiddlewareExchange,
    ):
        super().__init__(instances, index, stage_name, downstream_worker_quantity, source, output)
        self._results_per_session: dict[uuid.UUID, list[Message]] = {}

    @abstractmethod
    def format_fn(self, results_collected: list[Message]) -> bytes: ...

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        session_data: SessionData = session.get_storage()
        formatted_results: bytes = self.format_fn(session_data.result)
        if formatted_results:
            # TODO: Modificar gateway para que no necesite el FINAL:true y tome en su lugar el EOF.
            self._send_message(formatted_results, session_id=session.session_id, message_id=uuid.uuid4())
            logging.info(
                f"action: sent_final_results | size: {len(formatted_results)} bytes | session: {session.session_id}")

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        session_data: SessionData = session.get_storage()
        session_data.result.append(message)

import logging
import uuid
from abc import ABC, abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase


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
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self._results_per_session: dict[uuid.UUID, list[Message]] = {}

    @abstractmethod
    def format_fn(self, results_collected: list[Message]) -> bytes: ...

    def _start_of_session(self, session_id: uuid.UUID):
        self._results_per_session[session_id] = []

    def _end_of_session(self, session_id: uuid.UUID):
        formatted_results: bytes = self.format_fn(self._results_per_session[session_id])
        if formatted_results:
            for output in self._output:
                output.send(formatted_results, headers={SESSION_ID: session_id.int})
            logging.info(f"Sent batch results ({len(formatted_results)} bytes) | session: {session_id}")

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        self._results_per_session[session_id].append(message)

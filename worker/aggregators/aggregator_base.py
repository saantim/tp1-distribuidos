import logging
import uuid
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch


class AggregatorBase(WorkerBase, ABC):
    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
    ) -> None:
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self._aggregated_per_session: dict[uuid.UUID, Optional[Message]] = {}
        self._message_count_per_session: dict[uuid.UUID, int] = {}

    def _start_of_session(self, session_id: uuid.UUID()):
        self._aggregated_per_session[session_id] = None
        self._message_count_per_session[session_id] = 0

    def _end_of_session(self, session_id: uuid.UUID) -> None:
        if self._aggregated_per_session[session_id] is None:
            return

        final = pack_entity_batch([self._aggregated_per_session[session_id]])

        for output in self._output:
            output.send(final, headers={SESSION_ID: session_id.int})
            logging.info(f"action: flushed_aggregation | to: {output} | session: {session_id}")

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        self._aggregated_per_session[session_id] = self.aggregator_fn(self._aggregated_per_session[session_id], message)
        self._message_count_per_session[session_id] += 1
        if self._message_count_per_session[session_id] % 100000 == 0:
            logging.info(f"Aggregated {self._message_count_per_session[session_id]} messages | session: {session_id}")

    @abstractmethod
    def aggregator_fn(self, aggregated: Optional[Message], message: Message) -> Message:
        pass

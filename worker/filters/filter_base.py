# worker/filters/filter_main.py
import logging
import uuid
from abc import ABC, abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch


class FilterBase(WorkerBase, ABC):
    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        batch_size: int = 500,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self.buffer_size = batch_size
        self.buffer_per_session: dict[uuid.UUID, list[Message]] = {}
        self.received_per_session: dict[uuid.UUID, int] = {}
        self.passed_per_session: dict[uuid.UUID, int] = {}

    @abstractmethod
    def filter_fn(self, entity: Message) -> bool: ...

    def _start_of_session(self, session_id: uuid.UUID):
        self.received_per_session[session_id] = 0
        self.passed_per_session[session_id] = 0
        self.buffer_per_session[session_id] = []

    def _end_of_session(self, session_id: uuid.UUID):
        self._flush_buffer(session_id)
        self.passed_per_session[session_id] = 0

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        self.received_per_session[session_id] += 1

        if self.filter_fn(message):
            self.passed_per_session[session_id] += 1
            self.buffer_per_session[session_id].append(message)

        if len(self.buffer_per_session[session_id]) >= self.buffer_size:
            self._flush_buffer(session_id)

        if self.received_per_session[session_id] % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] checkpoint: received_per_session={self.received_per_session[session_id]},"
                f" pass={self.passed_per_session[session_id]}"
            )

    def _flush_buffer(self, session_id: uuid.UUID) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch(self.buffer_per_session[session_id])
        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.hex})
        self.buffer_per_session[session_id].clear()

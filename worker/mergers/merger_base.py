import logging
import uuid
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch


class MergerBase(WorkerBase, ABC):
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
        self._merged_per_session: dict[uuid.UUID, Optional[Message]] = {}

    @abstractmethod
    def merger_fn(self, merged: Optional[Message], payload: Message) -> None:
        pass

    def _start_of_session(self, session_id: uuid.UUID):
        self._merged_per_session[session_id] = None

    def _end_of_session(self, session_id: uuid.UUID):
        if self._merged_per_session[session_id] is not None:
            self._flush_merged(session_id)

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        self._merged_per_session[session_id] = self.merger_fn(self._merged_per_session[session_id], message)

    def _flush_merged(self, session_id: uuid.UUID) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch([self._merged_per_session[session_id]])

        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.hex})
            logging.info(f"action: flushed_merge | to: {output} | session: {session_id} | size: {len(packed)}")

        self._merged_per_session[session_id] = None

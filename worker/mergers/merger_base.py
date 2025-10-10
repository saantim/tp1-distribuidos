from abc import ABC, abstractmethod
from typing import Optional, Type

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
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
        self._merged: Optional[Message] = None

    @abstractmethod
    def merger_fn(self, merged: Message, payload: Message) -> Message: ...

    def _end_of_session(self):
        self._flush_merged()

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self._merged = self.merger_fn(self._merged, message)

    def get_entity_type(self) -> Type[Message]:
        pass

    def _flush_merged(self) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch([self._merged])
        for output in self._output:
            output.send(packed)
        self._merged = None

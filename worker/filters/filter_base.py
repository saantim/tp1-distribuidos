# worker/filters/filter_main.py
import logging
from abc import ABC, abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
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
        self.buffer: list[Message] = []
        self.received = 0
        self.passed = 0

    @abstractmethod
    def filter_fn(self, entity: Message) -> bool: ...

    def _end_of_session(self):
        self._flush_buffer()

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self.received += 1
        if self.filter_fn(message):
            self.passed += 1
            self.buffer.append(message)

        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()

        if self.received % 100000 == 0:
            logging.info(f"[{self._stage_name}] checkpoint: received={self.received}, pass={self.passed}")

    def _flush_buffer(self) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch(self.buffer)
        for output in self._output:
            output.send(packed)
        self.buffer.clear()

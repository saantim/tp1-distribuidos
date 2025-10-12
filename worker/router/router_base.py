# worker/router/router_main.py
import logging
from abc import abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from worker.base import WorkerBase
from worker.packer import pack_entity_batch


class RouterBase(WorkerBase):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        routing_keys: list[str],
        batch_size: int = 500,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self.buffer_size = batch_size
        self.buffer_per_key: dict[str, list[Message]] = {}
        self.routed = 0
        self.routing_keys = routing_keys

    def _end_of_session(self):
        for routing_key in self.buffer_per_key.keys():
            self._flush_buffer(routing_key)

    @abstractmethod
    def router_fn(self, message: Message) -> str:
        pass

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self.routed += 1
        routing_key = self.router_fn(message)
        if routing_key not in self.buffer_per_key:
            self.buffer_per_key[routing_key] = []
        selected_buffer = self.buffer_per_key[routing_key]
        selected_buffer.append(message)
        if len(selected_buffer) >= self.buffer_size:
            self._flush_buffer(routing_key)

        if self.routed % 100000 == 0:
            logging.info(f"[{self._stage_name}] checkpoint: routed={self.routed}")

    def _flush_buffer(self, routing_key: str) -> None:
        """Flush buffer to all queues"""
        buffer = self.buffer_per_key[routing_key]
        packed: bytes = pack_entity_batch(buffer)
        for output in self._output:
            output.send(packed, routing_key)
        self.buffer_per_key[routing_key] = []

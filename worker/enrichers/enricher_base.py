import logging
from abc import ABC, abstractmethod
from typing import Optional, Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from worker.base import WorkerBase
from worker.packer import pack_entity_batch, unpack_entity_batch


class EnricherBase(WorkerBase, ABC):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        enricher_input: MessageMiddleware,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self._loaded_entities: Optional[Message] = None
        self._buffer: list[Message] = []
        self._buffer_size: int = 500
        self._enricher_input: MessageMiddleware = enricher_input

    def _end_of_session(self):
        self._flush_buffer()

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self._buffer.append(self._enrich_entity_fn(message))
        if len(self._buffer) >= self._buffer_size:
            self._flush_buffer()

    def _flush_buffer(self) -> None:
        """Flush buffer to all queues"""
        packed: bytes = pack_entity_batch(self._buffer)
        for output in self._output:
            output.send(packed)
        self._buffer.clear()

    @abstractmethod
    def _enrich_entity_fn(self, entity: Message) -> Message:
        pass

    @abstractmethod
    def _load_entity_fn(self, message: Message) -> None:
        pass

    @abstractmethod
    def get_enricher_type(self) -> Type[Message]:
        pass

    def _on_enricher_msg(self, channel, method, properties, body: bytes):
        try:
            if EOF.is_type(body):
                self._enricher_input.stop_consuming()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            for entity in unpack_entity_batch(body, self.get_enricher_type()):
                self._load_entity_fn(entity)

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error in enricher_msg: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start(self):
        self._enricher_input.start_consuming(self._on_enricher_msg)
        logging.info(f"action: finished_loading | stage: {self._stage_name}")
        super().start()

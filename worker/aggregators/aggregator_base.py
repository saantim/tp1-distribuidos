import logging
from abc import ABC, abstractmethod
from typing import Optional

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
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
        self._aggregated: Optional[Message] = None
        self._message_count = 0

    def _end_of_session(self):
        if self._aggregated is not None:
            final = pack_entity_batch([self._aggregated])
            for output in self._output:
                output.send(final)
                logging.info(f"action: flushed_aggregation | to: {output}")

    @abstractmethod
    def aggregator_fn(self, message: Message) -> None:
        pass

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self.aggregator_fn(message)
        self._message_count += 1
        if self._message_count % 100000 == 0:
            logging.info(f"Aggregated {self._message_count} messages")

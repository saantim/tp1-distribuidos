"""
Transformer base module that extends WorkerBase.
Transforms CSV rows into entities.
"""

import logging
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from worker.base import WorkerBase
from worker.packer import is_raw_batch, pack_entity_batch, unpack_raw_batch


class TransformerBase(WorkerBase, ABC):
    """
    Base class for transformers that convert CSV rows into entities.
    Subclasses must implement parse_csv_row, create_entity, and get_entity_type.
    """

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
        self.transformed = 0

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        """
        Override to handle both EOF messages and raw Batch packets.

        Args:
            channel: RabbitMQ channel
            method: Delivery method
            properties: Message properties
            body: Message bytes (either EOF or raw Batch packet)
        """
        if not self._session_active:
            self._session_active = True

        if self._handle_eof(body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if is_raw_batch(body):
            try:
                for csv_row in unpack_raw_batch(body):
                    self._on_csv_row(csv_row)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"action: batch_process | stage: {self._stage_name} | error: {str(e)}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            logging.warning(f"action: unknown_message | stage: {self._stage_name} | message not EOF or Batch")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_csv_row(self, csv_row: str) -> None:
        """
        Process a single CSV row.

        Args:
            csv_row: CSV row as string
        """
        try:
            row_dict = self.parse_csv_row(csv_row)

            entity = self.create_entity(row_dict)

            self.transformed += 1
            self.buffer.append(entity)

            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()

            if self.transformed % 100000 == 0:
                logging.info(f"[{self._stage_name}] checkpoint: transformed={self.transformed}")

        except ValueError as e:
            logging.warning(
                f"action: transform_entity | stage: {self._stage_name} | " f"error: {str(e)} | csv_row: {csv_row}"
            )
        except Exception as e:
            logging.error(f"action: transform_entity | stage: {self._stage_name} | " f"error: {str(e)}")
            raise

    def _on_entity_upstream(self, channel, method, properties, body) -> None:
        """
        Not used in transformers - we override _on_message_upstream instead.
        This is here to satisfy the abstract method requirement.
        """
        pass

    def _end_of_session(self):
        """
        Called when session ends (after receiving EOF from all upstream workers).
        Flush remaining buffered entities.
        """
        self._flush_buffer()
        logging.info(f"action: end_of_session | stage: {self._stage_name} | " f"total_transformed: {self.transformed}")

    def _flush_buffer(self) -> None:
        """Flush buffer to all output queues."""
        if not self.buffer:
            return

        packed: bytes = pack_entity_batch(self.buffer)
        for output in self._output:
            output.send(packed)
        self.buffer.clear()

    @abstractmethod
    def parse_csv_row(self, csv_row: str) -> dict:
        """
        Parse CSV row string into dictionary.

        Args:
            csv_row: CSV row as string

        Returns:
            Dictionary with parsed values

        Raises:
            ValueError: If CSV row format is invalid
        """
        pass

    @abstractmethod
    def create_entity(self, row_dict: dict) -> Message:
        """
        Create entity from parsed row dictionary.

        Args:
            row_dict: Dictionary with parsed values

        Returns:
            Entity instance
        """
        pass

    @abstractmethod
    def get_entity_type(self) -> Type[Message]:
        """
        Returns the entity type produced by this transformer.

        Returns:
            Entity class type
        """
        pass

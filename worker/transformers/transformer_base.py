"""
Transformer base module that extends WorkerBase.
Transforms CSV rows into entities.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import is_raw_batch, pack_entity_batch, unpack_raw_batch


class TransformerBase(WorkerBase, ABC):
    """
    Base class for transformers that convert CSV rows into entities.
    Subclasses must implement parse_fn, create_fn, and get_entity_type.
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
        self._buffer_per_session: dict[uuid.UUID, list[Message]] = {}
        self._transformed_per_session: dict[uuid.UUID, int] = {}

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        """
        Override to handle both EOF messages and raw Batch packets.

        Args:
            channel: RabbitMQ channel
            method: Delivery method
            properties: Message properties
            body: Message bytes (either EOF or raw Batch packet)
        """
        session_id: uuid.UUID = uuid.UUID(int=properties.headers.get(SESSION_ID))

        if session_id not in self._active_sessions:
            self._active_sessions.add(session_id)
            self._eof_collected_by_session[session_id] = set()
            self._start_of_session(session_id)

        if self._handle_eof(body, session_id):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if is_raw_batch(body):
            try:
                for csv_row in unpack_raw_batch(body):
                    self._on_csv_row(csv_row, session_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(
                    f"action: batch_process | stage: {self._stage_name} |"
                    f" error: {str(e)} | session_id: {session_id}"
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            logging.warning(
                f"action: unknown_message |"
                f" stage: {self._stage_name} | message not EOF or Batch | session_id: {session_id}"
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_csv_row(self, csv_row: str, session_id: uuid.UUID) -> None:
        """
        Process a single CSV row.

        Args:
            csv_row: CSV row as string
        """
        try:
            row_dict = self.parse_fn(csv_row)
            entity = self.create_fn(row_dict)

            self._transformed_per_session[session_id] += 1
            self._buffer_per_session[session_id].append(entity)

            if len(self._buffer_per_session[session_id]) >= self.buffer_size:
                self._flush_buffer(session_id)

            if self._transformed_per_session[session_id] % 100000 == 0:
                logging.info(
                    f"[{self._stage_name}] checkpoint:"
                    f" transformed={self._transformed_per_session[session_id]}"
                    f" session_id={session_id}"
                )

        except ValueError as e:
            logging.warning(
                f"action: transform_entity | stage: {self._stage_name} | "
                f"error: {str(e)} |"
                f" csv_row: {csv_row} | session_id: {session_id}"
            )
        except Exception as e:
            logging.error(
                f"action: transform_entity | stage: {self._stage_name} |"
                f" "
                f"error: {str(e)} | session_id: {session_id}"
            )
            raise

    def _on_entity_upstream(self, body, session_id: uuid.UUID) -> None:
        """
        Not used in transformers - we override _on_message_upstream instead.
        This is here to satisfy the abstract method requirement.
        """
        pass

    def _start_of_session(self, session_id: uuid.UUID):
        self._buffer_per_session[session_id] = []
        self._transformed_per_session[session_id] = 0

    def _end_of_session(self, session_id: uuid.UUID):
        """
        Called when session ends (after receiving EOF from all upstream workers).
        Flush remaining buffered entities.
        """
        self._flush_buffer(session_id)
        transformed = self._transformed_per_session[session_id]
        logging.info(
            f"action: end_of_session | stage: {self._stage_name} |"
            f" "
            f"total_transformed: {transformed} | session_id: {session_id}"
        )

    def _flush_buffer(self, session_id: uuid.UUID) -> None:
        """Flush buffer to all output queues."""
        if not self._buffer_per_session[session_id]:
            return

        packed: bytes = pack_entity_batch(self._buffer_per_session[session_id])
        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id})
        self._buffer_per_session[session_id].clear()

    @abstractmethod
    def parse_fn(self, csv_row: str) -> dict:
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
    def create_fn(self, row_dict: dict) -> Message:
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

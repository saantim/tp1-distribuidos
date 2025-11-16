"""
Transformer base module that extends WorkerBase.
Transforms CSV rows into entities.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Type

from shared.entity import Message, RawMessage
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from worker.base import Session, WorkerBase
from worker.packer import is_raw_batch, pack_entity_batch, unpack_raw_batch


@dataclass
class SessionData:
    buffer: list[Message] = field(default_factory=list)
    transformed: int = 0


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

    def get_entity_type(self) -> Type[Message]:
        return RawMessage

    def _on_entity_upstream(self, message: RawMessage, session: Session) -> None:
        if is_raw_batch(message.raw_data):
            for csv_row in unpack_raw_batch(message.raw_data):
                self._on_csv_row(csv_row, session)
            return

        logging.warning(
            f"action: unknown_message |"
            f" stage: {self._stage_name} | message not EOF or Batch | session_id: {session.session_id}"
        )

    def _start_of_session(self, session: Session):
        session.set_storage(SessionData())

    def _end_of_session(self, session: Session):
        """
        Called when session ends (after receiving EOF from all upstream workers).
        Flush remaining buffered entities.
        """
        session_data: SessionData = session.get_storage()
        self._flush_buffer(session)
        logging.info(
            f"action: end_of_session | stage: {self._stage_name} |"
            f" "
            f"total_transformed: {session_data.transformed} | session_id: {session.session_id}"
        )

    def _flush_buffer(self, session: Session) -> None:
        """Flush buffer to all output queues."""
        session_data: SessionData = session.get_storage()
        if not session_data.buffer:
            return

        packed: bytes = pack_entity_batch(session_data.buffer)
        self._send_message(message=packed, session_id=session.session_id, message_id=uuid.uuid4())
        session_data.buffer.clear()

    def _on_csv_row(self, csv_row: str, session: Session) -> None:
        """
        Process a single CSV row.

        Args:
            csv_row: CSV row as string
        """
        try:
            session_data: SessionData = session.get_storage()
            row_dict = self.parse_fn(csv_row)
            entity = self.create_fn(row_dict)

            session_data.transformed += 1
            session_data.buffer.append(entity)

            if len(session_data.buffer) >= self.buffer_size:
                self._flush_buffer(session)

            if session_data.transformed % 100000 == 0:
                logging.info(
                    f"[{self._stage_name}] checkpoint:"
                    f" transformed={session_data.transformed}, "
                    f" session_id={session.session_id}"
                )

        except ValueError as e:
            logging.warning(
                f"action: transform_entity | stage: {self._stage_name} | "
                f"error: {str(e)} |"
                f" csv_row: {csv_row} | session_id: {session.session_id}"
            )
            raise e
        except Exception as e:
            logging.error(
                f"action: transform_entity | stage: {self._stage_name} |"
                f" "
                f"error: {str(e)} | session_id: {session.session_id}"
            )
            raise e

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

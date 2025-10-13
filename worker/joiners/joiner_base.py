import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch, unpack_entity_batch


class JoinerBase(WorkerBase, ABC):

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        reference_source: MessageMiddleware,
        batch_size: int = 500,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)

        self._reference_source: MessageMiddleware = reference_source
        self._reference_thread = None

        self._reference_data_per_session: dict[uuid.UUID, dict] = {}
        self._reference_ready_per_session: dict[uuid.UUID, bool] = {}
        self._pending_entities_per_session: dict[uuid.UUID, list[Message]] = {}
        self._output_buffer_per_session: dict[uuid.UUID, list[Message]] = {}
        self._batch_size: int = batch_size

        self._reference_count_per_session: dict[uuid.UUID, int] = {}
        self._pending_count_per_session: dict[uuid.UUID, int] = {}
        self._enriched_count_per_session: dict[uuid.UUID, int] = {}

    def _on_reference_message(self, channel, method, properties, body: bytes):
        """
        Callback for reference input (reference data).
        Runs in separate thread.
        """
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))

        try:
            if session_id not in self._reference_data_per_session:
                self._reference_data_per_session[session_id] = {}
                self._reference_ready_per_session[session_id] = False
                self._reference_count_per_session[session_id] = 0

            if EOF.is_type(body):
                self._reference_ready_per_session[session_id] = True
                logging.info(
                    f"action: reference_data_ready | stage: {self._stage_name} | "
                    f"session: {session_id} | count: {self._reference_count_per_session[session_id]}"
                )

                pending = self._pending_entities_per_session.get(session_id, [])
                logging.info(
                    f"action: processing_pending | stage: {self._stage_name} | "
                    f"session: {session_id} | pending_count: {len(pending)}"
                )

                for entity in pending:
                    enriched = self._join_entity_fn(self._reference_data_per_session[session_id], entity)
                    if session_id not in self._output_buffer_per_session:
                        self._output_buffer_per_session[session_id] = []
                    self._output_buffer_per_session[session_id].append(enriched)

                    if len(self._output_buffer_per_session[session_id]) >= self._batch_size:
                        self._flush_output_buffer(session_id)

                self._pending_entities_per_session[session_id] = []
                self._pending_count_per_session[session_id] = 0

                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            for entity in unpack_entity_batch(body, self.get_reference_type()):
                self._reference_data_per_session[session_id] = self._load_reference_fn(
                    self._reference_data_per_session[session_id], entity
                )
                self._reference_count_per_session[session_id] += 1

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.exception(
                f"action: reference_msg_error | stage: {self._stage_name} | " f"session: {session_id} | error: {e}"
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_entity_upstream(self, entity: Message, session_id: uuid.UUID) -> None:
        """
        Process an entity from main input.
        If reference data is not ready, buffer the entity.
        """
        if not self._reference_ready_per_session.get(session_id, False):
            if session_id not in self._pending_entities_per_session:
                self._pending_entities_per_session[session_id] = []
                self._pending_count_per_session[session_id] = 0

            self._pending_entities_per_session[session_id].append(entity)
            self._pending_count_per_session[session_id] += 1

            if self._pending_count_per_session[session_id] % 10000 == 0:
                logging.info(
                    f"action: buffering_pending | stage: {self._stage_name} | "
                    f"session: {session_id} | pending: {self._pending_count_per_session[session_id]}"
                )
            return

        enriched = self._join_entity_fn(self._reference_data_per_session[session_id], entity)

        if session_id not in self._output_buffer_per_session:
            self._output_buffer_per_session[session_id] = []
        if session_id not in self._enriched_count_per_session:
            self._enriched_count_per_session[session_id] = 0

        self._output_buffer_per_session[session_id].append(enriched)
        self._enriched_count_per_session[session_id] += 1

        if len(self._output_buffer_per_session[session_id]) >= self._batch_size:
            self._flush_output_buffer(session_id)

        if self._enriched_count_per_session[session_id] % 10000 == 0:
            logging.info(
                f"action: enriching | stage: {self._stage_name} | "
                f"session: {session_id} | enriched: {self._enriched_count_per_session[session_id]}"
            )

    def _start_of_session(self, session_id: uuid.UUID):
        """Hook when a new session starts."""
        logging.info(f"action: session_start | stage: {self._stage_name} | session: {session_id}")
        # State is initialized lazily in callbacks

    def _end_of_session(self, session_id: uuid.UUID):
        """Flush final output and cleanup when a session ends."""
        self._flush_output_buffer(session_id)

        # Cleanup session state
        self._output_buffer_per_session.pop(session_id, None)
        self._pending_entities_per_session.pop(session_id, None)
        self._reference_data_per_session.pop(session_id, None)
        self._reference_ready_per_session.pop(session_id, None)
        self._reference_count_per_session.pop(session_id, None)
        self._pending_count_per_session.pop(session_id, None)
        self._enriched_count_per_session.pop(session_id, None)

        logging.info(f"action: session_end | stage: {self._stage_name} | session: {session_id}")

    def _flush_output_buffer(self, session_id: uuid.UUID) -> None:
        """Send accumulated buffer to all output queues."""
        buffer = self._output_buffer_per_session.get(session_id, [])

        if not buffer:
            return

        packed: bytes = pack_entity_batch(buffer)

        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.hex})

        count = len(buffer)
        logging.info(
            f"action: flush_output_buffer | stage: {self._stage_name} | " f"session: {session_id} | count: {count}"
        )

        self._output_buffer_per_session[session_id].clear()

    def _cleanup(self):
        """Cleanup method that stops consuming and waits for threads."""
        logging.info(f"action: cleanup_start | stage: {self._stage_name}")

        try:
            self._reference_source.stop_consuming()
        except Exception as e:
            logging.debug(f"Error stopping reference consumer: {e}")

        if self._reference_thread and self._reference_thread.is_alive():
            self._reference_thread.join(timeout=5.0)
            if self._reference_thread.is_alive():
                logging.warning(f"action: reference_thread_timeout | stage: {self._stage_name}")

        try:
            self._reference_source.close()
        except Exception as e:
            logging.debug(f"Error closing reference connection: {e}")

        super()._cleanup()

    @abstractmethod
    def _join_entity_fn(self, reference_data: dict, entity: Message) -> Message:
        """
        Join an entity with reference data.

        Args:
            reference_data: Loaded reference data for this session
            entity: Entity to enrich

        Returns:
            Enriched entity
        """
        pass

    @abstractmethod
    def _load_reference_fn(self, reference_data: dict, entity: Message) -> dict:
        """
        Load a reference entity for a session.

        Args:
            reference_data: Accumulated reference data
            entity: Reference entity to load

        Returns:
            Updated reference data
        """
        pass

    @abstractmethod
    def get_reference_type(self) -> Type[Message]:
        """Return the reference entity type (User, Store, MenuItem)."""
        pass

    def start(self):
        """Start the joiner with reference thread."""
        self._reference_thread = threading.Thread(
            target=self._reference_source.start_consuming,
            args=[self._on_reference_message],
            name=f"{self._stage_name}_{self._index}_reference_thread",
            daemon=False,
        )
        self._reference_thread.start()

        logging.info(
            f"action: reference_thread_start | stage: {self._stage_name} | " f"thread: {self._reference_thread.name}"
        )

        super().start()

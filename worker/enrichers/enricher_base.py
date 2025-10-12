import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange, MessageMiddlewareQueue
from shared.protocol import SESSION_ID
from worker.base import WorkerBase
from worker.packer import pack_entity_batch, unpack_entity_batch


class EnricherBase(WorkerBase, ABC):
    """
    Base class for enricher workers that join data with reference data.

    Uses Dead Letter Queue (DLQ) pattern with TTL to defer messages
    when reference data is not yet available for a session.
    """

    DEFAULT_WAITING_TTL_MS = 5000

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
        enricher_input: MessageMiddleware,
        waiting_queue: MessageMiddlewareQueue,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)

        self._buffer_per_session: dict[uuid.UUID, list[Message]] = {}
        self._buffer_size: int = 500

        self._enricher_input: MessageMiddleware = enricher_input
        self._waiting_queue: MessageMiddleware = waiting_queue

        self._enricher_thread = None
        self._enricher_ready_per_session: dict[uuid.UUID, bool] = {}
        self._loaded_entities_per_session: dict[uuid.UUID, dict] = {}

    def _on_enricher_msg(self, channel, method, properties, body: bytes):
        """
        Callback para enricher input (datos de referencia).
        Corre en thread separado.
        """
        session_id: uuid.UUID = uuid.UUID(int=properties.headers.get(SESSION_ID))

        try:
            if EOF.is_type(body):
                self._enricher_ready_per_session[session_id] = True
                logging.info(f"action: enricher_data_ready | stage: {self._stage_name} | " f"session: {session_id}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            for entity in unpack_entity_batch(body, self.get_enricher_type()):
                loaded = self._loaded_entities_per_session.get(session_id, {})
                self._loaded_entities_per_session[session_id] = self._load_entity_fn(loaded, entity)

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(
                f"action: enricher_msg_error | stage: {self._stage_name} | " f"session: {session_id} | error: {e}"
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        """
        Callback para main input (datos a enriquecer).
        Sobrescribe WorkerBase._on_message_upstream para agregar lógica de waiting queue.
        """
        session_id: uuid.UUID = uuid.UUID(int=properties.headers.get(SESSION_ID))
        if not self._enricher_ready_per_session.get(session_id, False):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self._waiting_queue.send(message=body, headers=properties.headers)
            return
        super()._on_message_upstream(channel, method, properties, body)

    def _start_of_session(self, session_id: uuid.UUID):
        """Hook cuando una nueva sesión comienza."""
        logging.info(f"action: session_start | stage: {self._stage_name} | " f"session: {session_id}")

        self._buffer_per_session[session_id] = []

    def _end_of_session(self, session_id: uuid.UUID):
        """Flush final y limpieza cuando una sesión termina."""
        self._flush_buffer(session_id)

        self._buffer_per_session.pop(session_id, None)
        self._enricher_ready_per_session.pop(session_id, None)
        self._loaded_entities_per_session.pop(session_id, None)
        logging.info(f"action: session_end | stage: {self._stage_name} | " f"session: {session_id}")

    def _flush_buffer(self, session_id: uuid.UUID) -> None:
        """Envía buffer acumulado a todas las colas de salida."""
        buffer = self._buffer_per_session.get(session_id, [])

        if not buffer:
            return

        packed: bytes = pack_entity_batch(buffer)

        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.int})

        count = len(buffer)

        logging.info(f"action: flush_buffer | stage: {self._stage_name} | " f"session: {session_id} | count: {count}")

        self._buffer_per_session[session_id].clear()

    def _cleanup(self):
        """Cleanup method that stops consuming and waits for threads."""
        logging.info(f"action: cleanup_start | stage: {self._stage_name}")

        try:
            self._enricher_input.stop_consuming()
        except Exception as e:
            logging.debug(f"Error stopping consumers: {e}")

        if self._enricher_thread and self._enricher_thread.is_alive():
            self._enricher_thread.join(timeout=5.0)
            if self._enricher_thread.is_alive():
                logging.warning(f"action: enricher_thread_timeout | stage: {self._stage_name}")

        try:
            self._enricher_input.close()
        except Exception as e:
            logging.debug(f"Error closing connections: {e}")

        super()._cleanup()

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        """
        Procesa una entidad upstream.
        Las subclases implementan este método.
        """
        loaded = self._loaded_entities_per_session[session_id]
        self._buffer_per_session[session_id].append(self._enrich_entity_fn(loaded, message))
        if len(self._buffer_per_session[session_id]) >= self._buffer_size:
            self._flush_buffer(session_id)

    @abstractmethod
    def _enrich_entity_fn(self, loaded_entities: dict, entity: Message, session_id: uuid.UUID = None) -> Message:
        pass

    @abstractmethod
    def _load_entity_fn(self, loaded_entities: dict, entity: Message) -> dict:
        """
        Carga una entidad de referencia para una sesión.
        """
        pass

    @abstractmethod
    def get_enricher_type(self) -> Type[Message]:
        """Retorna el tipo de entidad de referencia (User, Store, MenuItem)."""
        pass

    def start(self):
        self._enricher_thread = threading.Thread(
            target=self._enricher_input.start_consuming,
            args=[self._on_enricher_msg],
            name=f"{self._stage_name}_{self._index}_enricher_thread",
            daemon=False,
        )
        self._enricher_thread.start()
        super().start()

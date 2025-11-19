import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
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
    _BUFFER_SIZE = 500

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

        self._loaded_entities_per_session: dict[uuid.UUID, dict] = {}
        self._buffer_per_session: dict[uuid.UUID, list[Message]] = {}
        self._enriched_count_per_session: dict[uuid.UUID, int] = {}

        self._enricher_input: MessageMiddleware = enricher_input
        self._queue_per_session: dict[uuid.UUID, MessageMiddleware] = {}

        self._enricher_thread = None
        self._thread_per_session: dict[uuid.UUID, threading.Thread] = {}

    def _on_enricher_msg(self, channel, method, properties, body: bytes):
        """
        Callback para enricher input (datos de referencia).
        Corre en thread separado.
        """
        with self._session_lock:
            session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))

            if session_id not in self._active_sessions and session_id not in self._finished_sessions:
                self._active_sessions.add(session_id)
                self._eof_collected_by_session[session_id] = set()
                self._start_of_session(session_id)

            try:
                if EOF.is_type(body):
                    self._consume_session_queue(session_id)
                    logging.info(
                        f"action: enricher_data_ready | stage: {self._stage_name} | session: {session_id.hex[:8]}"
                    )
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    return

                for entity in unpack_entity_batch(body, self.get_enricher_type()):
                    loaded = self._loaded_entities_per_session.get(session_id, {})
                    self._loaded_entities_per_session[session_id] = self._load_entity_fn(loaded, entity)

                channel.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                logging.error(
                    f"action: enricher_msg_error | stage: {self._stage_name} | "
                    f"session: {session_id.hex[:8]} | error: {e}"
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        """
        Callback para main input (datos a enriquecer).
        Sobrescribe WorkerBase._on_message_upstream para agregar lógica de waiting queue.
        """
        with self._session_lock:
            session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))

            if session_id not in self._active_sessions and session_id not in self._finished_sessions:
                self._active_sessions.add(session_id)
                self._eof_collected_by_session[session_id] = set()
                self._start_of_session(session_id)

            self._send_to_session_queue(message=body, session_id=session_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _start_of_session(self, session_id: uuid.UUID):
        """Hook cuando una nueva sesión comienza."""
        logging.info(f"action: session_start | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        self._buffer_per_session[session_id] = []
        self._enriched_count_per_session[session_id] = 0
        self._queue_per_session[session_id] = self._get_session_queue(session_id)

    def _end_of_session(self, session_id: uuid.UUID):
        """Flush final y limpieza cuando una sesión termina."""
        self._flush_buffer(session_id)
        final_count = self._enriched_count_per_session[session_id]

        self._buffer_per_session.pop(session_id, None)
        self._enriched_count_per_session.pop(session_id, None)
        self._loaded_entities_per_session.pop(session_id, None)

        self._queue_per_session[session_id].stop_consuming()
        self._queue_per_session.pop(session_id, None)

        logging.info(
            f"action: session_end | stage: {self._stage_name} | "
            f"session: {session_id.hex[:8]} | final_count: {final_count}"
        )

    def _flush_buffer(self, session_id: uuid.UUID) -> None:
        """Envía buffer acumulado a todas las colas de salida."""
        buffer = self._buffer_per_session.get(session_id, [])

        if not buffer:
            return

        packed: bytes = pack_entity_batch(buffer)

        for output in self._output:
            output.send(packed, headers={SESSION_ID: session_id.hex})

        count = len(buffer)
        self._enriched_count_per_session[session_id] += count

        logging.info(
            f"action: flush_buffer | stage: {self._stage_name} | session: {session_id.hex[:8]} | count: {count}"
        )

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

        for queue in self._queue_per_session.values():
            queue.stop_consuming()

        for thread in self._thread_per_session.values():
            if thread and thread.is_alive():
                thread.join(timeout=3.0)
            if thread.is_alive():
                logging.warning(f"action: enricher_thread_timeout | stage: {self._stage_name} | name: {thread.name}")

        super()._cleanup()

    def _on_message_session_queue(self, channel, method, properties, body: bytes):
        with self._session_lock:
            session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
            try:
                if not self._handle_eof(body, session_id):
                    for message in unpack_entity_batch(body, self.get_entity_type()):
                        self._on_entity_upstream(message, session_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                _ = e
                logging.exception(f"action: batch_process | stage: {self._stage_name}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        """
        Procesa una entidad upstream.
        Las subclases implementan este método.
        """
        loaded = self._loaded_entities_per_session[session_id]
        self._buffer_per_session[session_id].append(self._enrich_entity_fn(loaded, message))
        if len(self._buffer_per_session[session_id]) >= self._BUFFER_SIZE:
            self._flush_buffer(session_id)

    @abstractmethod
    def _enrich_entity_fn(self, loaded_entities: dict, entity: Message) -> Message:
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

    def _send_to_session_queue(self, message: bytes, session_id: uuid.UUID) -> None:
        session_queue = self._get_session_queue(session_id)
        session_queue.send(message, headers={SESSION_ID: session_id.hex})

    def _consume_session_queue(self, session_id: uuid.UUID):
        session_queue = self._get_session_queue(session_id)
        consumer = threading.Thread(
            target=session_queue.start_consuming,
            kwargs={"on_message_callback": self._on_message_session_queue},
            name=f"{self._stage_name}_{self._index}_{session_id.hex}_session_thread",
            daemon=False,
        )
        consumer.start()
        self._thread_per_session[session_id] = consumer

    def _get_session_queue(self, session_id: uuid.UUID) -> MessageMiddleware:
        queue_name = self._stage_name + session_id.hex
        if session_id not in self._queue_per_session:
            self._queue_per_session[session_id] = MessageMiddlewareQueueMQ("rabbitmq", queue_name)
        return self._queue_per_session[session_id]

    def start(self):
        self._enricher_thread = threading.Thread(
            target=self._enricher_input.start_consuming,
            args=[self._on_enricher_msg],
            name=f"{self._stage_name}_{self._index}_enricher_thread",
            daemon=False,
        )
        self._enricher_thread.start()
        super().start()

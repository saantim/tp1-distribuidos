import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Generic, Type, TypeVar

from pydantic.generics import GenericModel

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddlewareExchange
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import MESSAGE_ID, SESSION_ID
from worker.base import Session, WorkerBase
from worker.output import WorkerOutput
from worker.packer import unpack_entity_batch


TypedMSG = TypeVar("TypedMSG", bound=Message)


class EnricherSessionData(GenericModel, Generic[TypedMSG]):
    """Storage for per-session enricher state."""

    loaded_entities: Dict[str, Type[TypedMSG]] = {}
    loaded_finished: bool = False
    buffer: list[TypedMSG] = []
    enriched_count: int = 0


class EnricherBase(WorkerBase, ABC):
    """
    Base class for enricher workers that join data with reference data.

    Uses Dead Letter Queue (DLQ) pattern with TTL to defer messages
    when reference data is not yet available for a session.
    """

    DEFAULT_WAITING_TTL_MS = 5000
    _BUFFER_SIZE = 10000

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddlewareExchange,
        outputs: list[WorkerOutput],
        enricher_input: MessageMiddlewareExchange,
    ):
        super().__init__(instances, index, stage_name, source, outputs)

        self._enricher_input: MessageMiddlewareExchange = enricher_input

        self._queue_per_session: dict[uuid.UUID, MessageMiddlewareQueueMQ] = {}
        self._thread_per_session: dict[uuid.UUID, threading.Thread] = {}
        self._session_locks: dict[uuid.UUID, threading.Lock] = {}

        self._enricher_thread = None

    def _on_enricher_msg(self, channel, method, properties, body: bytes):
        """
        Callback para enricher input (datos de referencia).
        Corre en thread separado.
        """
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
        message_id: str = properties.headers.get(MESSAGE_ID)
        session: Session = self._session_manager.get_or_initialize(session_id)

        if session.is_duplicated_msg(message_id):
            logging.warning(
                f"action: duplicated_enrich_reference | stage: {self._stage_name} |"
                f" session: {session_id.hex[:8]} | message_id: {message_id}"
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        session.add_msg_received(message_id)

        try:
            session_data = session.get_storage(self.get_session_data_type())
            if EOF.is_type(body):
                with self._get_session_lock(session_id):
                    session_data.loaded_finished = True
                    self._session_manager.save_session(session)
                self._consume_session_queue(session_id)
                logging.info(f"action: enricher_data_ready | stage: {self._stage_name} | session: {session_id.hex[:8]}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            for entity in unpack_entity_batch(body, self.get_enricher_type()):
                self._load_entity_fn(session_data, entity)

            with self._get_session_lock(session_id):
                self._session_manager.save_session(session)

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.exception(
                f"action: enricher_msg_error | stage: {self._stage_name} | "
                f"session: {session_id.hex[:8]} | error: {e}"
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        """
        Procesa una entidad upstream.
        Las subclases implementan este método.
        """
        session_data = session.get_storage(self.get_session_data_type())
        enriched = self._enrich_entity_fn(session_data, message)
        session_data.buffer.append(enriched)

        session_data.enriched_count += 1
        if session_data.enriched_count % 100000 == 0:
            logging.info(
                f"[{self._stage_name}] {session_data.message_count//1000}k enriched | "
                f"session: {session.session_id.hex[:8]}"
            )

        if len(session_data.buffer) >= self._BUFFER_SIZE:
            self._flush_buffer(session)

    def _on_message_session_queue(self, channel, method, properties, body: bytes):
        message_id: str = properties.headers.get(MESSAGE_ID)
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
        session: Session = self._session_manager.get_or_initialize(session_id)
        try:
            if session.is_duplicated_msg(message_id):
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logging.warning(f"action: duplicated_msg_to_enrich | id: {message_id}")
                return
            session.add_msg_received(properties.headers.get(MESSAGE_ID))
            if not self._handle_eof(body, session):
                for message in unpack_entity_batch(body, self.get_entity_type()):
                    self._on_entity_upstream(message, session)
            with self._get_session_lock(session_id):
                self._session_manager.save_session(session)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _ = e
            logging.exception(f"action: batch_process | stage: {self._stage_name}")

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        """
        Callback para main input (datos a enriquecer).
        Sobrescribe WorkerBase._on_message_upstream para agregar lógica de waiting queue.
        """
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
        message_id: str = properties.headers.get(MESSAGE_ID)
        session = self._session_manager.get_or_initialize(session_id)

        if session.is_duplicated_msg(message_id):
            logging.warning(
                f"action: duplicated_enrich_upstream | stage: {self._stage_name} |"
                f" session: {session_id.hex[:8]} | message_id: {message_id}"
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        self._send_to_session_queue(message=body, session_id=session_id, message_id=message_id)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def _start_of_session(self, session: Session):
        """Hook cuando una nueva sesión comienza."""
        session_id = session.session_id
        self._session_locks[session_id] = threading.Lock()
        logging.info(f"action: session_start | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        session.set_storage(self.get_session_data_type()())

    def _end_of_session(self, session: Session):
        """Flush final y limpieza cuando una sesión termina."""
        session_id = session.session_id
        session_data = session.get_storage(self.get_session_data_type())

        self._flush_buffer(session)
        final_count = session_data.enriched_count

        queue = self._queue_per_session.get(session_id)
        if queue:
            try:
                queue.delete()
            except Exception as e:
                logging.warning(f"Failed to delete session queue: {e}")

        queue = self._queue_per_session.pop(session_id, None)
        if queue:
            queue.delete()
        self._thread_per_session.pop(session_id, None)

        logging.info(
            f"action: session_end | stage: {self._stage_name} | "
            f"session: {session_id.hex[:8]} | final_count: {final_count}"
        )

    def _flush_buffer(self, session: Session) -> None:
        """Envía buffer acumulado a todas las colas de salida."""
        session_id = session.session_id
        session_data = session.get_storage(self.get_session_data_type())
        buffer = session_data.buffer

        if not buffer:
            return

        self._send_message(messages=buffer, session_id=session_id)

        count = len(buffer)
        session_data.enriched_count += count

        logging.info(
            f"action: flush_buffer | stage: {self._stage_name} | session: {session_id.hex[:8]} | count: {count}"
        )

        session_data.buffer.clear()

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
            try:
                queue.stop_consuming()
                queue.close()
            except Exception as e:
                logging.debug(f"Error closing session queue: {e}")

        for thread in self._thread_per_session.values():
            if thread and thread.is_alive():
                thread.join(timeout=3.0)
            if thread.is_alive():
                logging.warning(f"action: enricher_thread_timeout | stage: {self._stage_name} | name: {thread.name}")

        super()._cleanup()

    @abstractmethod
    def _enrich_entity_fn(self, session_data, entity: TypedMSG) -> Message:
        """Enrich entity using loaded reference data from session_data."""
        pass

    @abstractmethod
    def _load_entity_fn(self, session_data, entity: TypedMSG) -> None:
        """Load enrichment reference data. Modifies session_data in place."""
        pass

    @abstractmethod
    def get_enricher_type(self) -> Type[Message]:
        """Retorna el tipo de entidad de referencia (User, Store, MenuItem)."""
        pass

    @abstractmethod
    def get_session_data_type(self):
        """Return the session data type for this enricher."""
        pass

    def _send_to_session_queue(self, message: bytes, session_id: uuid.UUID, message_id: str) -> None:
        """Send message to session-specific direct queue."""
        queue = self._get_session_queue(session_id)
        queue.send(message, headers={SESSION_ID: session_id.hex, MESSAGE_ID: message_id})

    def _consume_session_queue(self, session_id: uuid.UUID):
        """Start consuming from session queue (spawns thread with self-cleanup)."""
        session_queue = self._get_session_queue(session_id)

        def thread_target():
            try:
                session_queue.start_consuming(on_message_callback=self._on_message_session_queue)
            finally:
                try:
                    session_queue.close()
                    logging.debug(
                        f"action: closed_session_queue | stage: {self._stage_name} | " f"session: {session_id.hex[:8]}"
                    )
                except Exception as e:
                    logging.warning(f"Error closing session queue connection: {e}")

        consumer_thread = threading.Thread(
            target=thread_target,
            name=f"{self._stage_name}_{self._index}_{session_id.hex}_session_thread",
            daemon=False,
        )
        consumer_thread.start()
        self._thread_per_session[session_id] = consumer_thread

        logging.info(f"action: start_session_consumer | stage: {self._stage_name} | session: {session_id.hex[:8]}")

    def _get_session_queue(self, session_id: uuid.UUID) -> MessageMiddlewareQueueMQ:
        """Get or create session-specific direct queue"""
        if session_id not in self._queue_per_session:
            queue_name = f"{self._stage_name}_{self._index}_{session_id.hex[:8]}"
            self._queue_per_session[session_id] = MessageMiddlewareQueueMQ(host="rabbitmq", queue_name=queue_name)
        return self._queue_per_session[session_id]

    def _get_session_lock(self, session_id: uuid.UUID) -> threading.Lock:
        if session_id not in self._session_locks:
            self._session_locks[session_id] = threading.Lock()
        return self._session_locks[session_id]

    def start(self):
        self._heartbeat.start()

        self._enricher_thread = threading.Thread(
            target=self._enricher_input.start_consuming,
            args=[self._on_enricher_msg],
            name=f"{self._stage_name}_{self._index}_enricher_thread",
            daemon=False,
        )

        self._upstream_thread = threading.Thread(
            target=self._source.start_consuming,
            args=[self._on_message_upstream],
            name=f"{self._stage_name}_{self._index}_data_thread",
            daemon=False,
        )

        self._try_to_load_sessions()

        for session in self._session_manager.get_sessions().values():
            session_data = session.get_storage(self.get_session_data_type())
            if session_data.loaded_finished:
                self._consume_session_queue(session.session_id)
                logging.info(
                    f"action: recover_session_queue | stage: {self._stage_name} | session: {session.session_id.hex[:8]}"
                )

        self._mark_ready()

        self._enricher_thread.start()
        self._upstream_thread.start()

        if self._leader:
            logging.info(
                f"action: thread_start | stage: {self._stage_name} | enricher_thread: {self._enricher_thread.name}"
            )
            logging.info(
                f"action: thread_start | stage: {self._stage_name} | data_thread: {self._upstream_thread.name}"
            )

        self._shutdown_event.wait()

        self._cleanup()

        logging.info(f"action: exiting | stage: {self._stage_name}")

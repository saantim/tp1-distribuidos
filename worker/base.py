import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Type, Optional, Any, Callable

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange, MessageMiddlewareQueue
from shared.protocol import SESSION_ID, MESSAGE_ID
from shared.shutdown import ShutdownSignal
from worker.packer import unpack_entity_batch
from worker.types import EOFIntraExchange

class Session:
    def __init__(self, session_id: uuid.UUID):
        self.session_id = session_id
        self._storage: Optional[Any] = None
        self._eof_collected: set[str] = set()

    def get_storage(self) -> Optional[Any]:
        return self._storage

    def set_storage(self, storage: Any):
        self._storage = storage

    def add_eof(self, worker_id: str):
        self._eof_collected.add(worker_id)

    def get_eof_collected(self):
        return self._eof_collected

class SessionManager:
    def __init__(self,
                 stage_name: str,
                 on_start_of_session: Callable,
                 on_end_of_session: Callable,
                 instances: int,
                 is_leader: bool):
        self._sessions: dict[uuid.UUID, Session] = {}
        self._on_start_of_session = on_start_of_session
        self._on_end_of_session = on_end_of_session
        self._stage_name = stage_name
        self._instances = instances
        self._is_leader = is_leader
        self._setup_logging()

    def _setup_logging(self):
        """Configura el logging para la clase."""
        logging.basicConfig(
            level=logging.INFO,
            format=f"{self.__class__.__name__} - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        if session_id not in self._sessions:
            self._sessions[session_id] = Session(session_id)
            self._on_start_of_session(self._sessions[session_id])
            logging.info(f"action: create session | stage: {self._stage_name}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    def try_to_flush(self, session:Session) -> bool:
        if self._is_flushable(session):
            self._on_end_of_session(session)
            self._sessions.pop(session.session_id, None)
            return True
        return False

    def _is_flushable(self, session: Session) -> bool:
        return len(session.get_eof_collected()) >= (self._instances if self._is_leader else 1)


class WorkerBase(ABC):
    COMMON_ROUTING_KEY = "common"

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        downstream_worker_quantity: int,
        source: MessageMiddlewareExchange,
        output: MessageMiddleware,
    ):
        self._stage_name: str = stage_name
        self._instances: int = instances
        self._index: int = index
        self._leader: bool = index == 0
        self._source: MessageMiddlewareExchange = source
        self._output: MessageMiddleware = output
        self._upstream_thread = None
        self._downstream_worker_quantity = downstream_worker_quantity
        self._shutdown_event = threading.Event()
        self._shutdown_handler = ShutdownSignal(self._shutdown)
        self._session_manager = SessionManager(
            stage_name=self._stage_name,
            on_start_of_session=self._start_of_session,
            on_end_of_session=self._end_of_session,
            instances=self._instances,
            is_leader=self._leader,
        )

    def start(self):
        self._upstream_thread = threading.Thread(
            target=self._source.start_consuming,
            args=[self._on_message_upstream],
            name=f"{self._stage_name}_{self._index}_data_thread",
            daemon=False,
        )

        self._upstream_thread.start()

        logging.info(
            f"action: thread_start | stage: {self._stage_name} |"
            f" data_thread: {self._upstream_thread.name} |"
        )

        # Wait for shutdown signal
        self._shutdown_event.wait()

        # Perform cleanup
        self._cleanup()

        logging.info(f"action: exiting | stage: {self._stage_name}")

    def stop(self):
        """Signal shutdown."""
        logging.info(f"action: stop_requested | stage: {self._stage_name}")
        self._shutdown_event.set()

    def _cleanup(self):
        """Cleanup method that stops consuming and waits for threads."""
        logging.info(f"action: cleanup_start | stage: {self._stage_name}")

        try:
            self._source.stop_consuming()
        except Exception as e:
            logging.debug(f"Error stopping consumers: {e}")

        if self._upstream_thread and self._upstream_thread.is_alive():
            self._upstream_thread.join(timeout=5.0)
            if self._upstream_thread.is_alive():
                logging.warning(f"action: data_thread_timeout | stage: {self._stage_name}")

        try:
            self._source.close()
        except Exception as e:
            logging.debug(f"Error closing connections: {e}")

        logging.info(f"action: cleanup_completed | stage: {self._stage_name}")

    def _shutdown(self, _signum: int, _frame):
        """Signal handler for SIGTERM/SIGINT"""
        logging.info(f"action: signal_received | stage: {self._stage_name} | signal: {_signum}")
        self.stop()

    def _handle_eof(self, message: bytes, session :Session) -> bool:
        if not EOF.is_type(message):
            return False

        eof = EOF.deserialize(message)

        logging.info(f"action: receive_EOF | stage: {self._stage_name} | session: {session.session_id} | from: {self._source}")
        session.add_eof(eof.worker_id)

        if self._session_manager.try_to_flush(session):
            if self._leader:
                self._output.send(
                    EOF().serialize(), routing_key=self.COMMON_ROUTING_KEY, headers={SESSION_ID: session.session_id.hex, MESSAGE_ID: uuid.uuid4().hex}
                )
            else:
                self._source.send(
                    EOFIntraExchange(str(self._index)).serialize(), headers={SESSION_ID: session.session_id.hex, MESSAGE_ID: uuid.uuid4().hex}
                )

        return True

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
        session: Session = self._session_manager.get_or_initialize(session_id)

        try:
            if not self._handle_eof(body, session):
                for message in unpack_entity_batch(body, self.get_entity_type()):
                    self._on_entity_upstream(message, session)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _ = e
            logging.exception(f"action: batch_process | stage: {self._stage_name}")
            #channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _send_message(self, message: bytes, session_id: uuid.UUID, message_id: uuid.UUID):
        if isinstance(self._output, MessageMiddlewareQueue):
            self._output.send(message, headers={SESSION_ID: session_id.hex, MESSAGE_ID: message_id.hex})
        elif isinstance(self._output, MessageMiddlewareExchange):
            routing_key: str = str(message_id.int % self._downstream_worker_quantity)
            self._output.send(message, routing_key=routing_key, headers={SESSION_ID: session_id.hex, MESSAGE_ID: message_id.hex})
        else:
            raise TypeError(f"Unsupported output: {type(self._output)}")

    @abstractmethod
    def _end_of_session(self, session: Session):
        pass

    @abstractmethod
    def _start_of_session(self, session: Session):
        pass

    @abstractmethod
    def _on_entity_upstream(self, message: Message, session: Session) -> None:
        pass

    @abstractmethod
    def get_entity_type(self) -> Type[Message]:
        pass

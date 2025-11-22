import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Type

from shared.entity import EOF, Message, WorkerEOF
from shared.middleware.interface import MessageMiddlewareExchange
from shared.protocol import MESSAGE_ID, SESSION_ID
from shared.shutdown import ShutdownSignal
from worker.heartbeat import build_container_name, HeartbeatSender
from worker.output import WorkerOutput
from worker.packer import pack_entity_batch, unpack_entity_batch


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
    def __init__(
        self,
        stage_name: str,
        on_start_of_session: Callable,
        on_end_of_session: Callable,
        instances: int,
        is_leader: bool,
    ):
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
        logging.getLogger("pika").setLevel(logging.WARNING)

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        if session_id not in self._sessions:
            self._sessions[session_id] = Session(session_id)
            self._on_start_of_session(self._sessions[session_id])
            if self._is_leader:
                logging.info(f"action: create_session | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    # TODO: AGREGAR METODO QUE PERMITE CERRAR SESSION LUEGO DE FLUSH.

    def try_to_flush(self, session: Session) -> bool:
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
        source: MessageMiddlewareExchange,
        outputs: List[WorkerOutput],
    ):
        self._stage_name: str = stage_name
        self._instances: int = instances
        self._index: int = index
        self._leader: bool = index == 0
        self._source: MessageMiddlewareExchange = source
        self._outputs: List[WorkerOutput] = outputs
        self._upstream_thread = None
        self._shutdown_event = threading.Event()
        self._shutdown_handler = ShutdownSignal(self._shutdown)
        self._session_manager = SessionManager(
            stage_name=self._stage_name,
            on_start_of_session=self._start_of_session,
            on_end_of_session=self._end_of_session,
            instances=self._instances,
            is_leader=self._leader,
        )
        container_name = build_container_name(self._stage_name, self._index, self._instances)
        self._heartbeat = HeartbeatSender(container_name, self._shutdown_event)

    def start(self):
        self._heartbeat.start()

        self._upstream_thread = threading.Thread(
            target=self._source.start_consuming,
            args=[self._on_message_upstream],
            name=f"{self._stage_name}_{self._index}_data_thread",
            daemon=False,
        )

        self._upstream_thread.start()

        if self._leader:
            logging.info(f"action: thread_start | stage: {self._stage_name} | thread: {self._upstream_thread.name}")

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

        self._heartbeat.stop()

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

    def _handle_eof(self, message: bytes, session: Session) -> bool:

        if not WorkerEOF.is_type(message) and not EOF.is_type(message):
            return False

        if WorkerEOF.is_type(message):
            worker_eof = WorkerEOF.deserialize(message)
            session.add_eof(worker_eof.worker_id)
            logging.info(
                f"action: receive_WorkerEOF | stage: {self._stage_name} | session: {session.session_id.hex[:8]} | "
                f"from: {worker_eof.worker_id} | collected: {session.get_eof_collected()}"
            )
        else:
            session.add_eof(str(self._index))
            logging.info(
                f"action: receive_UpstreamEOF | stage: {self._stage_name} | session: {session.session_id.hex[:8]}"
            )

        if self._session_manager.try_to_flush(session):
            if self._leader:
                for output in self._outputs:
                    output.exchange.send(
                        EOF().serialize(),
                        routing_key=self.COMMON_ROUTING_KEY,
                        headers={SESSION_ID: session.session_id.hex, MESSAGE_ID: uuid.uuid4().hex},
                    )
                    logging.info(f"action: sent_DownstreamEOF | stage: {self._stage_name} | to: {output}")
            else:
                leader_routing_key = f"{self._stage_name}_0"
                worker_eof_bytes = WorkerEOF(worker_id=str(self._index)).serialize()
                self._source.send(
                    worker_eof_bytes,
                    routing_key=leader_routing_key,
                    headers={SESSION_ID: session.session_id.hex, MESSAGE_ID: uuid.uuid4().hex},
                )
                logging.info(f"action: sent_WorkerEOF | stage: {self._stage_name} | to: {leader_routing_key}")

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
            # channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _send_message(self, messages: List[Message], session_id: uuid.UUID, message_id: uuid.UUID):
        """
        Send messages to all outputs with appropriate routing.

        Args:
            messages: List of messages to send
            session_id: Session UUID
            message_id: Message UUID (used for default routing)
        """
        if not messages:
            return

        # TODO: Si la performance no esta del todo bien, optimizar para no hacer doble-for en casos
        #    que ya sabemos a donde va el mensaje batcheado completo (default y common)

        for output in self._outputs:

            buffers: dict[str, List[Message]] = {}

            for message in messages:
                routing_key = output.get_routing_key(message, message_id.int)
                if routing_key not in buffers:
                    buffers[routing_key] = []
                buffers[routing_key].append(message)

            for routing_key, msg_batch in buffers.items():
                packed = pack_entity_batch(msg_batch)
                output.exchange.send(
                    packed, routing_key=routing_key, headers={SESSION_ID: session_id.hex, MESSAGE_ID: message_id.hex}
                )

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

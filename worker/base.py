import json
import logging
import os
import tempfile
import threading
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, List, Optional, Type, Union

from shared.entity import EOF, Message, WorkerEOF
from shared.middleware.interface import MessageMiddlewareExchange
from shared.protocol import MESSAGE_ID, SESSION_ID
from shared.shutdown import ShutdownSignal
from worker.output import WorkerOutput
from worker.packer import pack_entity_batch, unpack_entity_batch
from pydantic import BaseModel


class Session(BaseModel):
    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: Optional[Any] = None
    
    def get_storage(self):
        return self.storage

    def set_storage(self, storage: Any):
        self.storage = storage
        
    def add_eof(self, worker_id: str):
        self.eof_collected.add(worker_id)

    def get_eof_collected(self):
        return self.eof_collected

    def add_msg_received(self, msg_id: str):
        self.msgs_received.add(msg_id)

    def is_msg_received(self, msg_id: str):
        return msg_id in self.msgs_received

    def save(self, save_dir: str = './sessions/saves') -> None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        
        tmp_dir = save_dir / 'tmp'
        tmp_dir.mkdir(exist_ok=True, mode=0o755)

        data = self.model_dump(mode="json")

        serialized = json.dumps(data, indent=2)
        session_file = save_dir / f"{self.session_id}.json"
        tmp_path = tmp_dir / f"{self.session_id}.json"
        
        logging.info(f"[Session] Saving session to temp file: {tmp_path}")

        try:
            with open(tmp_path, 'w') as f:
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())
            
            dir_fd = os.open(str(save_dir), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            
            os.replace(tmp_path, session_file)
            logging.info(f"[Session] Session saved to: {session_file}")
            
        except Exception as e:
            logging.error(f"[Session] Error saving session {self.session_id}. "
                         f"Temp file kept at: {tmp_path} - Error: {e}")
            raise

    @classmethod
    def load(cls, session_id: Union[str, uuid.UUID], save_dir: Union[str, Path]) -> Optional['Session']:
        save_dir = Path(save_dir)
        session_id = str(session_id)
        session_file = save_dir / f"{session_id}.json"
        
        if session_file.exists():
            try:
                with open(session_file, 'r') as f:
                    data = json.load(f)
                if 'eof_collected' in data:
                    data['eof_collected'] = set(data['eof_collected'])
                if 'msgs_received' in data:
                    data['msgs_received'] = set(data['msgs_received'])
                return cls(**data)
            except Exception as e:
                logging.error(f"[Session] Error loading session {session_id}: {e}")
                return None

        tmp_dir = save_dir / 'tmp'
        if tmp_dir.exists():
            for tmp_file in tmp_dir.glob(f"{session_id}.json"):
                try:
                    with open(tmp_file, 'r') as f:
                        data = json.load(f)
                    if 'eof_collected' in data:
                        data['eof_collected'] = set(data['eof_collected'])
                    if 'msgs_received' in data:
                        data['msgs_received'] = set(data['msgs_received'])
                    os.replace(tmp_file, session_file)
                    return cls(**data)
                except Exception as e:
                    logging.debug(f"[Session] Ignoring invalid tmp file {tmp_file}: {e}")
                    continue
        
        return None



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
            self._sessions[session_id] = Session(session_id=session_id)
            self._on_start_of_session(self._sessions[session_id])
            logging.info(f"action: create_session | stage: {self._stage_name} | session_id: {session_id}")
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

    def save_sessions(self, path: Union[str, Path] = './sessions/saves') -> None:
        for session in self._sessions.values():
            session.save(path)

    def save_session(self, session_id: uuid.UUID, path: Union[str, Path] = './sessions/saves') -> None:
        session = self._sessions.get(session_id, None)
        if session:
            session.save(path)

    def load_sessions(self, path: Union[str, Path] = './sessions/saves') -> None:
        path = Path(path)
        if not path.exists():
            logging.info(f"[SessionManager] No sessions directory found at: {path}")
            return
        if not path.is_dir():
            raise NotADirectoryError(f"El path de sesiones no es un directorio: {path}")

        self._sessions.clear()

        for session_file in path.glob("*.json"):
            session_id_str = session_file.stem
            try:
                session = Session.load(session_id_str, path)
                if session:
                    self._sessions[session.session_id] = session
            except Exception as e:
                logging.debug(f"[SessionManager] Ignorando sesión inválida {session_file}: {e}")


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

    def start(self):
        self._upstream_thread = threading.Thread(
            target=self._source.start_consuming,
            args=[self._on_message_upstream],
            name=f"{self._stage_name}_{self._index}_data_thread",
            daemon=False,
        )
        self._try_to_load_sessions()
        self._upstream_thread.start()

        logging.info(
            f"action: thread_start | stage: {self._stage_name} |" f" data_thread: {self._upstream_thread.name} |"
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

    def _handle_eof(self, message: bytes, session: Session) -> bool:

        if not WorkerEOF.is_type(message) and not EOF.is_type(message):
            return False

        if WorkerEOF.is_type(message):
            worker_eof = WorkerEOF.deserialize(message)
            session.add_eof(worker_eof.worker_id)
            logging.info(
                f"action: receive_WorkerEOF | stage: {self._stage_name} | session: {session.session_id} |"
                f" from: {worker_eof.worker_id} |"
                f" collected: {session.get_eof_collected()}"
            )
        else:
            session.add_eof(str(self._index))
            logging.info(f"action: receive_UpstreamEOF | stage: {self._stage_name} | session: {session.session_id}")

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
            if not self._handle_eof(body, session) and not session.is_msg_received(properties.headers.get(MESSAGE_ID)):
                session.add_msg_received(properties.headers.get(MESSAGE_ID))
                for message in unpack_entity_batch(body, self.get_entity_type()):
                    self._on_entity_upstream(message, session)
            self._session_manager.save_session(session_id)
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

    def _try_to_load_sessions(self):
        self._session_manager.load_sessions()

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

import hashlib
import logging
import threading
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Type

from pydantic import BaseModel

from shared.entity import EOF, Message, WorkerEOF
from shared.middleware.interface import MessageMiddlewareExchange
from shared.protocol import MESSAGE_ID, SESSION_ID
from shared.shutdown import ShutdownSignal
from worker.heartbeat import build_container_name, HeartbeatSender
from worker.output import WorkerOutput
from worker.packer import pack_entity_batch, unpack_entity_batch
from worker.session import Session
from worker.session_manager import SessionManager
from worker.session_storage import SessionStorage, SnapshotFileSessionStorage


class WorkerBase(ABC):
    COMMON_ROUTING_KEY = "common"

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddlewareExchange,
        outputs: List[WorkerOutput]
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
            session_storage=self.create_session_storage()
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
        self._try_to_load_sessions()
        self._mark_ready()
        self._upstream_thread.start()

        if self._leader:
            logging.info(f"action: thread_start | stage: {self._stage_name} | thread: {self._upstream_thread.name}")

        self._shutdown_event.wait()

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
        message_id: str = properties.headers.get(MESSAGE_ID)
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))
        session: Session = self._session_manager.get_or_initialize(session_id)
        try:
            if session.is_duplicated_msg(message_id):
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logging.warning(f"action: duplicated_msg | id: {message_id}")
                return
            session.add_msg_received(properties.headers.get(MESSAGE_ID))
            if not self._handle_eof(body, session):
                for message in unpack_entity_batch(body, self.get_entity_type()):
                    self._on_entity_upstream(message, session)
            self._session_manager.save_session(session)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _ = e
            logging.exception(f"action: batch_process | stage: {self._stage_name}")

    def _send_message(self, messages: List[Message], session_id: uuid.UUID):
        """
        Send messages to all outputs with appropriate routing.

        Args:
            messages: List of messages to send
            session_id: Session UUID
        """
        if not messages:
            return

        for output in self._outputs:

            buffers: dict[str, List[Message]] = {}

            for message in messages:
                message_id = int(hashlib.md5(pack_entity_batch([message])).hexdigest()[:8], 16)
                routing_key = output.get_routing_key(message, message_id)
                if routing_key not in buffers:
                    buffers[routing_key] = []
                buffers[routing_key].append(message)

            for routing_key, msg_batch in buffers.items():
                packed = pack_entity_batch(msg_batch)
                message_id = hashlib.md5(packed).hexdigest()
                output.exchange.send(
                    packed, routing_key=routing_key, headers={SESSION_ID: session_id.hex, MESSAGE_ID: message_id}
                )

    def _try_to_load_sessions(self):
        self._session_manager.load_sessions()
        logging.info(f"[SESSION] action: load_sessions | stage: {self._stage_name}")

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

    def create_session_storage(self) -> SessionStorage:
        return SnapshotFileSessionStorage()

    def get_session_data_type(self) -> Type[BaseModel]:
        pass

    def _mark_ready(self):
        ready_path = Path("/tmp/ready")
        try:
            ready_path.touch(exist_ok=True)
            logging.info(f"[HEALTHCHECK] Worker marked as ready at {ready_path}")
        except Exception as e:
            logging.error(f"[HEALTHCHECK] Failed to mark ready: {e}")

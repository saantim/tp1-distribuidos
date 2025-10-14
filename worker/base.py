import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from shared.protocol import SESSION_ID
from shared.shutdown import ShutdownSignal
from worker.packer import unpack_entity_batch
from worker.types import EOFIntraExchange


class WorkerBase(ABC):
    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
    ):
        self._stage_name: str = stage_name
        self._instances: int = instances
        self._index: int = index
        self._leader: bool = index == 0
        self._active_sessions: set[uuid.UUID] = set()
        self._finished_sessions: set[uuid.UUID] = set()
        self._eof_collected_by_session: dict[uuid.UUID, set[str]] = {}
        self._source: MessageMiddleware = source
        self._output: list[MessageMiddleware] = output
        self._intra_exchange: MessageMiddlewareExchange = intra_exchange
        self._not_processing_batch: threading.Event = threading.Event()
        self._not_processing_batch.set()
        self._data_thread = None
        self._control_thread = None
        self._shutdown_event = threading.Event()

        self._shutdown_handler = ShutdownSignal(self._shutdown)

        logging.basicConfig(
            level=logging.INFO,
            format=self.__class__.__name__ + " - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

    def start(self):
        self._data_thread = threading.Thread(
            target=self._source.start_consuming,
            args=[self._on_message_upstream],
            name=f"{self._stage_name}_{self._index}_data_thread",
            daemon=False,
        )

        self._data_thread.start()

        self._control_thread = threading.Thread(
            target=self._intra_exchange.start_consuming,
            args=[self._on_message_intra_exchange],
            name=f"{self._stage_name}_{self._index}_control_thread",
            daemon=False,
        )

        self._control_thread.start()

        logging.info(
            f"action: threads_start | stage: {self._stage_name} |"
            f" data_thread: {self._data_thread.name} |"
            f" control_thread: {self._control_thread.name}"
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
            self._intra_exchange.stop_consuming()
        except Exception as e:
            logging.debug(f"Error stopping consumers: {e}")

        if self._data_thread and self._data_thread.is_alive():
            self._data_thread.join(timeout=5.0)
            if self._data_thread.is_alive():
                logging.warning(f"action: data_thread_timeout | stage: {self._stage_name}")

        if self._control_thread and self._control_thread.is_alive():
            self._control_thread.join(timeout=5.0)
            if self._control_thread.is_alive():
                logging.warning(f"action: control_thread_timeout | stage: {self._stage_name}")

        try:
            self._source.close()
            self._intra_exchange.close()
        except Exception as e:
            logging.debug(f"Error closing connections: {e}")

        logging.info(f"action: cleanup_complete | stage: {self._stage_name}")

    def _shutdown(self, _signum: int, _frame):
        """Signal handler for SIGTERM/SIGINT"""
        logging.info(f"action: signal_received | stage: {self._stage_name} | signal: {_signum}")
        self.stop()

    def _handle_eof(self, message: bytes, session_id: uuid.UUID) -> bool:
        if not EOF.is_type(message):
            return False

        logging.info(f"action: receive_EOF | stage: {self._stage_name} | session: {session_id} | from: {self._source}")
        if session_id not in self._finished_sessions:
            self._active_sessions.discard(session_id)
            self._finished_sessions.add(session_id)
            self._end_of_session(session_id)
            self._intra_exchange.send(
                EOFIntraExchange(str(self._index)).serialize(), headers={SESSION_ID: session_id.hex}
            )
        return True

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))

        if session_id not in self._active_sessions and session_id not in self._finished_sessions:
            self._active_sessions.add(session_id)
            self._eof_collected_by_session[session_id] = set()
            self._start_of_session(session_id)

        try:
            self._not_processing_batch.clear()
            if not self._handle_eof(body, session_id):
                for message in unpack_entity_batch(body, self.get_entity_type()):
                    self._on_entity_upstream(message, session_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _ = e
            logging.exception(f"action: batch_process | stage: {self._stage_name}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            self._not_processing_batch.set()

    def _on_message_intra_exchange(self, channel, method, properties, body: bytes) -> None:
        session_id: uuid.UUID = uuid.UUID(hex=properties.headers.get(SESSION_ID))

        if not EOFIntraExchange.is_type(body):
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        eof_message: EOFIntraExchange = EOFIntraExchange.deserialize(body)
        worker_id = eof_message.worker_id

        if self._leader:
            logging.info(
                f"action: got_intra_msg | from_worker: {worker_id} |"
                f" stage: {self._stage_name} | session: {session_id}"
            )
            self._eof_collected_by_session[session_id].add(worker_id)

        channel.basic_ack(delivery_tag=method.delivery_tag)

        # TODO: si uno de los workers nunca laburo, nunca activa su sesion, por ende no propaga su Intra.
        #   buscar luego una manera de fixear.
        if session_id in self._active_sessions:
            assert self._not_processing_batch.wait(timeout=10.0), "timeamos out esperando desde un intra."
            self._finished_sessions.add(session_id)
            self._active_sessions.discard(session_id)
            self._end_of_session(session_id)
            self._intra_exchange.send(
                EOFIntraExchange(str(self._index)).serialize(), headers={SESSION_ID: session_id.hex}
            )

        if self._leader:
            self._flush_eof_if_possible(session_id)

    def _flush_eof_if_possible(self, session_id: uuid.UUID) -> None:
        if len(self._eof_collected_by_session[session_id]) == self._instances:
            for output in self._output:
                output.send(EOF().serialize(), headers={SESSION_ID: session_id.hex})
                logging.info(f"action: flush_eof | to: {output} | session: {session_id}")
            self._eof_collected_by_session.pop(session_id, None)

    @abstractmethod
    def _end_of_session(self, session_id: uuid.UUID):
        pass

    @abstractmethod
    def _start_of_session(self, session_id: uuid.UUID):
        pass

    @abstractmethod
    def _on_entity_upstream(self, message: Message, session_id: uuid.UUID) -> None:
        pass

    @abstractmethod
    def get_entity_type(self) -> Type[Message]:
        pass

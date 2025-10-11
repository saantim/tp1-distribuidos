import logging
import threading
from abc import ABC, abstractmethod
from typing import Type

from shared.entity import EOF, Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
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
        self._eof_collected = set()
        self._source: MessageMiddleware = source
        self._output: list[MessageMiddleware] = output
        self._intra_exchange: MessageMiddlewareExchange = intra_exchange

        self._session_active = False
        self._data_thread = None
        self._control_thread = None
        self._shutdown_event = threading.Event()

        self._shutdown_handler = ShutdownSignal(self._shutdown)

        logging.basicConfig(
            level=logging.INFO,
            format=self.__class__.__name__ + " - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
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

        # Stop consuming - sets the _should_stop flag
        try:
            self._source.stop_consuming()
            self._intra_exchange.stop_consuming()
        except Exception as e:
            logging.debug(f"Error stopping consumers: {e}")

        # Wait for threads - they should exit within ~2 seconds after stop flag is set
        if self._data_thread and self._data_thread.is_alive():
            self._data_thread.join(timeout=5.0)
            if self._data_thread.is_alive():
                logging.warning(f"action: data_thread_timeout | stage: {self._stage_name}")

        if self._control_thread and self._control_thread.is_alive():
            self._control_thread.join(timeout=5.0)
            if self._control_thread.is_alive():
                logging.warning(f"action: control_thread_timeout | stage: {self._stage_name}")

        # Close connections
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

    def _handle_eof(self, message: bytes) -> bool:
        if not EOF.is_type(message):
            return False

        logging.info(f"action: receive_EOF | stage: {self._stage_name} | from: {self._source}")
        self._session_active = False
        self._end_of_session()
        self._intra_exchange.send(EOFIntraExchange(str(self._index)).serialize())
        return True

    def _on_message_upstream(self, channel, method, properties, body: bytes) -> None:
        if not self._session_active:
            self._session_active = True

        if self._handle_eof(body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            for message in unpack_entity_batch(body, self.get_entity_type()):
                self._on_entity_upstream(channel, method, properties, message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _ = e
            logging.exception(f"action: batch_process | stage: {self._stage_name}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_message_intra_exchange(self, channel, method, _properties, body: bytes) -> None:
        if not EOFIntraExchange.is_type(body):
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        eof_message: EOFIntraExchange = EOFIntraExchange.deserialize(body)
        worker_id = eof_message.worker_id

        if self._leader:
            logging.info(f"action: got_intra_msg | from_worker: {worker_id} | stage: {self._stage_name}")
            self._eof_collected.add(worker_id)

        channel.basic_ack(delivery_tag=method.delivery_tag)

        # TODO: si uno de los workers nunca laburo, nunca activa su sesion, por ende no propaga su Intra.
        #   buscar luego una manera de fixear.
        if self._session_active:
            self._end_of_session()
            self._session_active = False
            self._intra_exchange.send(EOFIntraExchange(str(self._index)).serialize())

        if self._leader:
            self._flush_eof()

    def _flush_eof(self):
        if len(self._eof_collected) == self._instances:
            for output in self._output:
                output.send(EOF().serialize())
                logging.info(f"action: flush_eof | to: {output}")

    @abstractmethod
    def _end_of_session(self):
        pass

    @abstractmethod
    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        pass

    @abstractmethod
    def get_entity_type(self) -> Type[Message]:
        pass

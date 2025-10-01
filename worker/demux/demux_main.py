import logging
import os
import time
from typing import cast, Type

from shared.entity import EOF, MenuItem, Message, Store, Transaction, TransactionItem, User
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareQueue
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ, MessageMiddlewareQueueMQ
from shared.protocol import BatchPacket, Header, Packet
from shared.shutdown import ShutdownSignal


class Demux:
    def __init__(
        self,
        from_queue: MessageMiddlewareQueue,
        to_queue: MessageMiddleware,
        entity_class: Type[Message],
        replicas: int,
        shutdown_signal: ShutdownSignal,
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._entity_class = entity_class
        self._replicas = replicas
        self._shutdown_signal = shutdown_signal
        self._batch_count = 0
        self._message_count = 0
        self._batch_eof_seen = False

    def _on_message(self, channel, method, properties, body) -> None:
        if self._shutdown_signal.should_shutdown():
            self.stop()
            return

        try:
            try:
                eof_message = EOF.deserialize(body)
                self._handle_eof_coordination(eof_message)
                return
            except Exception:
                pass

            if len(body) < Header.SIZE:
                logging.error(f"message too short: {len(body)} bytes")
                return

            header_bytes = body[: Header.SIZE]
            payload_bytes = body[Header.SIZE :]
            header = Header.deserialize(header_bytes)
            packet = Packet.deserialize(header, payload_bytes)

            if not isinstance(packet, BatchPacket):
                logging.error(f"expected BatchPacket, got {type(packet)}")
                return

            batch_packet = cast(BatchPacket, packet)

            demux_start = time.time()
            for row in batch_packet.csv_rows:
                if self._shutdown_signal.should_shutdown():
                    logging.info("shutdown requested, stopping mid-batch")
                    self.stop()
                    return
                entity = self._entity_class.from_dict(row)
                serialized = entity.serialize()
                self._to_queue.send(serialized)
                self._message_count += 1
            demux_end = time.time()

            self._batch_count += 1
            logging.info(
                f"batch #{self._batch_count} of {self._entity_class.__name__} "
                f"took {demux_end - demux_start:.2f} seconds"
            )

            if batch_packet.eof:
                logging.info(f"Batch EOF received for {self._entity_class.__name__}")
                self._batch_eof_seen = True

                if self._replicas == 1:
                    self._from_queue.stop_consuming()
                    self._to_queue.send(EOF(0).serialize())
                    logging.info("Single replica: EOF sent downstream")
                else:
                    self._from_queue.send(EOF(1).serialize())
                    logging.info(f"Starting EOF coordination (replicas={self._replicas})")

            if self._batch_count % 100 == 0:
                logging.info(f"checkpoint: processed {self._batch_count} batches, " f"{self._message_count} messages")

        except Exception as e:
            logging.exception(f"error processing batch: {e}")

    def _handle_eof_coordination(self, eof_message: EOF) -> None:
        """Handle EOF coordination between replica workers."""
        if not self._batch_eof_seen:
            logging.warning("Received EOF coordination before batch EOF, ignoring")
            return

        logging.info(f"EOF coordination: {eof_message.metadata}/{self._replicas}")

        if eof_message.metadata + 1 == self._replicas:
            logging.info(f"All {self._replicas} replicas finished, sending final EOF downstream")
            self._from_queue.stop_consuming()
            self._to_queue.send(EOF(0).serialize())
            logging.info("Final EOF sent")
        else:
            eof_message.metadata += 1
            self._from_queue.send(eof_message.serialize())
            logging.info(f"EOF({eof_message.metadata}) forwarded to next replica")

    def start(self) -> None:
        logging.info(f"demux worker starting for {self._entity_class.__name__} " f"(replicas={self._replicas})")
        try:
            self._from_queue.start_consuming(self._on_message)
        except Exception as e:
            logging.error(f"consumer error: {e}")
        finally:
            self.stop()

    def stop(self) -> None:
        logging.info("stopping demux worker")
        try:
            self._from_queue.stop_consuming()
        except Exception as e:
            logging.error(f"consumer stop error: {e}")

        logging.info(f"demux worker stopped: {self._batch_count} batches, " f"{self._message_count} messages")


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM")
    to_queue_name: str = os.getenv("TO")
    to_type: str = os.getenv("TO_TYPE", "QUEUE")
    entity_type: str = os.getenv("ENTITY_TYPE")
    replicas: int = int(os.getenv("REPLICAS", "1"))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)

    shutdown_signal = ShutdownSignal()
    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)

    entity_map = {
        "Store": Store,
        "User": User,
        "Transaction": Transaction,
        "TransactionItem": TransactionItem,
        "MenuItem": MenuItem,
    }
    entity_class = entity_map.get(entity_type)
    if not entity_class:
        raise ValueError(f"Unknown ENTITY_TYPE: {entity_type}")

    if to_type == "EXCHANGE":
        to_queue = MessageMiddlewareExchangeRMQ(host, to_queue_name, ["common"])
    else:
        to_queue = MessageMiddlewareQueueMQ(host, to_queue_name)

    demux_worker = Demux(from_queue, to_queue, entity_class, replicas, shutdown_signal)
    demux_worker.start()


if __name__ == "__main__":
    main()

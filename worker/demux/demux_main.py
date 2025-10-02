import logging
import os
import time
from enum import IntEnum
from typing import cast, Dict, Tuple, Type

from shared.entity import EOF, MenuItem, Message, Store, Transaction, TransactionItem, User
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareQueue
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ, MessageMiddlewareQueueMQ
from shared.protocol import BatchPacket, Header, Packet, PacketType
from shared.shutdown import ShutdownSignal


class PublisherType(IntEnum):
    QUEUE = (1,)
    EXCHANGE = (2,)


ENTITY_MAP: Dict[int, Type[Message]] = {
    PacketType.STORE_BATCH: Store,
    PacketType.USERS_BATCH: User,
    PacketType.TRANSACTIONS_BATCH: Transaction,
    PacketType.TRANSACTION_ITEMS_BATCH: TransactionItem,
    PacketType.MENU_ITEMS_BATCH: MenuItem,
}

PUBLISHER_MAP: Dict[int, Tuple[str, PublisherType]] = {
    PacketType.STORE_BATCH: ("stores_source", PublisherType.EXCHANGE),
    PacketType.USERS_BATCH: ("users_source", PublisherType.QUEUE),
    PacketType.TRANSACTIONS_BATCH: ("transactions_source", PublisherType.QUEUE),
    PacketType.TRANSACTION_ITEMS_BATCH: ("transaction_items_source", PublisherType.QUEUE),
    PacketType.MENU_ITEMS_BATCH: ("menu_items_source", PublisherType.EXCHANGE),
}


class Demux:

    def __init__(
        self,
        from_queue: MessageMiddlewareQueue,
        publishers: Dict[PacketType, MessageMiddleware],
        shutdown_signal: ShutdownSignal,
    ) -> None:
        self._from_queue = from_queue
        self.publishers = publishers
        self._shutdown_signal = shutdown_signal
        self._to_queues: Dict[int, MessageMiddlewareQueue] = {}
        self._batch_count = 0
        self._message_count = 0

    def _on_message(self, channel, method, properties, body) -> None:
        if self._shutdown_signal.should_shutdown():
            self.stop()
            return

        try:
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
            packet_type = batch_packet.get_message_type()

            entity_class = ENTITY_MAP[packet_type]
            target_publisher = self.publishers.get(PacketType(packet_type))

            demux_start = time.time()
            for row in batch_packet.csv_rows:
                if self._shutdown_signal.should_shutdown():
                    logging.info("shutdown requested, stopping mid-batch")
                    self.stop()
                    return

                entity = entity_class.from_dict(row)
                target_publisher.send(entity.serialize())
                self._message_count += 1
            demux_end = time.time()

            if batch_packet.eof:
                target_publisher.send(EOF(0).serialize())
                logging.info(f"EOF sent for packet_type {type(batch_packet)}")

            self._batch_count += 1
            logging.info(
                f"batch #{self._batch_count} of type {type(batch_packet)} took {demux_end - demux_start:.2f} seconds"
            )

            if self._batch_count % 100 == 0:
                logging.info(f"checkpoint: processed {self._batch_count} batches, {self._message_count} messages")

        except Exception as e:
            logging.error(f"error processing batch: {e}")

    def start(self) -> None:
        logging.info("demux worker starting")
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
            logging.error(f"consumer close error: {e}")

        for queue in self._to_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.error(f"error closing queue: {e}")

        logging.info(f"demux worker stopped: {self._batch_count} batches, {self._message_count} messages")


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM", "demux_batches")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    shutdown_signal = ShutdownSignal()  # todo. capaz conviene agregar un callback custom para los workers.
    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)

    logging.getLogger("pika").setLevel(logging.WARNING)

    publishers: Dict[PacketType, MessageMiddleware] = {}
    for packet_type, (name, publisher_type) in PUBLISHER_MAP.items():
        ptype = PacketType(packet_type)
        if publisher_type == PublisherType.QUEUE:
            publishers[ptype] = MessageMiddlewareQueueMQ(host, name)
        elif publisher_type == PublisherType.EXCHANGE:
            publishers[ptype] = MessageMiddlewareExchangeRMQ(host, name, ["common"])

    demux_worker = Demux(from_queue, publishers, shutdown_signal)
    demux_worker.start()


if __name__ == "__main__":
    main()

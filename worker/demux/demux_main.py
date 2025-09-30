import logging
import os
from typing import cast, Dict, Type

from shared.entity import EOF, MenuItem, Message, Store, Transaction, TransactionItem, User
from shared.middleware.interface import MessageMiddlewareQueue
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import BatchPacket, Header, Packet, PacketType
from shared.shutdown import ShutdownSignal


ENTITY_MAP: Dict[int, Type[Message]] = {
    PacketType.STORE_BATCH: Store,
    PacketType.USERS_BATCH: User,
    PacketType.TRANSACTIONS_BATCH: Transaction,
    PacketType.TRANSACTION_ITEMS_BATCH: TransactionItem,
    PacketType.MENU_ITEMS_BATCH: MenuItem,
}

QUEUE_MAP: Dict[int, str] = {
    PacketType.STORE_BATCH: "stores_source",
    PacketType.USERS_BATCH: "users_source",
    PacketType.TRANSACTIONS_BATCH: "transactions_source",
    PacketType.TRANSACTION_ITEMS_BATCH: "transaction_items_source",
    PacketType.MENU_ITEMS_BATCH: "menu_items_source",
}


class Demux:

    def __init__(
        self, from_queue: MessageMiddlewareQueue, middleware_host: str, shutdown_signal: ShutdownSignal
    ) -> None:
        self._from_queue = from_queue
        self._middleware_host = middleware_host
        self._shutdown_signal = shutdown_signal
        self._to_queues: Dict[int, MessageMiddlewareQueue] = {}
        self._batch_count = 0
        self._message_count = 0

    def _get_target_queue(self, packet_type: int) -> MessageMiddlewareQueue:
        if packet_type not in self._to_queues:
            queue_name = QUEUE_MAP[packet_type]
            self._to_queues[packet_type] = MessageMiddlewareQueueMQ(self._middleware_host, queue_name)
            logging.info(f"created output queue: {queue_name}")
        return self._to_queues[packet_type]

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
            target_queue = self._get_target_queue(packet_type)

            for row in batch_packet.csv_rows:
                if self._shutdown_signal.should_shutdown():
                    logging.info("shutdown requested, stopping mid-batch")
                    self.stop()
                    return

                entity = entity_class.from_dict(row)
                target_queue.send(entity.serialize())
                self._message_count += 1

            if batch_packet.eof:
                target_queue.send(EOF(0).serialize())
                logging.info(f"EOF sent for packet_type {type(batch_packet)}")

            self._batch_count += 1

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
    from_queue_name: str = os.getenv("FROM_QUEUE", "demux_batches")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    shutdown_signal = ShutdownSignal()  # todo. capaz conviene agregar un callback custom para los workers.
    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)

    logging.getLogger("pika").setLevel(logging.WARNING)

    demux_worker = Demux(from_queue, host, shutdown_signal)
    demux_worker.start()


if __name__ == "__main__":
    main()

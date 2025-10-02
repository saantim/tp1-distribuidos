import logging
import os
import time
from typing import cast, Type

from shared.entity import EOF, MenuItem, Message, Store, Transaction, TransactionItem, User
from shared.middleware.interface import MessageMiddleware
from shared.protocol import BatchPacket, Header, Packet
from shared.shutdown import ShutdownSignal
from worker import utils


class Demux:
    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
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

    def _on_message(self, channel, method, properties, body) -> None:
        if self._shutdown_signal.should_shutdown():
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.stop()
            return

        if self._handle_eof(body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            if len(body) < Header.SIZE:
                logging.error(f"message too short: {len(body)} bytes")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            header_bytes = body[: Header.SIZE]
            payload_bytes = body[Header.SIZE :]

            header = Header.deserialize(header_bytes)
            packet = Packet.deserialize(header, payload_bytes)

            if not isinstance(packet, BatchPacket):
                logging.error(f"expected BatchPacket, got {type(packet)}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            batch_packet = cast(BatchPacket, packet)

            demux_start = time.time()
            for row in batch_packet.csv_rows:
                entity = self._entity_class.from_dict(row)
                serialized = entity.serialize()
                for queue in self._to_queue:
                    queue.send(serialized)
                self._message_count += 1
            demux_end = time.time()

            self._batch_count += 1
            logging.info(
                f"batch #{self._batch_count} of type {type(batch_packet)} took {demux_end - demux_start:.2f} seconds"
            )

            if batch_packet.eof:
                logging.info(f"Batch EOF received for {self._entity_class.__name__} sending EOF(0) to back of queue.")
                for queue in self._from_queue:
                    queue.send(EOF(0).serialize())

            if self._batch_count % 100 == 0:
                logging.info(f"checkpoint: processed {self._batch_count} batches, {self._message_count} messages")

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(f"error processing batch: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _handle_eof(self, body: bytes) -> bool:

        try:
            eof_msg: EOF = EOF.deserialize(body)
            logging.info(f"successful parsing of {eof_msg}")
        except Exception as e:
            _ = e
            return False

        logging.info(f"EOF received {eof_msg}, stopping demux worker...")
        for queue in self._from_queue:
            queue.stop_consuming()
        if eof_msg.metadata + 1 == self._replicas:
            logging.info("EOF sent to next stage")
            for queue in self._to_queue:
                queue.send(EOF(0).serialize())
        else:
            eof_msg.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_msg.serialize())

        self.stop()
        return True

    def start(self) -> None:
        logging.info(f"demux worker starting for {self._entity_class.__name__} " f"(replicas={self._replicas})")
        try:
            for queue in self._from_queue:
                queue.start_consuming(self._on_message)
            logging.info(f"demux worker finished: {self._batch_count} batches, " f"{self._message_count} messages")
        except Exception as e:
            logging.error(f"consumer error: {e}")

    def stop(self) -> None:
        pass


def main():
    from_queues: list = utils.get_input_queue()
    to_queues: list = utils.get_output_queue()
    entity_type: str = os.getenv("ENTITY_TYPE")
    replicas: int = int(os.getenv("REPLICAS", "1"))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)

    shutdown_signal = ShutdownSignal()

    # TODO: ADAPTAR A modo serialize_fn como los workers de los chicos.
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

    demux_worker = Demux(from_queues, to_queues, entity_class, replicas, shutdown_signal)
    demux_worker.start()


if __name__ == "__main__":
    main()

# worker/filters/filter_main.py
import dataclasses
import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable, Type

from shared.entity import EOF, Message, Transaction, TransactionItem
from shared.middleware.interface import MessageMiddleware
from shared.protocol import BatchPacket, TransactionItemsBatch, TransactionsBatch
from worker import utils
from worker.packer import is_batch, pack_batch, unpack_batch


logging.basicConfig(
    level=logging.INFO,
    format="FILTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Filter:
    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        filter_fn: Callable[[Any], bool],
        entity_class: Type[Message],
        batch_packet_class: Type[BatchPacket],
        replicas: int,
        batch_size: int = 500,
        next_step_allows_batch: list[bool] = None,  # Per-queue batching flags
    ):
        self.name: str = os.getenv("MODULE_NAME")
        self._replicas: int = replicas
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: list[MessageMiddleware] = to_queue
        self._filter_fn: Callable[[Any], bool] = filter_fn
        self._entity_class = entity_class
        self._batch_packet_class = batch_packet_class
        self._batch_size = batch_size

        # Per-queue output buffers
        self._next_step_allows_batch = next_step_allows_batch or [True] * len(to_queue)
        self._output_buffers: list[list[dict]] = [[] for _ in to_queue]

        self.received = 0
        self.passed = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        try:
            if self._handle_eof(body, channel, method):
                return

            if is_batch(body):
                for entity in unpack_batch(body, self._entity_class):
                    self.received += 1
                    serialized = entity.serialize()

                    if self._filter_fn(serialized):
                        self.passed += 1

                        # Send to each queue based on its batching preference
                        for idx, (queue, allows_batch) in enumerate(zip(self._to_queue, self._next_step_allows_batch)):
                            if allows_batch:
                                # Buffer for batching
                                self._output_buffers[idx].append(dataclasses.asdict(entity))
                                if len(self._output_buffers[idx]) >= self._batch_size:
                                    self._flush_buffer(idx)
                            else:
                                # Send individual message immediately
                                queue.send(serialized)
            else:
                # Fallback for individual messages
                self.received += 1
                if self._filter_fn(body):
                    self.passed += 1
                    for queue in self._to_queue:
                        queue.send(body)

            if self.received % 100000 == 0:
                logging.info(f"[{self.name}] checkpoint: received={self.received}, pass={self.passed}")

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _flush_buffer(self, queue_idx: int) -> None:
        """Flush buffer for a specific queue"""
        if not self._output_buffers[queue_idx]:
            return

        packed = pack_batch(self._output_buffers[queue_idx], self._batch_packet_class)
        self._to_queue[queue_idx].send(packed)
        self._output_buffers[queue_idx].clear()

    def _flush_all_buffers(self) -> None:
        """Flush all buffers (called on EOF)"""
        for idx in range(len(self._to_queue)):
            self._flush_buffer(idx)

    def _handle_eof(self, body: bytes, channel, method) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info(f"EOF received {eof_message}, stopping filter worker...")

        # Flush all remaining buffers
        self._flush_all_buffers()

        channel.basic_ack(delivery_tag=method.delivery_tag)

        for queue in self._from_queue:
            queue.stop_consuming()

        if eof_message.metadata + 1 == self._replicas:
            logging.info("EOF sent to next stage")
            for queue in self._to_queue:
                queue.send(EOF(0).serialize())
        else:
            eof_message.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_message.serialize())
                logging.info(f"sent eof with {eof_message} to my queue")

        self.stop()
        return True

    def start(self) -> None:
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    filter_module_name: str = os.getenv("MODULE_NAME")
    replicas: int = int(os.getenv("REPLICAS", 1))
    entity_type: str = os.getenv("ENTITY_TYPE")
    batch_size: int = int(os.getenv("BATCH_SIZE", 500))

    # Parse NEXT_STEP_ALLOWS_BATCH as comma-separated booleans
    next_step_str = os.getenv("NEXT_STEP_ALLOWS_BATCH", "")
    next_step_allows_batch = None
    if next_step_str:
        next_step_allows_batch = [s.strip().lower() == "true" for s in next_step_str.split(",")]

    from_queue: list[MessageMiddleware] = utils.get_input_queue()
    to_queue: list[MessageMiddleware] = utils.get_output_queue()
    filter_module: ModuleType = importlib.import_module(filter_module_name)

    entity_map = {
        "Transaction": (Transaction, TransactionsBatch),
        "TransactionItem": (TransactionItem, TransactionItemsBatch),
    }

    entity_class, batch_packet_class = entity_map.get(entity_type, (None, None))
    if not entity_class:
        raise ValueError(f"Unknown ENTITY_TYPE: {entity_type}")

    # Validate next_step_allows_batch matches number of output queues
    if next_step_allows_batch and len(next_step_allows_batch) != len(to_queue):
        raise ValueError(
            f"NEXT_STEP_ALLOWS_BATCH has {len(next_step_allows_batch)} values "
            f"but there are {len(to_queue)} output queues"
        )

    filter_worker = Filter(
        from_queue,
        to_queue,
        filter_module.filter_fn,
        entity_class,
        batch_packet_class,
        replicas,
        batch_size,
        next_step_allows_batch,
    )
    filter_worker.start()


if __name__ == "__main__":
    main()

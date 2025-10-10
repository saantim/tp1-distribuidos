# worker/router/router_main.py
import dataclasses
import importlib
import logging
import os
from collections import defaultdict
from types import ModuleType
from typing import Any, Callable, Type

from shared.entity import EOF, Transaction
from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import TransactionsBatch
from worker import utils
from worker.packer import is_batch, pack_batch, unpack_batch


logging.basicConfig(
    level=logging.INFO,
    format="ROUTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Router:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        router_fn: Callable[[Any], str],
        entity_class: Type[Transaction],
        batch_packet_class: Type[TransactionsBatch],
        replicas: int,
    ) -> None:
        self.name: str = os.getenv("MODULE_NAME")
        self._replicas: int = replicas
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: dict[str, MessageMiddleware] = {}
        self._router_fn: Callable[[Any], str] = router_fn
        self._entity_class = entity_class
        self._batch_packet_class = batch_packet_class
        self.received = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        try:
            if self._handle_eof(body):
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Check if it's a batch and route by destination
            if is_batch(body):
                # Group entities by routing destination
                route_groups = defaultdict(list)

                for entity in unpack_batch(body, self._entity_class):
                    self.received += 1
                    serialized = entity.serialize()
                    queue_name = self._router_fn(serialized)
                    route_groups[queue_name].append(dataclasses.asdict(entity))

                # Send a batch to each destination
                for queue_name, entities in route_groups.items():
                    if queue_name not in self._to_queue:
                        self._to_queue[queue_name] = MessageMiddlewareQueueMQ(
                            host=os.getenv(utils.MIDDLEWARE_HOST), queue_name=queue_name
                        )

                    # Pack and send batch
                    packed_batch = pack_batch(entities, self._batch_packet_class)
                    self._to_queue[queue_name].send(packed_batch)
            else:
                # Individual message fallback
                self.received += 1
                queue_name: str = self._router_fn(body)
                if queue_name not in self._to_queue:
                    self._to_queue[queue_name] = MessageMiddlewareQueueMQ(
                        host=os.getenv(utils.MIDDLEWARE_HOST), queue_name=queue_name
                    )
                self._to_queue[queue_name].send(body)

            if self.received % 100000 == 0:
                logging.info(f"[{self.name}] checkpoint: routed={self.received}")

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _handle_eof(self, body: bytes) -> bool:
        try:
            EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info("EOF received, forwarding to all routed queues")

        for queue_name, queue in self._to_queue.items():
            logging.info(f"Sending EOF to queue {queue_name}")
            queue.send(EOF(0).serialize())

        for queue in self._from_queue:
            queue.stop_consuming()

        self.stop()
        return True

    def start(self) -> None:
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    router_module_name: str = os.getenv("MODULE_NAME")
    replicas: int = int(os.getenv("REPLICAS", 1))

    from_queue: list[MessageMiddleware] = utils.get_input_queue()
    router_module: ModuleType = importlib.import_module(router_module_name)

    entity_class = Transaction
    batch_packet_class = TransactionsBatch

    router_worker = Router(from_queue, router_module.router_fn, entity_class, batch_packet_class, replicas)
    router_worker.start()


if __name__ == "__main__":
    main()

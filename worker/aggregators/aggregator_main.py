import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.entity import EOF
from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import Packet


logging.basicConfig(
    level=logging.INFO,
    format="AGGREGATOR - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Aggregator:

    def __init__(
        self,
        from_queue: MessageMiddleware,
        to_queue: MessageMiddleware,
        aggregator_fn: Callable[[Any, Packet], Any],
        replicas: int,
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._aggregator_fn = aggregator_fn
        self._aggregated: Any = None
        self._replicas = replicas

    def _on_message(self, channel, method, properties, body) -> None:
        if not self._handle_eof(body):
            self._aggregated = self._aggregator_fn(self._aggregated, body)

    def start(self) -> None:
        logging.info("Starting aggregator worker...")
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("Stopping aggregator worker...")
        # self._to_queue.close()
        pass

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info("EOF received, stopping worker...")
        self._from_queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:

            self._to_queue.send(self._aggregated.serialize())
            self._to_queue.send(EOF(0).serialize())
            logging.info("EOF sent to next stage")
        else:
            eof_message.metadata += 1
            self._from_queue.send(eof_message.serialize())
        self.stop()

        return True


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM_QUEUE")
    to_queue_name: str = os.getenv("TO_QUEUE")
    aggregator_module_name: str = os.getenv("MODULE_NAME")
    stage_replicas: int = int(os.getenv("REPLICAS"))

    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)
    to_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, to_queue_name)
    aggregator_module: ModuleType = importlib.import_module(aggregator_module_name)

    aggregator_worker = Aggregator(from_queue, to_queue, aggregator_module.aggregator_fn, stage_replicas)
    aggregator_worker.start()


if __name__ == "__main__":
    main()

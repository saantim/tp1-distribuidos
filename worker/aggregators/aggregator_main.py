import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import Packet
from worker.types import EOF


logging.basicConfig(
    level=logging.INFO,
    format="AGGREGATOR - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Aggregator:

    def __init__(
        self, from_queue: MessageMiddleware, to_queue: MessageMiddleware, aggregator_fn: Callable[[Any, Packet], Any]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._aggregator_fn = aggregator_fn
        self._aggregated: Any = None

    def _on_message(self, channel, method, properties, body) -> None:
        try:
            EOF.deserialize(body)
            logging.info("EOF received, stopping aggregator worker...")
            self.stop()
            return
        except Exception:
            pass
        self._aggregated = self._aggregator_fn(self._aggregated, body)

    def start(self) -> None:
        logging.info("Starting aggregator worker...")
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("Stopping aggregator worker...")
        self._from_queue.stop_consuming()
        self._to_queue.send(self._aggregated)
        self._to_queue.send(EOF().serialize())
        self._to_queue.close()


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM_QUEUE")
    to_queue_name: str = os.getenv("TO_QUEUE")
    aggregator_module_name: str = os.getenv("MODULE_NAME")

    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)
    to_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, to_queue_name)
    aggregator_module: ModuleType = importlib.import_module(aggregator_module_name)

    aggregator_worker = Aggregator(from_queue, to_queue, aggregator_module.aggregator_fn)
    aggregator_worker.start()


if __name__ == "__main__":
    main()

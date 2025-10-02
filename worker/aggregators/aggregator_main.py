import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.entity import EOF
from shared.middleware.interface import MessageMiddleware
from shared.protocol import Packet
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="AGGREGATOR - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Aggregator:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        aggregator_fn: Callable[[Any, Packet], Any],
        replicas: int,
    ) -> None:
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: list[MessageMiddleware] = to_queue
        self._aggregator_fn: Callable[[Any, Packet], Any] = aggregator_fn
        self._aggregated: Any = None
        self._replicas: int = replicas

    def _on_message(self, channel, method, properties, body) -> None:
        if not self._handle_eof(body):
            self._aggregated = self._aggregator_fn(self._aggregated, body)

    def start(self) -> None:
        logging.info("Starting aggregator worker...")
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

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
        for queue in self._from_queue:
            queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            for queue in self._to_queue:
                queue.send(self._aggregated.serialize())
                queue.send(EOF(0).serialize())
                logging.info("EOF sent to next stage")
        else:
            eof_message.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_message.serialize())
        self.stop()

        return True


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    aggregator_module_name: str = os.getenv("MODULE_NAME")
    stage_replicas: int = int(os.getenv("REPLICAS"))

    aggregator_module: ModuleType = importlib.import_module(aggregator_module_name)
    from_queues: list[MessageMiddleware] = utils.get_input_queue()
    to_queues: list[MessageMiddleware] = utils.get_output_queue()

    aggregator_worker = Aggregator(from_queues, to_queues, aggregator_module.aggregator_fn, stage_replicas)
    aggregator_worker.start()


if __name__ == "__main__":
    main()

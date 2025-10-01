import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

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
        self, from_queue: MessageMiddleware, to_queue: MessageMiddleware, aggregator_fn: Callable[[Any, Packet], Any]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._aggregator_fn = aggregator_fn
        self._aggregated: Any = None
        self._eof_handler = utils.get_eof_handler(from_queue, to_queue)

    def _on_message(self, channel, method, properties, body) -> None:

        def send_results():
            if self._aggregated:
                self._to_queue.send(self._aggregated.serialize())

        if not self._eof_handler.handle_eof(body, on_eof_callback=send_results):
            self._aggregated = self._aggregator_fn(self._aggregated, body)

    def start(self) -> None:
        logging.info("Starting aggregator worker...")
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("Stopping aggregator worker...")
        # self._to_queue.close()
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    aggregator_module_name: str = os.getenv("MODULE_NAME")

    aggregator_module: ModuleType = importlib.import_module(aggregator_module_name)
    from_queue = utils.get_input_queue()
    to_queue = utils.get_output_queue()

    aggregator_worker = Aggregator(from_queue, to_queue, aggregator_module.aggregator_fn)
    aggregator_worker.start()


if __name__ == "__main__":
    main()

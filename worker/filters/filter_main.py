import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.middleware.interface import MessageMiddleware
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="FILTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Filter:

    def __init__(
        self, from_queue: list[MessageMiddleware], to_queue: list[MessageMiddleware], filter_fn: Callable[[Any], bool]
    ) -> None:
        self.name = os.getenv("MODULE_NAME")
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._filter_fn = filter_fn
        self._eof_handler = utils.get_eof_handler(from_queue, to_queue)

        self.received = 0
        self.passed = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        self.received += 1
        print(body.decode("utf-8"))
        if not self._eof_handler.handle_eof(body):
            if self._filter_fn(body):
                self.passed += 1
                for queue in self._to_queue:
                    queue.send(body)

        if self.received % 100000 == 0:
            logging.info(f"[{self.name}] checkpoint: " f"received={self.received}, pass={self.passed}")

    def start(self) -> None:
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass
        # todo: remove queues
        # self._from_queue.close()


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    filter_module_name: str = os.getenv("MODULE_NAME")

    from_queue = utils.get_input_queue()
    to_queue = utils.get_output_queue()
    filter_module: ModuleType = importlib.import_module(filter_module_name)

    filter_worker = Filter(from_queue, to_queue, filter_module.filter_fn)
    filter_worker.start()


if __name__ == "__main__":
    main()

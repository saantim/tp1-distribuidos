import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.middleware.interface import MessageMiddleware
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="MERGER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Merger:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        merger_fn: Callable[[Any, Any], Any],
    ) -> None:
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: list[MessageMiddleware] = to_queue
        self._merger_fn: Callable[[Any, Any], Any] = merger_fn
        self._merged: Any = None
        self.eof_handler = utils.get_eof_handler(from_queue, to_queue)

    def _on_message(self, channel, method, properties, body) -> None:

        def _flush():
            for queue in self._to_queue:
                queue.send(self._merged.serialize())

        if not self.eof_handler.handle_eof(body, on_eof_callback=_flush):
            self._merged = self._merger_fn(self._merged, body)

    def start(self) -> None:
        logging.info("Starting merger worker")
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("Stopping merger worker")
        # self._from_queue.stop_consuming()
        # self._to_queue.send(self._merged)
        # self._to_queue.send(EOF(0).serialize())
        # self._to_queue.close()
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)

    merger_module_name: str = os.getenv("MODULE_NAME")

    from_queue = utils.get_input_queue()
    to_queue = utils.get_output_queue()

    merger_module: ModuleType = importlib.import_module(merger_module_name)
    merger_worker = Merger(from_queue, to_queue, merger_module.merger_fn)
    merger_worker.start()


if __name__ == "__main__":
    main()

import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.entity import EOF
from shared.middleware.interface import MessageMiddleware
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="ENRICHER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Enricher:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        enricher_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        build_enricher_fn: Callable[[Any, Any], Any],
        enricher_fn: Callable[[Any, Any], Any],
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._enricher_queue = enricher_queue
        self._build_enricher_fn = build_enricher_fn
        self._enricher_fn = enricher_fn
        self._enricher = None
        self._work_eof_handler = utils.get_eof_handler(from_queue, to_queue)

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        if not self._work_eof_handler.handle_eof(body):
            enriched_message = self._enricher_fn(self._enricher, body)
            for queue in self._to_queue:
                queue.send(enriched_message.serialize())

    def _enricher_msg(self, channel, method, properties, body: bytes) -> None:
        try:
            EOF.deserialize(body)
            logging.info("Enricher build phase complete, starting work phase")
            logging.info(f"Enricher data loaded: {self._enricher}")

            for queue in self._enricher_queue:
                queue.stop_consuming()

            for queue in self._from_queue:
                queue.start_consuming(self._on_message)

            return
        except Exception:
            pass

        self._enricher = self._build_enricher_fn(self._enricher, body)

    def start(self) -> None:
        logging.info("Starting enricher build phase")
        for queue in self._enricher_queue:
            queue.start_consuming(self._enricher_msg)

    def stop(self) -> None:
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    enricher_module_name: str = os.getenv("MODULE_NAME")
    enricher_module: ModuleType = importlib.import_module(enricher_module_name)

    from_queue = utils.get_input_queue()
    to_queue = utils.get_output_queue()
    enricher_queue = utils.get_enricher_queue()

    enricher_worker = Enricher(
        from_queue,
        enricher_queue,
        to_queue,
        enricher_module.build_enricher_fn,
        enricher_module.enricher_fn,
    )
    enricher_worker.start()


if __name__ == "__main__":
    main()

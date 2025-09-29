import importlib
import os
import logging
from types import ModuleType
from typing import Any, Callable

from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from worker.types import EOF

logging.basicConfig(
    level=logging.INFO,
    format="ENRICHER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Enricher:

    def __init__(
        self,
        from_queue: MessageMiddleware,
        enricher_queue: MessageMiddleware,
        to_queue: MessageMiddleware,
        build_enricher_fn: Callable[[Any, Any], Any],
        enricher_fn: Callable[[Any, Any], Any],
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._enricher_queue = enricher_queue
        self._build_enricher_fn = build_enricher_fn
        self._enricher_fn = enricher_fn
        self._enricher = None

    def _on_message(self, channel, method, properties, body) -> None:
        try:
            EOF.deserialize(body)
            self.stop()
            return
        except ValueError:
            pass
        enriched_message = self._enricher_fn(self._enricher, body)
        self._to_queue.send(enriched_message)

    def _enricher_msg(self, channel, method, properties, body) -> None:
        try:
            EOF.deserialize(body)
            logging.info("EOF received, stopping enricher + start consuming from queue")
            self._from_queue.start_consuming(self._on_message)
            self._enricher_queue.close()
            return
        except Exception:
            pass
        self._enricher = self._build_enricher_fn(self._enricher, body)

    def start(self) -> None:
        logging.info("Starting Enricher!")
        logging.info("started consuming enricher queue")
        self._enricher_queue.start_consuming(self._enricher_msg)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send(EOF().serialize())
        self._to_queue.close()


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM_QUEUE")
    enricher_queue_name: str = os.getenv("ENRICHER_QUEUE")
    to_queue_name: str = os.getenv("TO_QUEUE")
    enricher_module_name: str = os.getenv("MODULE_NAME")

    logging.info(f"host = {host}")
    logging.info(f"from_queue_name = {from_queue_name}")
    logging.info(f"enricher_queue_name = {enricher_queue_name}")
    logging.info(f"to_queue_name = {to_queue_name}")
    logging.info(f"enricher_module_name = {enricher_module_name}")

    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)
    to_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, to_queue_name)
    enricher_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, enricher_queue_name)
    enricher_module: ModuleType = importlib.import_module(enricher_module_name)

    enricher_worker = Enricher(
        from_queue, enricher_queue, to_queue, enricher_module.build_enricher_fn, enricher_module.enricher_fn
    )
    enricher_worker.start()


if __name__ == "__main__":
    main()

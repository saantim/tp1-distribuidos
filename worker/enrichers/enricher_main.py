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
        from_queue: MessageMiddleware,
        enricher_queue: MessageMiddleware,
        to_queue: MessageMiddleware,
        build_enricher_fn: Callable[[Any, Any], Any],
        enricher_fn: Callable[[Any, Any], Any],
        replicas: int,
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._enricher_queue = enricher_queue
        self._build_enricher_fn = build_enricher_fn
        self._enricher_fn = enricher_fn
        self._enricher = None
        self._replicas = replicas

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        if not self._handle_eof(body):
            enriched_message = self._enricher_fn(self._enricher, body)
            self._to_queue.send(enriched_message.serialize())

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info("EOF received, stopping enricher worker...")
        self._from_queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            self._to_queue.send(EOF(0).serialize())
            logging.info("EOF sent to next stage")
        else:
            eof_message.metadata += 1
            self._from_queue.send(eof_message.serialize())
        self.stop()

        return True

    def _enricher_msg(self, channel, method, properties, body: bytes) -> None:
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
        logging.info(f"started consuming enricher queue: {self._enricher_queue}")
        self._enricher_queue.start_consuming(self._enricher_msg)

    def stop(self) -> None:
        # self._from_queue.stop_consuming()
        # self._to_queue.send(EOF().serialize())
        # self._to_queue.close()
        pass


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    enricher_module_name: str = os.getenv("MODULE_NAME")
    stage_replicas: int = int(os.getenv("REPLICAS"))
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
        stage_replicas,
    )
    enricher_worker.start()


if __name__ == "__main__":
    main()

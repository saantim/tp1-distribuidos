import logging
import os
import importlib
from types import ModuleType
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from typing import Any, Callable
from shared.middleware.interface import MessageMiddlewareQueue
from shared.entity import EOF

logging.basicConfig(
    level=logging.INFO,
    format="FILTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Filter:

    def __init__(
        self,
        from_queue: MessageMiddlewareQueue,
        to_queue: MessageMiddlewareQueue,
        filter_fn: Callable[[Any], bool],
        replicas: int,
    ) -> None:
        self._replicas = replicas
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._filter_fn = filter_fn

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        if not self._handle_eof(body):
            if self._filter_fn(body):
                self._to_queue.send(body)

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info("EOF received, stopping filter worker...")
        self._from_queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            self._to_queue.send(EOF(0).serialize())
            logging.info("EOF sent to next stage")
        else:
            eof_message.metadata += 1
            self._from_queue.send(eof_message.serialize())
        self.stop()

        return True

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass
        # todo: remove queues
        # self._from_queue.close()


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM_QUEUE")
    to_queue_name: str = os.getenv("TO_QUEUE")
    filter_module_name: str = os.getenv("MODULE_NAME")
    stage_replicas: int = int(os.getenv("REPLICAS"))

    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)
    to_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, to_queue_name)
    filter_module: ModuleType = importlib.import_module(filter_module_name)

    filter_worker = Filter(from_queue, to_queue, filter_module.filter_fn, stage_replicas)
    filter_worker.start()


if __name__ == "__main__":
    main()

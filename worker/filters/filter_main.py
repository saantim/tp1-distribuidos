import os
import importlib
from types import ModuleType
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from typing import Any, Callable
from shared.middleware.interface import MessageMiddlewareQueue


class Filter:

    def __init__(
        self, from_queue: MessageMiddlewareQueue, to_queue: MessageMiddlewareQueue, filter_fn: Callable[[Any], bool]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._filter_fn = filter_fn

    def _on_message(self, channel, method, properties, body) -> None:
        if body == "EOF":
            self.stop()
        elif self._filter_fn(body):
            self._to_queue.send(body)

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()


def main():
    host: str = os.getenv("MIDDLEWARE_HOST")
    from_queue_name: str = os.getenv("FROM_QUEUE")
    to_queue_name: str = os.getenv("TO_QUEUE")
    filter_module_name: str = os.getenv("FILTER_MODULE_NAME")

    from_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, from_queue_name)
    to_queue: MessageMiddlewareQueueMQ = MessageMiddlewareQueueMQ(host, to_queue_name)
    filter_module: ModuleType = importlib.import_module(filter_module_name)

    filter_worker = Filter(from_queue, to_queue, filter_module.filter_fn)
    filter_worker.start()


if __name__ == "__main__":
    main()

from typing import Any, Callable

from middleware.interface import MessageMiddlewareQueue


class Filter:

    def __init__(
        self, from_queue: MessageMiddlewareQueue, to_queue: MessageMiddlewareQueue, filter_fn: Callable[[Any], bool]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._filter_fn = filter_fn

    def _on_message(self, message: bytes) -> None:
        if message == "EOF":
            self.stop()
        elif self._filter_fn(message):
            self._to_queue.send(message)

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()

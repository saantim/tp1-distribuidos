from typing import Any, Callable

from shared.middleware.interface import MessageMiddlewareQueue


class Merger:

    def __init__(
        self, from_queue: MessageMiddlewareQueue, to_queue: MessageMiddlewareQueue, merger_fn: Callable[[Any, Any], Any]
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._merger_fn = merger_fn
        self._merged = None

    def _on_message(self, message: bytes) -> None:

        if message == "EOF":
            self._to_queue.send(self._merged)
            self.stop()
            return
        self._merged = self._merger_fn(self._merged, message)

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()

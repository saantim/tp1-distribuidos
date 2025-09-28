from typing import Callable, Any
from middleware import MessageMiddlewareQueue, MessageMiddleware


class Aggregator:

    def __init__(self,
                 from_queue: MessageMiddleware,
                 to_queue: MessageMiddleware,
                 aggregator_fn: Callable[[Any, Any], Any]) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._aggregator_fn = aggregator_fn
        self._aggregated = None

    def _on_message(self, message: str) -> None:
        if message == "EOF":
            self._to_queue.send(self._aggregated)
            self.stop()
            return
        self._aggregated = self._aggregator_fn(self._aggregated, message)

    def start(self) -> None:
        self._from_queue.start_consuming(self._on_message)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()

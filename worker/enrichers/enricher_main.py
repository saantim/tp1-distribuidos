from typing import Any, Callable

from shared.middleware.interface import MessageMiddlewareQueue


class Enricher:

    def __init__(
        self,
        from_queue: MessageMiddlewareQueue,
        to_queue: MessageMiddlewareQueue,
        enricher_queue: MessageMiddlewareQueue,
        build_enricher_fn: Callable[[Any, Any], Any],
        enricher_fn: Callable[[Any, Any], Any],
    ) -> None:
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._enricher_queue = enricher_queue
        self._build_enricher_fn = build_enricher_fn
        self._enricher_fn = enricher_fn
        self._enricher = None

    def _on_message(self, message: str) -> None:
        if message == "EOF":
            self.stop()
            return
        enriched_message = self._enricher_fn(self._enricher, message)
        self._to_queue.send(enriched_message)

    def _enricher_msg(self, message: str) -> None:
        if message == "EOF":
            self._from_queue.start_consuming(self._on_message)
            self._enricher_queue.close()
            return
        self._enricher = self._build_enricher_fn(self._enricher, message)

    def start(self) -> None:
        self._enricher_queue.start_consuming(self._enricher_msg)

    def stop(self) -> None:
        self._from_queue.stop_consuming()
        self._to_queue.send("EOF")
        self._to_queue.close()

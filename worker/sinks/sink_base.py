import logging
from abc import ABC, abstractmethod

from shared.entity import Message
from shared.middleware.interface import MessageMiddleware, MessageMiddlewareExchange
from worker.base import WorkerBase


class SinkBase(WorkerBase, ABC):
    """
    Collects results from pipeline, formats them using a query-specific function,
    and sends formatted results to the query-specific results queue.
    """

    def __init__(
        self,
        instances: int,
        index: int,
        stage_name: str,
        source: MessageMiddleware,
        output: list[MessageMiddleware],
        intra_exchange: MessageMiddlewareExchange,
    ):
        super().__init__(instances, index, stage_name, source, output, intra_exchange)
        self._results_collected: list[Message] = []

    @abstractmethod
    def format_fn(self, results_collected: list[Message]) -> bytes: ...

    def _end_of_session(self):
        formatted_results: bytes = self.format_fn(self._results_collected)
        if formatted_results:
            for output in self._output:
                output.send(formatted_results)
            logging.info(f"Sent batch results ({len(formatted_results)} bytes)")

    def _on_entity_upstream(self, channel, method, properties, message: Message) -> None:
        self._results_collected.append(message)

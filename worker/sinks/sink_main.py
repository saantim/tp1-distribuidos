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
    format="SINK - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Sink:
    """
    Collects results from pipeline, formats them using a query-specific function,
    and sends formatted results to the query-specific results queue.
    Supports both streaming (one-at-a-time) and batch (collect-all) modes.
    """

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        results_queue: list[MessageMiddleware],
        format_fn: Callable[[Any], bytes],
        stream_mode: bool = False,
    ) -> None:
        self._from_queue = from_queue
        self._results_queue = results_queue
        self._format_fn = format_fn
        self._stream_mode = stream_mode
        self._results_collected = [] if not stream_mode else None
        self._eof_count = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        if not self._handle_eof(body):
            if self._stream_mode:
                try:
                    formatted = self._format_fn([body])
                    if formatted:
                        for queue in self._results_queue:
                            queue.send(formatted)
                except Exception as e:
                    logging.error(f"Error formatting/sending result: {e}")
            else:
                self._results_collected.append(body)

    def _handle_eof(self, body: bytes) -> bool:
        try:
            _ = EOF.deserialize(body)
        except Exception:
            return False

        self._eof_count += 1
        logging.info(f"EOF received for sink: {self._results_queue}!")

        for queue in self._from_queue:
            queue.stop_consuming()

        if not self._stream_mode and self._results_collected:
            try:
                logging.info(f"Formatting {len(self._results_collected)} collected results")
                formatted_results = self._format_fn(self._results_collected)
                if formatted_results:
                    for queue in self._results_queue:
                        queue.send(formatted_results)
                    logging.info(f"Sent batch results ({len(formatted_results)} bytes)")
            except Exception as e:
                logging.error(f"Error formatting/sending batch results: {e}")

        for queue in self._results_queue:
            queue.send(EOF(0).serialize())
        logging.info("EOF sent to results queue")

        self._eof_count = 0
        self._results_collected = [] if not self._stream_mode else None

        self.stop()
        return True

    def start(self) -> None:
        mode = "streaming" if self._stream_mode else "batch"
        logging.info(f"Starting sink worker in {mode} mode")
        for from_queue in self._from_queue:
            from_queue.start_consuming(self._on_message)

    @staticmethod
    def stop() -> None:
        logging.info("Sink worker stopped, ready for next session")


def main():
    from_queue = utils.get_input_queue()
    results_queue = utils.get_output_queue()
    sink_module_name = os.getenv("MODULE_NAME")
    stream_mode = os.getenv("STREAM_MODE", "false").lower() == "true"

    logging.info("Sink configuration:")
    logging.info(f"  MODULE_NAME: {sink_module_name}")
    logging.info(f"  STREAM_MODE: {stream_mode}")

    logging.getLogger("pika").setLevel(logging.WARNING)

    sink_module: ModuleType = importlib.import_module(sink_module_name)

    sink_worker = Sink(from_queue, results_queue, sink_module.format_fn, stream_mode)
    sink_worker.start()


if __name__ == "__main__":
    main()

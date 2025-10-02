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
    format="FILTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Filter:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        filter_fn: Callable[[Any], bool],
        replicas: int,
    ) -> None:
        self.name: str = os.getenv("MODULE_NAME")
        self._replicas: int = replicas
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: list[MessageMiddleware] = to_queue
        self._filter_fn: Callable[[Any], bool] = filter_fn

        self.received = 0
        self.passed = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        logging.info(f"Message received: {body.decode()}")
        self.received += 1
        if not self._handle_eof(body):
            if self._filter_fn(body):
                self.passed += 1
                for queue in self._to_queue:
                    queue.send(body)

        if self.received % 100000 == 0:
            logging.info(f"[{self.name}] checkpoint: " f"received={self.received}, pass={self.passed}")

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
            logging.info(f"successful parsing of {eof_message}")
        except Exception as e:
            _ = e
            return False

        logging.info(f"EOF received {eof_message}, stopping filter worker...")
        for queue in self._from_queue:
            queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            logging.info("EOF sent to next stage")
            for queue in self._to_queue:
                queue.send(EOF(0).serialize())
        else:
            eof_message.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_message.serialize())

        self.stop()
        return True

    def start(self) -> None:
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass
        # todo: remove queues
        # self._from_queue.close()


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    filter_module_name: str = os.getenv("MODULE_NAME")
    replicas: int = int(os.getenv("REPLICAS", 1))

    from_queue: list[MessageMiddleware] = utils.get_input_queue()
    to_queue: list[MessageMiddleware] = utils.get_output_queue()
    filter_module: ModuleType = importlib.import_module(filter_module_name)

    filter_worker = Filter(from_queue, to_queue, filter_module.filter_fn, replicas)
    filter_worker.start()


if __name__ == "__main__":
    main()

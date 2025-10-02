import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable

from shared.entity import EOF
from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="FILTER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Router:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        router_fn: Callable[[Any], str],
        replicas: int,
    ) -> None:
        self.name: str = os.getenv("MODULE_NAME")
        self._replicas: int = replicas
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: dict[str, MessageMiddleware] = {}
        self._router_fn: Callable[[Any], str] = router_fn
        self.received = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        self.received += 1

        if not self._handle_eof(body):
            queue_name: str = self._router_fn(body)
            if queue_name not in self._to_queue:
                self._to_queue[queue_name] = MessageMiddlewareQueueMQ(
                    host=os.getenv(utils.MIDDLEWARE_HOST), queue_name=queue_name
                )
            output_queue = self._to_queue[queue_name]
            output_queue.send(body)

        if self.received % 100000 == 0:
            logging.info(f"[{self.name}] checkpoint: " f"routed={self.received}")

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info(f"EOF received {eof_message}, stopping filter worker...")
        for queue in self._from_queue:
            queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            logging.info("EOF will be sent to next stage")
            for n, queue in self._to_queue.items():
                logging.info(f"sent EOF to queue {n}")
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


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    router_module_name: str = os.getenv("MODULE_NAME")
    replicas: int = int(os.getenv("REPLICAS", 1))

    from_queue: list[MessageMiddleware] = utils.get_input_queue()
    router_module: ModuleType = importlib.import_module(router_module_name)

    router_worker = Router(from_queue, router_module.router_fn, replicas)
    router_worker.start()


if __name__ == "__main__":
    main()

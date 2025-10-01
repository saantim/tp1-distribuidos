import logging

from shared.middleware.interface import MessageMiddleware
from worker import utils


logging.basicConfig(
    level=logging.INFO,
    format="FORWARDER - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Forwarder:
    def __init__(self, from_exchange: MessageMiddleware, to_queue: MessageMiddleware):
        self._from_exchange = from_exchange
        self._to_queue = to_queue
        self._eof_handler = utils.get_eof_handler(from_exchange, to_queue)
        self._message_count = 0

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        if not self._eof_handler.handle_eof(body):
            self._to_queue.send(body)
            self._message_count += 1

            if self._message_count % 10000 == 0:
                logging.info(f"Forwarded {self._message_count} messages")

    def start(self) -> None:
        logging.info("Starting forwarder worker")
        self._from_exchange.start_consuming(self._on_message)


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)

    from_exchange = utils.get_input_queue()
    to_queue = utils.get_output_queue()

    forwarder_worker = Forwarder(from_exchange, to_queue)
    forwarder_worker.start()


if __name__ == "__main__":
    main()

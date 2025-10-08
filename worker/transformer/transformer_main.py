"""
Transformer worker: receives raw CSV batches, parses into entities, and forwards.
"""

import importlib
import logging
import os
import time

from shared.middleware.interface import MessageMiddleware
from shared.shutdown import ShutdownSignal
from worker import utils
from worker.packer import get_batch_metadata, is_raw_batch, pack_entity_batch, unpack_raw_batch


class Transformer:
    """
    Transformer worker that converts raw CSV batches into entity batches.

    Expects a transform module with:
    - parse_csv_row(csv_row: str) -> dict
    - create_entity(row_dict: dict) -> Message
    """

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        transform_module,
        replicas: int,
        replica_id: int,
        shutdown_signal: ShutdownSignal,
    ):
        self._from_queue = from_queue
        self._to_queue = to_queue
        self._transform_module = transform_module
        self._replicas = replicas
        self._replica_id = replica_id
        self._shutdown_signal = shutdown_signal
        self._batch_count = 0
        self._row_count = 0

    def _on_message(self, channel, method, properties, body):
        """Handle incoming raw batch message."""
        if self._shutdown_signal.should_shutdown():
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.stop()
            return

        if self._handle_eof(body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if not is_raw_batch(body):
            logging.error("Expected raw batch packet")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            metadata = get_batch_metadata(body)
            if metadata["eof"]:
                # TODO: NEW EOF LOGIC (Tomi)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            transform_start = time.time()
            entities = []

            for csv_row in unpack_raw_batch(body):
                row_dict = self._transform_module.parse_csv_row(csv_row)
                entity = self._transform_module.create_entity(row_dict)
                entities.append(entity)
                self._row_count += 1

            entity_batch_bytes = pack_entity_batch(entities)

            for queue in self._to_queue:
                queue.send(entity_batch_bytes)

            transform_end = time.time()
            self._batch_count += 1

            logging.info(
                f"action: transform_batch | batch: {self._batch_count} | "
                f"entities: {len(entities)} | duration: {transform_end - transform_start:.3f}s"
            )

            if self._batch_count % 100 == 0:
                logging.info(f"checkpoint: {self._batch_count} batches, {self._row_count} rows")

            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.exception(f"error transforming batch: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    @staticmethod
    def _handle_eof(_body: bytes) -> bool:
        """
        Handle EOF coordination between replicas.

        Returns:
            True if EOF was handled, False otherwise
        """
        # TODO: Insert new EOF handler here (Tomi)
        return True

    def start(self):
        """Start consuming and transforming batches."""
        logging.info(
            f"action: transformer_start | module: {self._transform_module.__name__} | "
            f"replica: {self._replica_id}/{self._replicas}"
        )

        try:
            for queue in self._from_queue:
                queue.start_consuming(self._on_message)

            logging.info(f"action: transformer_complete | batches: {self._batch_count} | rows: {self._row_count}")
        except Exception as e:
            logging.exception(f"consumer error: {e}")

    def stop(self):
        """Cleanup on shutdown."""
        pass


def main():
    from_queues = utils.get_input_queue()
    to_queues = utils.get_output_queue()

    replicas = int(os.getenv("REPLICAS", "1"))
    replica_id = int(os.getenv("REPLICA_ID", "0"))

    module_name = os.getenv("MODULE_NAME")
    if not module_name:
        raise ValueError("MODULE_NAME environment variable is required")

    transform_module = importlib.import_module(module_name)
    if not hasattr(transform_module, "parse_csv_row"):
        raise ValueError(f"{module_name} must have parse_csv_row(csv_row: str) -> dict")
    if not hasattr(transform_module, "create_entity"):
        raise ValueError(f"{module_name} must have create_entity(row_dict: dict) -> Message")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)

    shutdown_signal = ShutdownSignal()

    transformer = Transformer(from_queues, to_queues, transform_module, replicas, replica_id, shutdown_signal)
    transformer.start()


if __name__ == "__main__":
    main()

import importlib
import logging
import os
from types import ModuleType
from typing import Any, Callable, Optional, Type

from shared.entity import EOF, Message, Transaction, TransactionItem
from shared.middleware.interface import MessageMiddleware
from worker import utils
from worker.packer import is_batch, unpack_batch
from worker.types import UserPurchasesByStore


logging.basicConfig(
    level=logging.INFO,
    format="AGGREGATOR - %(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Aggregator:

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queue: list[MessageMiddleware],
        aggregator_fn: Callable[[Any, bytes], Any],
        entity_class: Optional[Type[Message]],
        replicas: int,
    ) -> None:
        self._from_queue: list[MessageMiddleware] = from_queue
        self._to_queue: list[MessageMiddleware] = to_queue
        self._aggregator_fn: Callable[[Any, bytes], Any] = aggregator_fn
        self._entity_class = entity_class
        self._aggregated: Any = None
        self._replicas: int = replicas
        self._message_count = 0

    def _on_message(self, channel, method, properties, body) -> None:
        try:
            if self._handle_eof(body):
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Check if we should unpack batches
            if self._entity_class and is_batch(body):
                # Unpack batch and aggregate each entity
                for entity in unpack_batch(body, self._entity_class):
                    serialized = entity.serialize()
                    self._aggregated = self._aggregator_fn(self._aggregated, serialized)
                    self._message_count += 1
            else:
                # Individual message or no entity_class (pass through)
                self._aggregated = self._aggregator_fn(self._aggregated, body)
                self._message_count += 1

            if self._message_count % 100000 == 0:
                logging.info(f"Aggregated {self._message_count} messages")

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start(self) -> None:
        logging.info("Starting aggregator worker...")
        for queue in self._from_queue:
            queue.start_consuming(self._on_message)

    def stop(self) -> None:
        pass

    def _handle_eof(self, body: bytes) -> bool:
        try:
            eof_message: EOF = EOF.deserialize(body)
        except Exception as e:
            _ = e
            return False

        logging.info("EOF received, stopping worker...")
        for queue in self._from_queue:
            queue.stop_consuming()
        if eof_message.metadata + 1 == self._replicas:
            if os.getenv("TRUNCATE"):
                result = truncate_top_3(self._aggregated).serialize()
            else:
                result = self._aggregated.serialize()
            for queue in self._to_queue:
                queue.send(result)
                queue.send(EOF(0).serialize())
                logging.info("EOF sent to next stage")
        else:
            eof_message.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_message.serialize())
        self.stop()

        return True


def truncate_top_3(aggregated: UserPurchasesByStore) -> UserPurchasesByStore:
    for store_id, dict_of_user_purchases_info in aggregated.user_purchases_by_store.items():
        if len(dict_of_user_purchases_info) > 3:
            users_purchases_info = list(dict_of_user_purchases_info.values())
            users_purchases_info.sort(key=lambda x: x.purchases, reverse=True)
            users_purchases_info = users_purchases_info[:3]
            aggregated.user_purchases_by_store[store_id] = {
                user_purchases_info.user: user_purchases_info for user_purchases_info in users_purchases_info
            }

    return aggregated


def main():
    logging.getLogger("pika").setLevel(logging.WARNING)
    aggregator_module_name: str = os.getenv("MODULE_NAME")
    stage_replicas: int = int(os.getenv("REPLICAS"))
    entity_type: str = os.getenv("ENTITY_TYPE", "")

    aggregator_module: ModuleType = importlib.import_module(aggregator_module_name)
    from_queues: list[MessageMiddleware] = utils.get_input_queue()
    to_queues: list[MessageMiddleware] = utils.get_output_queue()

    # Map entity types to classes
    entity_map = {
        "Transaction": Transaction,
        "TransactionItem": TransactionItem,
    }

    # Only set entity_class if ENTITY_TYPE is provided and valid
    entity_class = entity_map.get(entity_type) if entity_type else None

    if entity_type and not entity_class:
        logging.warning(f"Unknown ENTITY_TYPE: {entity_type}, will pass messages through without unpacking")

    aggregator_worker = Aggregator(
        from_queues, to_queues, aggregator_module.aggregator_fn, entity_class, stage_replicas
    )
    aggregator_worker.start()


if __name__ == "__main__":
    main()

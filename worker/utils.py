import logging
import os

from shared.entity import EOF
from shared.middleware.interface import MessageMiddleware
from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ, MessageMiddlewareQueueMQ


MIDDLEWARE_HOST = "MIDDLEWARE_HOST"

FROM_TYPE = "FROM_TYPE"
FROM = "FROM"
FROM_STRATEGY = "FROM_STRATEGY"

TO_TYPE = "TO_TYPE"
TO = "TO"
TO_STRATEGY = "TO_STRATEGY"

ENRICHER_TYPE = "ENRICHER_TYPE"
ENRICHER = "ENRICHER"
ENRICHER_STRATEGY = "ENRICHER_STRATEGY"

QUEUE_TYPE = "QUEUE"
EXCHANGE_TYPE = "EXCHANGE"

FANOUT_STRATEGY = "FANOUT"
SHARDING_STRATEGY = "SHARDING"


def get_input_queue() -> list[MessageMiddleware] | ValueError:
    return get_source(env_var_source_type=FROM_TYPE, env_var_source_name=FROM, env_var_source_strategy=FROM_STRATEGY)


def get_output_queue() -> list[MessageMiddleware] | ValueError:
    return get_source(env_var_source_type=TO_TYPE, env_var_source_name=TO, env_var_source_strategy=TO_STRATEGY)


def get_enricher_queue() -> list[MessageMiddleware] | ValueError:
    return get_source(
        env_var_source_type=ENRICHER_TYPE, env_var_source_name=ENRICHER, env_var_source_strategy=ENRICHER_STRATEGY
    )


def get_source(
    env_var_source_type: str, env_var_source_name: str, env_var_source_strategy: str
) -> list[MessageMiddleware] | ValueError:
    host: str = os.getenv(MIDDLEWARE_HOST)
    source_type: str = os.getenv(env_var_source_type)
    source_names: list[str] = os.getenv(env_var_source_name).split(",")
    source_strategy: str = os.getenv(env_var_source_strategy)

    if source_type == QUEUE_TYPE:
        return [MessageMiddlewareQueueMQ(host, name) for name in source_names]
    elif source_type == EXCHANGE_TYPE:
        if source_strategy == FANOUT_STRATEGY:
            route_key: str = "common"
        elif source_strategy == SHARDING_STRATEGY:
            raise NotImplementedError
        else:
            raise ValueError(f"STRATEGY must be {FANOUT_STRATEGY} or {SHARDING_STRATEGY}")
        return [
            MessageMiddlewareExchangeRMQ(host=host, exchange_name=source_name, route_keys=[route_key])
            for source_name in source_names
        ]

    raise ValueError(f"TYPE must be {QUEUE_TYPE} or {EXCHANGE_TYPE}")


class EOFHandler:
    """
    Handles EOF propagation for workers consuming from queues or exchanges.

    - Queue mode: EOF passes through N replicas before forwarding downstream
    - Exchange mode: Each worker independently forwards EOF (no coordination)
    """

    def __init__(
        self,
        from_queue: list[MessageMiddleware],
        to_queues: list[MessageMiddleware],
        replicas: int,
    ):
        self._from_queue = from_queue
        self._to_queues = to_queues
        self._replicas = replicas

    def handle_eof(self, body: bytes, on_eof_callback=None) -> bool:
        """
        Handle EOF message.

        Args:
            body: Message body to check for EOF
            on_eof_callback: Optional callback to run before forwarding EOF
                           (e.g., send aggregated results)

        Returns:
            True if EOF was handled, False if body is not EOF
        """
        try:
            eof_message = EOF.deserialize(body)
        except Exception:
            return False

        logging.info("EOF received, stopping worker...")
        for queue in self._from_queue:
            queue.stop_consuming()

        if on_eof_callback:
            on_eof_callback()

        if eof_message.metadata + 1 == self._replicas:
            for queue in self._to_queues:
                queue.send(EOF(0).serialize())
                logging.info(f"EOF sent to next stage in queue {queue} (final replica)")
        else:
            eof_message.metadata += 1
            for queue in self._from_queue:
                queue.send(eof_message.serialize())
            logging.info(f"EOF forwarded to replica {eof_message.metadata}/{self._replicas}")

        return True


def get_eof_handler(from_queue: list[MessageMiddleware], to_queues: list[MessageMiddleware]) -> EOFHandler:
    """
    Create an EOFHandler configured from environment variables.

    Reads REPLICAS and FROM_TYPE to determine EOF propagation mode.
    """
    replicas = int(os.getenv("REPLICAS", "1"))
    logging.info(
        f"Creating EOFHandler with {len(from_queue)} input queues, {len(to_queues)} output queues,"
        f" replicas={replicas}"
    )
    return EOFHandler(from_queue, to_queues, replicas)

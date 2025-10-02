import os

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

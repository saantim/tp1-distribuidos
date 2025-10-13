import json
from typing import List

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


def build_middlewares_list(middlewares: str) -> List[MessageMiddleware]:
    middlewares_list = json.loads(middlewares)
    result = []
    host = "rabbitmq"

    for middleware in middlewares_list:
        m_type = middleware[0]
        if m_type == "QUEUE":
            result.append(MessageMiddlewareQueueMQ(host=host, queue_name=middleware[1]))
        else:
            result.append(
                MessageMiddlewareExchangeRMQ(host=host, exchange_name=middleware[1], route_keys=middleware[3])
            )
    return result


def build_enricher_middlewares(sources: str, enricher_module) -> tuple[MessageMiddleware, MessageMiddleware]:
    """
    Construye source y waiting_queue para enrichers.

    Enrichers necesitan dos colas:
    - Main queue: donde llegan mensajes a enriquecer
    - Waiting queue: donde se ponen mensajes si enricher data no está listo

    La waiting queue tiene TTL y dead-letter a main queue para reintentar
    automáticamente después del TTL.

    Args:
        sources: JSON string del env var FROM
        enricher_module: Módulo del enricher (para leer DEFAULT_WAITING_TTL_MS)

    Returns:
        (source_queue, waiting_queue) tuple de MessageMiddleware
    """
    source_list = build_middlewares_list(sources)
    source_queue = source_list[0]

    if not hasattr(source_queue, "_queue_name"):
        raise ValueError("Enricher source must be a queue, not an exchange")

    main_queue_name = source_queue._queue_name
    waiting_queue_name = f"{main_queue_name}.waiting"

    ttl_ms = getattr(enricher_module.Enricher, "DEFAULT_WAITING_TTL_MS", 5000)

    source = MessageMiddlewareQueueMQ(host="rabbitmq", queue_name=main_queue_name)
    waiting_queue = MessageMiddlewareQueueMQ(
        host="rabbitmq",
        queue_name=waiting_queue_name,
        arguments={
            "x-message-ttl": ttl_ms,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": main_queue_name,
        },
    )

    return source, waiting_queue

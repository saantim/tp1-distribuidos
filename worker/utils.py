"""
Worker utility functions for building middleware connections.
"""

import json
from typing import List

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ, MessageMiddlewareQueueMQ


RABBITMQ_HOST = "rabbitmq"


def build_input_exchange(exchange_name: str, stage_name: str, replica_id: int) -> MessageMiddlewareExchangeRMQ:
    """
    Build input exchange that subscribes to:
    - "common" (for broadcasts)
    - "{stage_name}_{replica_id}" (for targeted routing)

    Args:
        exchange_name: Name of the exchange to consume from
        stage_name: Full stage name (e.g., "q1_filter_hour")
        replica_id: This worker's replica index

    Returns:
        MessageMiddlewareExchangeRMQ configured with routing keys
    """
    routing_keys = ["common", f"{stage_name}_{replica_id}"]
    return MessageMiddlewareExchangeRMQ(host=RABBITMQ_HOST, exchange_name=exchange_name, route_keys=routing_keys)


def build_queue(queue_name: str) -> MessageMiddlewareQueueMQ:
    """
    Build a simple queue middleware (for transformers input and sinks output).

    Args:
        queue_name: Name of the queue

    Returns:
        MessageMiddlewareQueueMQ
    """
    return MessageMiddlewareQueueMQ(host=RABBITMQ_HOST, queue_name=queue_name)


def build_output_exchanges(outputs_json: str) -> List[MessageMiddlewareExchangeRMQ]:
    """
    Build output exchanges from JSON config.

    Args:
        outputs_json: JSON string with list of output configs

    Returns:
        List of MessageMiddlewareExchangeRMQ (one per output)
    """
    outputs_config = json.loads(outputs_json)
    exchanges = []

    for output in outputs_config:
        exchange_name = output["name"]
        exchanges.append(
            MessageMiddlewareExchangeRMQ(
                host=RABBITMQ_HOST, exchange_name=exchange_name, route_keys=[]  # Routing keys provided at send time
            )
        )

    return exchanges


def build_enricher_input(enricher_exchange: str) -> MessageMiddlewareExchangeRMQ:
    """
    Build enricher input exchange (subscribes to broadcast data).
    Enrichers always subscribe to "common" for broadcast enrichment data.

    Args:
        enricher_exchange: Name of the enrichment data exchange

    Returns:
        MessageMiddlewareExchangeRMQ configured for broadcast
    """
    return MessageMiddlewareExchangeRMQ(host=RABBITMQ_HOST, exchange_name=enricher_exchange, route_keys=["common"])


def parse_outputs_config(outputs_json: str) -> List[dict]:
    """
    Parse outputs JSON config.

    Args:
        outputs_json: JSON string with list of output configs

    Returns:
        List of output config dicts
    """
    return json.loads(outputs_json)

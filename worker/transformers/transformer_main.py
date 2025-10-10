import importlib
import logging
import os

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from worker.utils import build_middlewares_list


def main():
    """Main entry point for transformer workers."""
    logging.getLogger("pika").setLevel(logging.WARNING)

    instances: int = int(os.getenv("REPLICAS"))
    transformer_id: int = int(os.getenv("REPLICA_ID"))
    module: str = os.getenv("MODULE_NAME")
    sources: str = os.getenv("FROM")
    destinations: str = os.getenv("TO")

    transformer_module = importlib.import_module(module)

    if not hasattr(transformer_module, "Transformer"):
        raise AttributeError(f"Module {module} must have a 'Transformer' class")

    transformer = transformer_module.Transformer(
        instances=instances,
        index=transformer_id,
        stage_name=module,
        source=build_middlewares_list(sources)[0],
        output=build_middlewares_list(destinations),
        intra_exchange=MessageMiddlewareExchangeRMQ(host="rabbitmq", exchange_name=module, route_keys=["common"]),
    )

    transformer.start()


if __name__ == "__main__":
    main()

import importlib
import os
from types import ModuleType

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from worker.utils import build_middlewares_list


def main():
    instances: int = int(os.getenv("REPLICAS"))
    joiner_id: int = int(os.getenv("REPLICA_ID"))
    module_name: str = os.getenv("MODULE_NAME")
    sources: str = os.getenv("FROM")
    destinations: str = os.getenv("TO")
    reference: str = os.getenv("REFERENCE")

    joiner_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(joiner_module, "Joiner"):
        raise AttributeError(f"Module {module_name} must have a 'Joiner' class")

    source = build_middlewares_list(sources)[0]
    reference_source = build_middlewares_list(reference)[0]

    joiner_worker = joiner_module.Joiner(
        instances=instances,
        index=joiner_id,
        stage_name=module_name,
        source=source,
        output=build_middlewares_list(destinations),
        intra_exchange=MessageMiddlewareExchangeRMQ(host="rabbitmq", exchange_name=module_name, route_keys=["common"]),
        reference_source=reference_source,
    )

    joiner_worker.start()


if __name__ == "__main__":
    main()

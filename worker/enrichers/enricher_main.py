import importlib
import os
from types import ModuleType

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from worker.utils import build_enricher_middlewares, build_middlewares_list


def main():
    instances: int = int(os.getenv("REPLICAS"))
    filter_id: int = int(os.getenv("REPLICA_ID"))
    module_name: str = os.getenv("MODULE_NAME")
    sources: str = os.getenv("FROM")
    destinations: str = os.getenv("TO")
    enricher: str = os.getenv("ENRICHER")

    enricher_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(enricher_module, "Enricher"):
        raise AttributeError(f"Module {module_name} must have a 'Enricher' class")

    source, waiting_queue = build_enricher_middlewares(sources, enricher_module)

    enricher_worker = enricher_module.Enricher(
        instances=instances,
        index=filter_id,
        stage_name=module_name,
        source=source,
        output=build_middlewares_list(destinations),
        intra_exchange=MessageMiddlewareExchangeRMQ(host="rabbitmq", exchange_name=module_name, route_keys=["common"]),
        enricher_input=build_middlewares_list(enricher)[0],
        waiting_queue=waiting_queue,
    )

    enricher_worker.start()


if __name__ == "__main__":
    main()

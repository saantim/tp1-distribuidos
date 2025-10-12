# worker/router/router_main.py
import importlib
import json
import os
from types import ModuleType

from shared.middleware.rabbit_mq import MessageMiddlewareExchangeRMQ
from worker.utils import build_middlewares_list


def extract_routing_keys(destinations: str) -> list[str]:
    """Extract routing keys from TO configuration for routing logic"""
    destinations_list = json.loads(destinations)
    for dest in destinations_list:
        if dest[0] == "EXCHANGE" and len(dest) >= 4:
            return dest[3]
    return []


def main():
    instances: int = int(os.getenv("REPLICAS"))
    router_id: int = int(os.getenv("REPLICA_ID"))
    module_name: str = os.getenv("MODULE_NAME")
    sources: str = os.getenv("FROM")
    destinations: str = os.getenv("TO")
    router_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(router_module, "Router"):
        raise AttributeError(f"Module {module_name} must have a 'Router' class")

    routing_keys = extract_routing_keys(destinations)

    worker_router = router_module.Router(
        instances=instances,
        index=router_id,
        stage_name=module_name,
        source=build_middlewares_list(sources)[0],
        output=build_middlewares_list(destinations),
        intra_exchange=MessageMiddlewareExchangeRMQ(host="rabbitmq", exchange_name=module_name, route_keys=["common"]),
        routing_keys=routing_keys,
    )
    worker_router.start()


if __name__ == "__main__":
    main()

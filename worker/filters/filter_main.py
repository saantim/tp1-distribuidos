# worker/filters/filter_main.py
import importlib
import os
from types import ModuleType

from shared.middleware.interface import MessageMiddlewareExchange
from worker.utils import build_middlewares_list


def main():
    instances: int = int(os.getenv("REPLICAS"))
    filter_id: int = int(os.getenv("REPLICA_ID"))
    module_name: str = os.getenv("MODULE_NAME")
    sources: str = os.getenv("FROM")
    destinations: str = os.getenv("TO")
    filter_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(filter_module, "Filter"):
        raise AttributeError(f"Module {module_name} must have a 'Filter' class")

    worker_filter = filter_module.Filter(
        instances=instances,
        index=filter_id,
        stage_name=module_name,
        source=build_middlewares_list(sources)[0],
        output=build_middlewares_list(destinations),
        intra_exchange=MessageMiddlewareExchange(host="rabbitmq", exchange_name=module_name, route_keys=["common"]),
    )

    worker_filter.start()


if __name__ == "__main__":
    main()

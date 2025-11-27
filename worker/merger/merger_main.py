import importlib
import os
from types import ModuleType

from worker.output import WorkerOutput
from worker.utils import build_input_exchange, build_output_exchanges, parse_outputs_config


def main():
    instances: int = int(os.getenv("REPLICAS"))
    merger_id: int = int(os.getenv("REPLICA_ID"))
    stage_name: str = os.getenv("STAGE_NAME")
    module_name: str = os.getenv("MODULE_NAME")
    input_exchange: str = os.getenv("FROM")
    outputs_json: str = os.getenv("TO")

    merger_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(merger_module, "Merger"):
        raise AttributeError(f"Module {module_name} must have a 'Merger' class")

    # Build input
    source = build_input_exchange(input_exchange, stage_name, merger_id)

    # Build outputs
    exchanges = build_output_exchanges(outputs_json)
    outputs_config = parse_outputs_config(outputs_json)
    outputs = [WorkerOutput.from_config(cfg, exch) for cfg, exch in zip(outputs_config, exchanges)]

    worker_merger = merger_module.Merger(
        instances=instances,
        index=merger_id,
        stage_name=stage_name,
        source=source,
        outputs=outputs,
    )

    worker_merger.start()


if __name__ == "__main__":
    main()

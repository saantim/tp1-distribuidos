import importlib
import os
from types import ModuleType

from worker.output import WorkerOutput
from worker.utils import build_enricher_input, build_input_exchange, build_output_exchanges, parse_outputs_config


def main():
    instances: int = int(os.getenv("REPLICAS"))
    replica_id: int = int(os.getenv("REPLICA_ID"))
    stage_name: str = os.getenv("STAGE_NAME")
    module_name: str = os.getenv("MODULE_NAME")
    input_exchange: str = os.getenv("FROM")
    outputs_json: str = os.getenv("TO")
    enricher_exchange: str = os.getenv("ENRICHER")

    enricher_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(enricher_module, "Enricher"):
        raise AttributeError(f"Module {module_name} must have a 'Enricher' class")

    # Build input (main data exchange)
    source = build_input_exchange(input_exchange, stage_name, replica_id)

    # Build outputs
    exchanges = build_output_exchanges(outputs_json)
    outputs_config = parse_outputs_config(outputs_json)
    outputs = [WorkerOutput.from_config(cfg, exch) for cfg, exch in zip(outputs_config, exchanges)]

    # Build enricher input (reference data exchange)
    enricher_input = build_enricher_input(enricher_exchange)

    enricher_worker = enricher_module.Enricher(
        instances=instances,
        index=replica_id,
        stage_name=stage_name,
        source=source,
        outputs=outputs,
        enricher_input=enricher_input,
    )

    enricher_worker.start()


if __name__ == "__main__":
    main()

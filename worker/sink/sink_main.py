import importlib
import os
from types import ModuleType

from worker.output import WorkerOutput
from worker.utils import build_input_exchange, build_output_exchanges, parse_outputs_config


def main():
    instances: int = int(os.getenv("REPLICAS"))
    sink_id: int = int(os.getenv("REPLICA_ID"))
    stage_name: str = os.getenv("STAGE_NAME")
    module_name: str = os.getenv("MODULE_NAME")
    input_exchange: str = os.getenv("FROM")
    outputs_json: str = os.getenv("TO")

    sink_module: ModuleType = importlib.import_module(module_name)

    if not hasattr(sink_module, "Sink"):
        raise AttributeError(f"Module {module_name} must have a 'Sink' class")

    # Build input
    source = build_input_exchange(input_exchange, stage_name, sink_id)

    # Build outputs (sink now use exchanges with by_stage_name routing)
    exchanges = build_output_exchanges(outputs_json)
    outputs_config = parse_outputs_config(outputs_json)
    outputs = [WorkerOutput.from_config(cfg, exch) for cfg, exch in zip(outputs_config, exchanges)]

    sink = sink_module.Sink(
        instances=instances,
        index=sink_id,
        stage_name=stage_name,
        source=source,
        outputs=outputs,
    )

    sink.start()


if __name__ == "__main__":
    main()

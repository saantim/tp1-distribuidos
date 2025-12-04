import importlib
import logging
import os

from worker.output import WorkerOutput
from worker.utils import build_input_exchange, build_output_exchanges, parse_outputs_config


def main():
    """Main entry point for transformer workers."""
    logging.getLogger("pika").setLevel(logging.WARNING)

    instances: int = int(os.getenv("REPLICAS"))
    transformer_id: int = int(os.getenv("REPLICA_ID"))
    stage_name: str = os.getenv("STAGE_NAME")
    module_name: str = os.getenv("MODULE_NAME")
    input_exchange: str = os.getenv("FROM")
    outputs_json: str = os.getenv("TO")

    transformer_module = importlib.import_module(module_name)

    if not hasattr(transformer_module, "Transformer"):
        raise AttributeError(f"Module {module_name} must have a 'Transformer' class")

    source = build_input_exchange(input_exchange, stage_name, transformer_id)

    # Build outputs
    exchanges = build_output_exchanges(outputs_json)
    outputs_config = parse_outputs_config(outputs_json)
    outputs = [WorkerOutput.from_config(cfg, exch) for cfg, exch in zip(outputs_config, exchanges)]

    transformer = transformer_module.Transformer(
        instances=instances,
        index=transformer_id,
        stage_name=stage_name,
        source=source,
        outputs=outputs,
    )

    transformer.start()


if __name__ == "__main__":
    main()

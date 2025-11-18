#!/usr/bin/env python3
import json
import os
import sys

import yaml


def load_config(config_file="compose_config.yaml"):
    """Load YAML configuration"""
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def create_worker_service(
    name, worker_type, replica_id, total_replicas, stage_name, module, input_exchange, outputs, enricher=None
):
    """Create a worker service definition"""

    # Entrypoint mapping
    entrypoints = {
        "filter": "python /worker/filters/filter_main.py",
        "aggregator": "python /worker/aggregators/aggregator_main.py",
        "merger": "python /worker/mergers/merger_main.py",
        "enricher": "python /worker/enrichers/enricher_main.py",
        "router": "python /worker/router/router_main.py",
        "sink": "python /worker/sinks/sink_main.py",
        "transformer": "python /worker/transformers/transformer_main.py",
    }

    service = {
        "container_name": name,
        "image": f"{worker_type}_worker",
        "build": {"context": ".", "dockerfile": "./worker/Dockerfile"},
        "entrypoint": entrypoints[worker_type],
        "networks": ["coffee"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": {
            "REPLICA_ID": str(replica_id),
            "REPLICAS": str(total_replicas),
            "STAGE_NAME": stage_name,
            "MODULE_NAME": module,
            "FROM": input_exchange,
            "TO": json.dumps(outputs),
        },
    }

    if enricher:
        service["environment"]["ENRICHER"] = enricher

    return service


def add_transformer_workers(services, name, transformer, stage_map):
    """Add transformer workers"""
    stage_name = f"transformer_{name}"

    # Validate outputs
    validate_outputs(stage_name, transformer.get("output", []), stage_map)

    for i in range(transformer["replicas"]):
        worker_name = f"{stage_name}_{i}" if transformer["replicas"] > 1 else stage_name

        services[worker_name] = create_worker_service(
            name=worker_name,
            worker_type="transformer",
            replica_id=i,
            total_replicas=transformer["replicas"],
            stage_name=stage_name,
            module=transformer["module"],
            input_exchange=transformer["input"],
            outputs=transformer["output"],
        )


def add_stage_workers(services, query_name, stage_name, stage, query_config, stage_map):
    """Add workers for a query stage"""
    full_stage_name = f"{query_name}_{stage_name}"

    # Validate outputs
    validate_outputs(full_stage_name, stage.get("output", []), stage_map)

    for i in range(stage["replicas"]):
        worker_name = f"{full_stage_name}_{i}" if stage["replicas"] > 1 else full_stage_name

        services[worker_name] = create_worker_service(
            name=worker_name,
            worker_type=stage["type"],
            replica_id=i,
            total_replicas=stage["replicas"],
            stage_name=full_stage_name,
            module=stage["module"],
            input_exchange=stage.get("input"),
            outputs=stage.get("output", []),
            enricher=stage.get("enricher"),
        )


def build_stage_replica_map(config):
    """Build a map of stage_name -> replica_count for validation"""
    stage_map = {}

    # Add transformers
    for name, transformer in config.get("transformers", {}).items():
        stage_name = f"transformer_{name}"
        stage_map[stage_name] = transformer["replicas"]

    # Add query stages
    for query_name, query in config.get("queries", {}).items():
        if not query.get("enabled", True):
            continue
        for stage_name, stage in query.get("stages", {}).items():
            full_stage_name = f"{query_name}_{stage_name}"
            stage_map[full_stage_name] = stage["replicas"]

    return stage_map


def validate_outputs(stage_name, outputs, stage_map):
    """Validate that downstream_workers matches actual downstream stage replicas"""
    if not outputs:
        return

    for output in outputs:
        # Skip broadcast outputs (they don't need downstream_stage/downstream_workers)
        if output.get("routing_fn") == "broadcast":
            continue

        downstream_stage = output.get("downstream_stage")
        downstream_workers = output.get("downstream_workers")

        if not downstream_stage or downstream_workers is None:
            raise ValueError(
                f"Stage '{stage_name}': Output '{output['name']}' missing 'downstream_stage' or 'downstream_workers'"
            )

        # Check if downstream stage exists (allow query names like "q1", "q2", etc. for sinks)
        is_query_name = downstream_stage in ["q1", "q2", "q3", "q4"]
        if downstream_stage not in stage_map and downstream_stage != "gateway" and not is_query_name:
            raise ValueError(
                f"Stage '{stage_name}': Unknown downstream_stage '{downstream_stage}' in output '{output['name']}'"
            )

        # Validate worker count (skip gateway and query names)
        if downstream_stage != "gateway" and not is_query_name:
            actual_replicas = stage_map[downstream_stage]
            if downstream_workers != actual_replicas:
                raise ValueError(
                    f"Stage '{stage_name}': Output '{output['name']}' has downstream_workers={downstream_workers}, "
                    f"but stage '{downstream_stage}' has {actual_replicas} replicas"
                )


def generate_compose(config):
    """Generate docker-compose services from config"""
    services = {}

    # Build stage replica map for validation
    stage_map = build_stage_replica_map(config)

    # Add RabbitMQ
    rmq = config["settings"]["rabbitmq"]
    services["rabbitmq"] = {
        "container_name": "rabbitmq",
        "image": f"rabbitmq:{rmq['version']}",
        "volumes": ["rabbitmq-volume:/var/lib/rabbitmq"],
        "environment": {
            "RABBITMQ_DEFAULT_USER": rmq["user"],
            "RABBITMQ_DEFAULT_PASS": rmq["password"],
            "RABBITMQ_NODENAME": "rabbit@rabbitmq",
        },
        "networks": ["coffee"],
        "ports": ["5672:5672", "8080:15672"],
        "healthcheck": {
            "test": "rabbitmq-diagnostics -q check_running && rabbitmq-diagnostics -q check_local_alarms &&"
            " rabbitmq-diagnostics -q check_port_connectivity",
            "interval": "5s",
            "timeout": "10s",
            "retries": 10,
            "start_period": "40s",
        },
    }

    # Add Gateway
    services["gateway"] = {
        "container_name": "gateway",
        "build": {"context": ".", "dockerfile": "./gateway/Dockerfile"},
        "entrypoint": "python main.py",
        "networks": ["coffee"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
    }

    # Add Client
    clients = config["settings"]["clients"]

    amount_min = int(clients["min"])
    amount_full = int(clients["full"])

    if amount_min > 0:
        services["client_min"] = {
            "build": {"context": ".", "dockerfile": "./client/Dockerfile"},
            "entrypoint": "python main.py",
            "networks": ["coffee"],
            "depends_on": ["gateway"],
            "environment": {"LOGGING_LEVEL": "DEBUG"},
            "volumes": ["./.data/dataset_min:/client/.data", "./.results:/client/.results"],
            "scale": amount_min,
        }

    if amount_full > 0:
        services["client_full"] = {
            "build": {"context": ".", "dockerfile": "./client/Dockerfile"},
            "entrypoint": "python main.py",
            "networks": ["coffee"],
            "depends_on": ["gateway"],
            "environment": {"LOGGING_LEVEL": "DEBUG"},
            "volumes": ["./.data/dataset_full:/client/.data", "./.results:/client/.results"],
            "scale": int(amount_full),
        }

    # Add transformers
    for name, transformer in config.get("transformers", {}).items():
        add_transformer_workers(services, name, transformer, stage_map)

    # Add query stages
    for query_name, query in config.get("queries", {}).items():
        if not query.get("enabled", True):
            continue

        for stage_name, stage in query.get("stages", {}).items():
            add_stage_workers(services, query_name, stage_name, stage, query, stage_map)

    # Build complete compose
    compose = {
        "name": "coffee-shop-analyzer",
        "services": services,
        "volumes": {"rabbitmq-volume": None},
        "networks": {"coffee": {"driver": "bridge"}},
    }

    return compose


def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else "compose_config.yaml"

    print(f"Loading {config_file}...")
    config = load_config(config_file)

    print("Generating docker-compose.yml...")
    compose = generate_compose(config)

    with open("docker-compose.yml", "w") as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False, width=1000)

    os.chmod(__file__, 0o755)

    print("✓ Generated docker-compose.yml successfully!")
    print(f"  Services: {len(compose['services'])}")
    print(f"  Queries: {len([q for q in config.get('queries', {}).values() if q.get('enabled', True)])}")


if __name__ == "__main__":
    main()

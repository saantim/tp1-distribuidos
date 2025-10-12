#!/usr/bin/env python3
import json
import os
import sys

import yaml


def load_config(config_file="compose_config.yaml"):
    """Load YAML configuration"""
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def create_worker_service(name, worker_type, replica_id, total_replicas, module, inputs, outputs, enricher=None):
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
            "MODULE_NAME": module,
            "FROM": json.dumps(inputs),
            "TO": json.dumps(outputs),
        },
    }

    if enricher:
        service["environment"]["ENRICHER"] = json.dumps([enricher])

    return service


def parse_connection(conn_config, replica_id=0, total_replicas=1):
    """Parse a connection config into (type, name, strategy, routing_keys) tuple"""
    if not conn_config:
        return None

    if "queue" in conn_config:
        return "QUEUE", conn_config["queue"]
    elif "queues" in conn_config:
        return [("QUEUE", q) for q in conn_config["queues"]]
    elif "exchange" in conn_config:
        strategy = conn_config.get("strategy")
        routing_keys = conn_config.get("routing_keys", [])
        routing_keys_indexed = conn_config.get("routing_keys_indexed", False)

        if routing_keys_indexed and strategy:
            exchange_name = conn_config["exchange"]
            base_name = exchange_name.rsplit("_", 1)[0] if "_" in exchange_name else exchange_name
            routing_keys = [f"{base_name}_{replica_id}"]

        if strategy and routing_keys:
            return "EXCHANGE", conn_config["exchange"], strategy, routing_keys
        else:
            return "EXCHANGE", conn_config["exchange"]

    return None


def add_transformer_workers(services, name, transformer):
    """Add transformer workers"""
    for i in range(transformer["replicas"]):
        worker_name = f"transformer_{name}_{i}" if transformer["replicas"] > 1 else f"transformer_{name}"

        inputs = [parse_connection(transformer["input"])]
        outputs = [parse_connection(transformer["output"])]

        services[worker_name] = create_worker_service(
            name=worker_name,
            worker_type="transformer",
            replica_id=i,
            total_replicas=transformer["replicas"],
            module=transformer["module"],
            inputs=inputs,
            outputs=outputs,
        )


def resolve_routing_keys(conn_config, query_config):
    """Resuelve routing_keys_from para generar automáticamente los routing keys"""
    if not conn_config or "routing_keys_from" not in conn_config:
        return conn_config

    # Buscar el stage referenciado
    target_stage = conn_config["routing_keys_from"]
    if target_stage in query_config.get("stages", {}):
        target_replicas = query_config["stages"][target_stage]["replicas"]
        # Generar routing keys basándose en el exchange name
        exchange_name = conn_config["exchange"]
        base_name = exchange_name.rsplit("_", 1)[0] if "_" in exchange_name else exchange_name
        conn_config["routing_keys"] = [f"{base_name}_{i}" for i in range(target_replicas)]
        del conn_config["routing_keys_from"]

    return conn_config


def add_stage_workers(services, query_name, stage_name, stage, query_config):
    """Add workers for a query stage"""
    for i in range(stage["replicas"]):
        full_name = f"{query_name}_{stage_name}"
        worker_name = f"{full_name}_{i}" if stage["replicas"] > 1 else full_name

        # Resolver routing_keys automáticos
        if stage.get("output"):
            stage["output"] = resolve_routing_keys(stage["output"], query_config)
        if stage.get("input"):
            stage["input"] = resolve_routing_keys(stage["input"], query_config)

        # Parse inputs (con replica_id para routing_keys_indexed)
        input_conn = parse_connection(stage.get("input"), i, stage["replicas"])
        inputs = [input_conn] if isinstance(input_conn, tuple) else input_conn

        # Parse outputs
        output_conn = parse_connection(stage.get("output"), i, stage["replicas"])
        outputs = []
        if output_conn:
            outputs = [output_conn] if isinstance(output_conn, tuple) else output_conn

        # Parse enricher
        enricher = None
        if stage.get("enricher"):
            enricher = parse_connection(stage["enricher"], i, stage["replicas"])

        services[worker_name] = create_worker_service(
            name=worker_name,
            worker_type=stage["type"],
            replica_id=i,
            total_replicas=stage["replicas"],
            module=stage["module"],
            inputs=inputs,
            outputs=outputs,
            enricher=enricher,
        )


def generate_compose(config):
    """Generate docker-compose services from config"""
    services = {}

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
    dataset = config["settings"]["dataset"]
    dataset_path = "/dataset_full" if dataset == "full" else "/dataset_min"
    services["client"] = {
        "container_name": "client",
        "build": {"context": ".", "dockerfile": "./client/Dockerfile"},
        "entrypoint": "python main.py",
        "networks": ["coffee"],
        "depends_on": ["gateway"],
        "environment": {"LOGGING_LEVEL": "DEBUG"},
        "volumes": [f"./.data{dataset_path}:/client/.data", "./.results:/client/.results"],
    }

    # Add transformers
    for name, transformer in config.get("transformers", {}).items():
        add_transformer_workers(services, name, transformer)

    # Add query stages
    for query_name, query in config.get("queries", {}).items():
        if not query.get("enabled", True):
            continue

        for stage_name, stage in query.get("stages", {}).items():
            add_stage_workers(services, query_name, stage_name, stage, query)

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

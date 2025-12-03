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
    name,
    worker_type,
    replica_id,
    total_replicas,
    stage_name,
    module,
    input_exchange,
    outputs,
    enricher=None,
    health_checker_config=None,
):
    """Create a worker service definition"""

    entrypoints = {
        "filter": "python /worker/filter/filter_main.py",
        "aggregator": "python /worker/aggregator/aggregator_main.py",
        "merger": "python /worker/merger/merger_main.py",
        "enricher": "python /worker/enricher/enricher_main.py",
        "sink": "python /worker/sink/sink_main.py",
        "transformer": "python /worker/transformer/transformer_main.py",
    }

    depends_on = {"rabbitmq": {"condition": "service_healthy"}}

    if health_checker_config:
        hc_replicas = health_checker_config.get("replicas", 1)
        for i in range(hc_replicas):
            depends_on[f"health_checker_{i}"] = {"condition": "service_started"}
    service = {
        "container_name": name,
        "image": f"{worker_type}_worker",
        "build": {"context": ".", "dockerfile": "./worker/Dockerfile"},
        "entrypoint": entrypoints[worker_type],
        "networks": ["coffee"],
        "depends_on": depends_on,
        "volumes": [f"./.saved_sessions/{stage_name}-{replica_id}:/sessions", "./worker:/worker", "./shared:/shared"],
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

    if health_checker_config:
        hc_replicas = health_checker_config.get("replicas", 1)
        service["environment"]["HEALTH_CHECKER_REPLICAS"] = str(hc_replicas)
        service["environment"]["HEALTH_CHECKER_PORT"] = str(health_checker_config["worker"]["port"])
        service["environment"]["HEARTBEAT_INTERVAL"] = str(health_checker_config["worker"]["heartbeat_interval"])

    return service


def add_transformer_workers(services, name, transformer, stage_map, health_checker_config=None):
    """Add transformer workers"""
    stage_name = f"transformer_{name}"

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
            health_checker_config=health_checker_config,
        )
        services[worker_name]["healthcheck"] = {
            "test": "test -f /tmp/ready",
            "interval": "5s",
            "timeout": "3s",
            "retries": 20,
            "start_period": "10s",
        }


def add_stage_workers(services, query_name, stage_name, stage, query_config, stage_map, health_checker_config=None):
    """Add workers for a query stage"""
    full_stage_name = f"{query_name}_{stage_name}"

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
            health_checker_config=health_checker_config,
        )
        services[worker_name]["healthcheck"] = {
            "test": "test -f /tmp/ready",
            "interval": "5s",
            "timeout": "3s",
            "retries": 20,
            "start_period": "10s",
        }


def build_stage_replica_map(config):
    """Build a map of stage_name -> replica_count for validation"""
    stage_map = {}

    # Add transformer
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

        # Check if downstream stage exists (allow query names like "q1", "q2", etc. for sink)
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

    stage_map = build_stage_replica_map(config)

    chaos_monkey_config = config["settings"].get("chaos_monkey")
    chaos_monkey_enabled = chaos_monkey_config and chaos_monkey_config.get("enabled", False)

    health_checker_config = config["settings"].get("health_checker")
    health_checker_enabled = health_checker_config and health_checker_config.get("enabled", False)

    # Add RabbitMQ
    rmq = config["settings"]["rabbitmq"]
    services["rabbitmq"] = {
        "hostname": "rabbitmq",
        "container_name": "rabbitmq",
        "image": f"rabbitmq:{rmq['version']}",
        "volumes": ["rabbitmq-volume:/var/lib/rabbitmq", "./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf"],
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
        "volumes": ["./gateway:/gateway", "./shared:/shared", "./compose_config.yaml:/gateway/compose_config.yaml"],
        "entrypoint": "python main.py",
        "networks": ["coffee"],
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
    }

    if chaos_monkey_enabled:
        services["chaos_monkey"] = {
            "container_name": "chaos_monkey",
            "build": {"context": ".", "dockerfile": "./chaos_monkey/Dockerfile"},
            "entrypoint": "python main.py",
            "networks": ["coffee"],
            "restart": "on-failure",
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock", "./chaos_monkey:/chaos_monkey"],
            "environment": {
                "CONTAINERS_EXCLUDED": str(chaos_monkey_config["containers_excluded"]),
                "KILL_INTERVAL": float(chaos_monkey_config["kill_interval"]),
                "LOGGING_LEVEL": str(chaos_monkey_config["logging_level"]),
            },
            "depends_on": {"gateway": {"condition": "service_started"}},
        }

    hc_volumes = {}
    if health_checker_enabled:
        hc_replicas = health_checker_config.get("replicas", 1)
        for i in range(hc_replicas):
            hc_name = f"health_checker_{i}"
            volume_name = f"hc_{i}_data"
            hc_volumes[volume_name] = None
            services[hc_name] = {
                "container_name": hc_name,
                "build": {"context": ".", "dockerfile": "./health_checker/Dockerfile"},
                "entrypoint": "python main.py",
                "networks": ["coffee"],
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    f"{volume_name}:/state",
                ],
                "environment": {
                    "REPLICA_ID": str(i),
                    "REPLICAS": str(hc_replicas),
                    "CHECK_INTERVAL": str(health_checker_config["check_interval"]),
                    "WORKER_PORT": str(health_checker_config["worker"]["port"]),
                    "WORKER_TIMEOUT": str(health_checker_config["worker"]["timeout"]),
                    "WORKER_HEARTBEAT_INTERVAL": str(health_checker_config["worker"]["heartbeat_interval"]),
                    "PEER_PORT": str(health_checker_config["peer"]["port"]),
                    "PEER_HEARTBEAT_INTERVAL": str(health_checker_config["peer"]["heartbeat_interval"]),
                    "PEER_TIMEOUT": str(health_checker_config["peer"]["timeout"]),
                    "ELECTION_TIMEOUT": str(health_checker_config["election"]["timeout"]),
                    "COORDINATOR_TIMEOUT": str(health_checker_config["election"]["coordinator_timeout"]),
                    "PERSIST_PATH": "/state/registry.json",
                },
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
            "volumes": ["./.data/dataset_min:/client/.data", "./.results:/client/.results", "./client:/client", "./shared:/shared"],
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

    hc_config = health_checker_config if health_checker_enabled else None

    for name, transformer in config.get("transformers", {}).items():
        add_transformer_workers(services, name, transformer, stage_map, hc_config)

    for query_name, query in config.get("queries", {}).items():
        if not query.get("enabled", True):
            continue

        for stage_name, stage in query.get("stages", {}).items():
            add_stage_workers(services, query_name, stage_name, stage, query, stage_map, hc_config)

    # Build complete compose
    volumes = {"rabbitmq-volume": None}
    volumes.update(hc_volumes)

    compose = {
        "name": "coffee-shop-analyzer",
        "services": services,
        "volumes": volumes,
        "networks": {"coffee": {"driver": "bridge"}},
    }

    services["gateway"]["depends_on"] = {
        name: {"condition": "service_healthy"}
        for name in services
        if name.startswith(("transformer_", "q1_", "q2_", "q3_", "q4_"))
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

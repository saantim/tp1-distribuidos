#!/usr/bin/env python3

import logging
from typing import Dict

import yaml

from shared.protocol import EntityType


ENTITY_TO_TRANSFORMER = {
    EntityType.STORE: "stores",
    EntityType.USER: "users",
    EntityType.TRANSACTION: "transactions",
    EntityType.TRANSACTION_ITEM: "transaction_items",
    EntityType.MENU_ITEM: "menu_items",
}


def parse_enabled_queries(config_file: str = "compose_config.yaml") -> list[str]:
    """
    Parse compose_config.yaml and return list of enabled query names.

    Args:
        config_file: Path to compose_config.yaml

    Returns:
        List of enabled query names (e.g., ["q1", "q2"])
    """
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.warning(f"Config file not found: {config_file}")
        return []
    except yaml.YAMLError as e:
        logging.warning(f"Failed to parse YAML config: {e}")
        return []

    enabled_queries = []
    queries = config.get("queries", {})

    for query_name, query_def in queries.items():
        if query_def is None:
            continue
        if isinstance(query_def, dict) and query_def.get("enabled", True) is False:
            continue
        enabled_queries.append(query_name)

    return enabled_queries


def parse_gateway_config(config_file: str = "compose_config.yaml") -> dict:
    """
    Parse compose_config.yaml for gateway configuration.

    Returns:
        {
            "transformer": {EntityType -> {exchange, downstream_stage, replicas}},
            "enabled_queries": ["q1", ...]
        }
    """
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML config: {e}")

    transformers_config = _parse_transformers(config.get("transformers", {}))
    enabled_queries = parse_enabled_queries(config_file)

    return {"transformers": transformers_config, "enabled_queries": enabled_queries}


def _parse_transformers(transformers_yaml: dict) -> Dict[EntityType, dict]:
    transformers_config = {}
    transformer_to_entity = {v: k for k, v in ENTITY_TO_TRANSFORMER.items()}

    for transformer_name, transformer_def in transformers_yaml.items():
        if transformer_def is None:
            logging.debug(f"Skipping disabled transformer: {transformer_name}")
            continue

        entity_type = transformer_to_entity.get(transformer_name)
        if entity_type is None:
            logging.warning(f"Unknown transformer '{transformer_name}' - no matching EntityType")
            continue

        try:
            input_queue = transformer_def["input"]
            replicas = transformer_def["replicas"]
            module = transformer_def["module"]

            transformers_config[entity_type] = {
                "exchange": input_queue,
                "downstream_stage": f"transformer_{transformer_name}",
                "replicas": replicas,
                "module": module,
            }

            logging.debug(f"Loaded transformer: {entity_type.name} -> {input_queue}, replicas={replicas}")

        except KeyError as e:
            logging.warning(f"Transformer '{transformer_name}' missing field: {e}")
            continue

    return transformers_config

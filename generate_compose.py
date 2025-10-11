#!/usr/bin/env python3
"""
Script para generar dinámicamente el archivo docker-compose.yml
"""
import json
import os
from typing import Any, Dict

import yaml

from docker_compose_builder import DockerComposeBuilder


def save_docker_compose(docker_compose: Dict[str, Any], output_file: str = "docker-compose.yml") -> None:
    with open(output_file, "w") as f:
        yaml.dump(docker_compose, f, default_flow_style=False, sort_keys=False, width=1000)
    print(f"Archivo {output_file} generado exitosamente!")


def load_config(config_file="compose_config.json"):
    with open(config_file, "r") as f:
        return json.load(f)


def build_q1(builder: DockerComposeBuilder, config: Dict[str, Any]) -> DockerComposeBuilder:
    for i in range(config["replicas"]["filter_tx_2024_2025"]):
        builder.add_filter(
            name=f"filter_tx_2024_2025_{i}",
            filter_id=i,
            from_queue="transactions_source",
            to_queues=["filtered_tx_2024_2025_q1", "filtered_tx_2024_2025_q4"],
            module_name="year",
            replicas=config["replicas"]["filter_tx_2024_2025"],
        )
    for i in range(config["replicas"]["filter_tx_6am_11pm"]):
        builder.add_filter(
            filter_id=i,
            name=f"filter_tx_6am_11pm_{i}",
            from_queue="filtered_tx_2024_2025_q1",
            to_queues=["filtered_tx_6am_11pm_q1", "filtered_tx_6am_11pm_q3"],
            module_name="hour",
            replicas=config["replicas"]["filter_tx_6am_11pm"],
        )
    for i in range(config["replicas"]["filter_amount"]):
        builder.add_filter(
            filter_id=i,
            name=f"filter_amount_{i}",
            from_queue="filtered_tx_6am_11pm_q1",
            to_queues=["filtered_amount"],
            module_name="amount",
            replicas=config["replicas"]["filter_amount"],
        )
    for i in range(config["replicas"]["sink_q1"]):
        builder.add_sink(
            sink_id=i,
            name=f"sink_q1_{i}",
            from_queue="filtered_amount",
            to_queue="results_q1",
            module_name="sink_q1",
            replicas=config["replicas"]["sink_q1"],
        )

    return builder


def build_q2(builder: DockerComposeBuilder, config: Dict[str, Any]) -> DockerComposeBuilder:
    for i in range(config["replicas"]["filter_tx_item_2024_2025"]):
        builder.add_filter(
            filter_id=i,
            name=f"filter_tx_item_2024_2025_{i}",
            from_queue="transaction_items_source",
            to_queues=["filtered_item_amount"],
            module_name="item_year",
            replicas=config["replicas"]["filter_tx_item_2024_2025"],
        )
    for i in range(config["replicas"]["aggregator_period"]):
        builder.add_aggregator(
            aggregator_id=i,
            name=f"aggregator_period_{i}",
            from_type="QUEUE",
            from_name="filtered_item_amount",
            to_type="QUEUE",
            to_name="aggregated_item_by_period",
            module_name="period_agg",
            replicas=config["replicas"]["aggregator_period"],
        )
    for i in range(config["replicas"]["merger_period"]):
        builder.add_merger(
            merger_id=i,
            name=f"merger_period_{i}",
            from_queue="aggregated_item_by_period",
            to_queue="merged_item_by_period",
            module_name="period_merge",
            replicas=config["replicas"]["merger_period"],
        )
    for i in range(config["replicas"]["enricher_item"]):
        builder.add_enricher(
            enricher_id=i,
            name=f"enricher_item_{i}",
            from_queue="merged_item_by_period",
            to="enriched_item_by_level",
            to_type="QUEUE",
            enricher="menu_items_source",
            enricher_strategy="FANOUT",
            enricher_routing_key=["common"],
            module_name="item_enricher",
            replicas=config["replicas"]["enricher_item"],
        )
    for i in range(config["replicas"]["sink_q2"]):
        builder.add_sink(
            sink_id=i,
            name=f"sink_q2_{i}",
            from_queue="enriched_item_by_level",
            to_queue="results_q2",
            module_name="sink_q2",
            replicas=config["replicas"]["sink_q2"],
        )
    return builder


def build_q3(builder: DockerComposeBuilder, config: Dict[str, Any]) -> DockerComposeBuilder:
    for i in range(config["replicas"]["semester_aggregator"]):
        builder.add_aggregator(
            aggregator_id=i,
            name=f"semester_aggregator_{i}",
            from_type="QUEUE",
            from_name="filtered_tx_6am_11pm_q3",
            to_type="QUEUE",
            to_name="aggregated_semester_by_store",
            module_name="semester_agg",
            replicas=config["replicas"]["semester_aggregator"],
        )
    for i in range(config["replicas"]["merger_semester_results"]):
        builder.add_merger(
            merger_id=i,
            name=f"merger_semester_results_{i}",
            from_queue="aggregated_semester_by_store",
            to_queue="merged_semester_by_store",
            module_name="semester_merge",
            replicas=config["replicas"]["merger_semester_results"],
        )
    for i in range(config["replicas"]["enricher_semester_tx"]):
        builder.add_enricher(
            enricher_id=i,
            name=f"enricher_semester_tx_{i}",
            from_queue="merged_semester_by_store",
            to="enriched_semester_tx",
            to_type="QUEUE",
            enricher="stores_source",
            enricher_strategy="FANOUT",
            enricher_routing_key=["common"],
            module_name="store_enricher",
            replicas=config["replicas"]["enricher_semester_tx"],
        )
    for i in range(config["replicas"]["sink_q3"]):
        builder.add_sink(
            sink_id=i,
            name=f"sink_q3_{i}",
            from_queue="enriched_semester_tx",
            to_queue="results_q3",
            module_name="sink_q3",
            replicas=config["replicas"]["sink_q3"],
        )

    return builder


def build_q4(builder: DockerComposeBuilder, config: Dict[str, Any]) -> DockerComposeBuilder:
    for i in range(config["replicas"]["router"]):
        builder.add_router(
            router_id=i,
            name=f"router_{i}",
            from_queue="filtered_tx_2024_2025_q4",
            to="tx_filtered_q4",  # TODO!: REVISAR
            to_type="EXCHANGE",
            to_strategy="DIRECT",
            to_routing_key=[f"tx_filtered_q4_{i}" for i in range(config["replicas"]["user_purchase_aggregator"])],
            module_name="router_transactions_q4",
            replicas=config["replicas"]["router"],
        )
    for i in range(config["replicas"]["user_purchase_aggregator"]):
        builder.add_aggregator(
            aggregator_id=i,
            name=f"aggregator_user_purchase_{i}",
            from_name="tx_filtered_q4",
            from_type="EXCHANGE",
            from_strategy="DIRECT",
            from_routing_key=[f"tx_filtered_q4_{i}"],
            to_type="EXCHANGE",
            to_name="user_purchase_aggregated",
            to_strategy="FANOUT",
            to_routing_key=["common"],
            module_name="user_purchase_aggregator",
            replicas=config["replicas"]["user_purchase_aggregator"],
        )
    for i in range(config["replicas"]["user_enricher_tx"]):
        builder.add_enricher(
            enricher_id=i,
            name=f"user_enricher_tx_{i}",
            from_queue="users_source",
            to="enriched_store_user_top3",
            to_type="QUEUE",
            enricher="user_purchase_aggregated",
            enricher_strategy="FANOUT",
            enricher_routing_key=["common"],
            module_name="user_enricher",
            replicas=config["replicas"]["user_enricher_tx"],
        )
    for i in range(config["replicas"]["merger_final_top3"]):
        builder.add_merger(
            merger_id=i,
            name=f"merger_final_top3_{i}",
            from_queue="enriched_store_user_top3",
            to_queue="enriched_final_top3",
            module_name="top_3",
            replicas=config["replicas"]["merger_final_top3"],
        )
    for i in range(config["replicas"]["store_enricher_tx"]):
        builder.add_enricher(
            enricher_id=i,
            name=f"store_enricher_tx_{i}",
            from_queue="enriched_final_top3",
            to="top3_users_store_enriched",
            to_type="QUEUE",
            enricher="stores_source",
            enricher_strategy="FANOUT",
            enricher_routing_key=["common"],
            module_name="store_enricher_q4",
            replicas=config["replicas"]["store_enricher_tx"],
        )
    for i in range(config["replicas"]["sink_q4"]):
        builder.add_sink(
            sink_id=i,
            name=f"sink_q4_{i}",
            from_queue="top3_users_store_enriched",
            to_queue="results_q4",
            module_name="sink_q4",
            replicas=config["replicas"]["sink_q4"],
        )

    return builder


def build_transformers(builder: DockerComposeBuilder, config: Dict[str, Any]) -> DockerComposeBuilder:
    for i in range(config["replicas"]["transformer_menu"]):
        builder.add_transformer(
            transformer_id=i,
            name=f"transformer_menu_{i}",
            from_queue="raw_menu_items_batches",
            to="menu_items_source",
            to_strategy="FANOUT",
            to_routing_keys=["common"],
            replicas=config["replicas"]["transformer_menu"],
            module_name="menu",
        )

    for i in range(config["replicas"]["transformer_store"]):
        builder.add_transformer(
            transformer_id=i,
            name=f"transformer_store_{i}",
            from_queue="raw_stores_batches",
            to="stores_source",
            to_strategy="FANOUT",
            to_routing_keys=["common"],
            replicas=config["replicas"]["transformer_store"],
            module_name="store",
        )

    for i in range(config["replicas"]["transformer_user"]):
        builder.add_transformer(
            transformer_id=i,
            name=f"transformer_user_{i}",
            from_queue="raw_users_batches",
            to="users_source",
            replicas=config["replicas"]["transformer_user"],
            module_name="user",
        )

    for i in range(config["replicas"]["transformer_transaction"]):
        builder.add_transformer(
            transformer_id=i,
            name=f"transformer_transaction_{i}",
            from_queue="raw_transactions_batches",
            to="transactions_source",
            replicas=config["replicas"]["transformer_transaction"],
            module_name="transaction",
        )

    for i in range(config["replicas"]["transformer_transaction_item"]):
        builder.add_transformer(
            transformer_id=i,
            name=f"transformer_tx_items_{i}",
            from_queue="raw_transaction_items_batches",
            to="transaction_items_source",
            replicas=config["replicas"]["transformer_transaction_item"],
            module_name="transaction_item",
        )

    return builder


def main():
    config = load_config()
    builder = DockerComposeBuilder()
    builder.add_gateway()
    builder.add_client(config["dataset"]["full"].lower() == "true")
    builder.add_rabbitmq()
    builder = build_transformers(builder, config)

    builder = build_q1(builder, config)
    builder = build_q2(builder, config)
    builder = build_q3(builder, config)
    # TODO: Refactor como se arma todo esto para hacerlo mas flexible.
    builder = build_q4(builder, config)

    builder.save("docker-compose.yml")
    os.chmod("generate_compose.py", 0o755)


if __name__ == "__main__":
    main()

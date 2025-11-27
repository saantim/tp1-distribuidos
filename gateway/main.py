#!/usr/bin/env python3

import configparser
import logging
import os

from core.server import Server

from shared.config_parser import parse_gateway_config
from shared.shutdown import ShutdownSignal


def initialize_config():
    config = configparser.ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
        config_params["listen_backlog"] = int(os.getenv("LISTEN_BACKLOG", config["DEFAULT"]["LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["middleware_host"] = os.getenv("MIDDLEWARE_HOST", config["MIDDLEWARE"]["MIDDLEWARE_HOST"])

    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting gateway")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting gateway")

    return config_params


def initialize_log(logging_level):
    """python custom logging initialization."""
    logging.basicConfig(
        level=logging.INFO,
        format="GATEWAY" + " - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    try:
        config_params = initialize_config()
    except (KeyError, ValueError) as e:
        print(f"Configuration error: {e}")
        return

    port = config_params["port"]
    listen_backlog = config_params["listen_backlog"]
    logging_level = config_params["logging_level"]
    middleware_host = config_params["middleware_host"]

    initialize_log(logging_level)
    logging.getLogger("pika").setLevel(logging.WARNING)

    try:
        gateway_config = parse_gateway_config()
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"action: parse_config | result: fail | error: {e}")
        return

    batch_exchanges = {}
    transformer_configs = {}

    for entity_type, config in gateway_config["transformer"].items():
        batch_exchanges[entity_type.name] = config["exchange"]
        transformer_configs[entity_type] = {
            "exchange": config["exchange"],
            "downstream_stage": config["downstream_stage"],
            "replicas": config["replicas"],
        }

    enabled_queries = gateway_config["enabled_queries"]

    logging.debug(
        f"action: config | result: success | "
        f"port: {port} | listen_backlog: {listen_backlog} | "
        f"logging_level: {logging_level} | middleware_host: {middleware_host} | "
        f"transformer: {len(transformer_configs)} | queries: {enabled_queries}"
    )

    try:
        shutdown_signal = ShutdownSignal()
        server = Server(
            port,
            listen_backlog,
            middleware_host,
            batch_exchanges,
            transformer_configs,
            enabled_queries,
            shutdown_signal,
        )

        logging.info(f"action: start_gateway | port: {port}")
        server.run()

    except Exception as e:
        logging.error(f"action: gateway | result: fail | error: {e}")


if __name__ == "__main__":
    main()

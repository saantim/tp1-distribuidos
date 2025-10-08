#!/usr/bin/env python3

import configparser
import logging
import os

from core.server import Server

from shared.shutdown import ShutdownSignal


def initialize_config():
    """
    parse env variables or config file to find program config params.
    throws KeyError if param not found, ValueError if parsing fails.
    """
    config = configparser.ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
        config_params["listen_backlog"] = int(os.getenv("LISTEN_BACKLOG", config["DEFAULT"]["LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["middleware_host"] = os.getenv("MIDDLEWARE_HOST", config["MIDDLEWARE"]["MIDDLEWARE_HOST"])

        config_params["stores"] = os.getenv("STORES_QUEUE", config["QUEUES"]["STORES"])
        config_params["users"] = os.getenv("USERS_QUEUE", config["QUEUES"]["USERS"])
        config_params["transactions"] = os.getenv("TRANSACTIONS_QUEUE", config["QUEUES"]["TRANSACTIONS"])
        config_params["transaction_items"] = os.getenv("TRANSACTION_ITEMS_QUEUE", config["QUEUES"]["TRANSACTION_ITEMS"])
        config_params["menu_items"] = os.getenv("MENU_ITEMS_QUEUE", config["QUEUES"]["MENU_ITEMS"])

    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting gateway")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting gateway")

    return config_params


def initialize_log(logging_level):
    """python custom logging initialization."""
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
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

    batch_queues = {
        "STORE": config_params["raw_stores"],
        "USER": config_params["raw_users"],
        "TRANSACTION": config_params["raw_transactions"],
        "TRANSACTION_ITEM": config_params["raw_transaction_items"],
        "MENU_ITEM": config_params["raw_menu_items"],
    }

    initialize_log(logging_level)
    logging.getLogger("pika").setLevel(logging.WARNING)

    logging.debug(
        f"action: config | result: success | "
        f"port: {port} | listen_backlog: {listen_backlog} | "
        f"logging_level: {logging_level} | middleware_host: {middleware_host}"
    )

    try:
        shutdown_signal = ShutdownSignal()
        server = Server(port, listen_backlog, middleware_host, batch_queues, shutdown_signal)

        logging.info(f"action: start_gateway | port: {port}")
        server.run()

    except Exception as e:
        logging.error(f"action: gateway | result: fail | error: {e}")


if __name__ == "__main__":
    main()

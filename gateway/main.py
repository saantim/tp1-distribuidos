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

        config_params["demux_stores"] = os.getenv("DEMUX_STORES", config["DEMUX_QUEUES"]["STORES"])
        config_params["demux_users"] = os.getenv("DEMUX_USERS", config["DEMUX_QUEUES"]["USERS"])
        config_params["demux_transactions"] = os.getenv("DEMUX_TRANSACTIONS", config["DEMUX_QUEUES"]["TRANSACTIONS"])
        config_params["demux_transaction_items"] = os.getenv(
            "DEMUX_TRANSACTION_ITEMS", config["DEMUX_QUEUES"]["TRANSACTION_ITEMS"]
        )
        config_params["demux_menu_items"] = os.getenv("DEMUX_MENU_ITEMS", config["DEMUX_QUEUES"]["MENU_ITEMS"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))

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

    demux_queues = {
        "stores": config_params["demux_stores"],
        "users": config_params["demux_users"],
        "transactions": config_params["demux_transactions"],
        "transaction_items": config_params["demux_transaction_items"],
        "menu_items": config_params["demux_menu_items"],
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
        server = Server(port, listen_backlog, middleware_host, demux_queues, shutdown_signal)

        logging.info(f"action: start_gateway | port: {port}")
        server.run()

    except Exception as e:
        logging.error(f"action: gateway | result: fail | error: {e}")


if __name__ == "__main__":
    main()

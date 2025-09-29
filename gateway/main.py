#!/usr/bin/env python3

import configparser
import logging
import os

from core.router import PacketRouter
from core.server import Server

from shared.entity import MenuItem, Store, Transaction, TransactionItem, User
from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
from shared.protocol import PacketType
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
        config_params["stores_queue"] = os.getenv("STORES_QUEUE", config["QUEUES"]["STORES_QUEUE"])
        config_params["users_queue"] = os.getenv("USERS_QUEUE", config["QUEUES"]["USERS_QUEUE"])
        config_params["transactions_queue"] = os.getenv("TRANSACTIONS_QUEUE", config["QUEUES"]["TRANSACTIONS_QUEUE"])
        config_params["transaction_items_queue"] = os.getenv(
            "TRANSACTION_ITEMS_QUEUE", config["QUEUES"]["TRANSACTION_ITEMS_QUEUE"]
        )
        config_params["menu_items_queue"] = os.getenv("MENU_ITEMS_QUEUE", config["QUEUES"]["MENU_ITEMS_QUEUE"])
        config_params["results_queue"] = os.getenv("RESULTS_QUEUE", config["QUEUES"]["RESULTS_QUEUE"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))

    return config_params


def create_router(config_params) -> PacketRouter:
    """create packet router with middleware publishers and entity mappings."""

    middleware_host = config_params["middleware_host"]

    publishers = {
        PacketType.STORE_BATCH: MessageMiddlewareQueueMQ(middleware_host, queue_name=config_params["stores_queue"]),
        PacketType.USERS_BATCH: MessageMiddlewareQueueMQ(middleware_host, queue_name=config_params["users_queue"]),
        PacketType.TRANSACTIONS_BATCH: MessageMiddlewareQueueMQ(
            middleware_host, queue_name=config_params["transactions_queue"]
        ),
        PacketType.TRANSACTION_ITEMS_BATCH: MessageMiddlewareQueueMQ(
            middleware_host, queue_name=config_params["transaction_items_queue"]
        ),
        PacketType.MENU_ITEMS_BATCH: MessageMiddlewareQueueMQ(
            middleware_host, queue_name=config_params["menu_items_queue"]
        ),
    }

    entity_mappings = {
        PacketType.STORE_BATCH: Store,
        PacketType.USERS_BATCH: User,
        PacketType.TRANSACTIONS_BATCH: Transaction,
        PacketType.TRANSACTION_ITEMS_BATCH: TransactionItem,
        PacketType.MENU_ITEMS_BATCH: MenuItem,
    }

    return PacketRouter(publishers, entity_mappings)


def create_listener(config_params):
    """create result listener with middleware consumer."""
    middleware_host = config_params["middleware_host"]
    results_queue = config_params["results_queue"]

    consumer = MessageMiddlewareQueueMQ(middleware_host, results_queue)
    return consumer


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
    rabbit = config_params["middleware_host"]
    results_queue = config_params["results_queue"]

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | "
        f"port: {port} | listen_backlog: {listen_backlog} | "
        f"logging_level: {logging_level} | rabbit_host: {rabbit} | "
        f"results_queue: {results_queue}"
    )

    try:
        shutdown_signal = ShutdownSignal()
        router = create_router(config_params)
        server = Server(port, listen_backlog, router, rabbit, results_queue, shutdown_signal)

        logging.info(f"action: start_gateway | port: {port} | results_queue: {results_queue}")
        server.run()

    except Exception as e:
        logging.error(f"action: gateway | result: fail | error: {e}")


if __name__ == "__main__":
    main()

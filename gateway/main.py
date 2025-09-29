#!/usr/bin/env python3

import configparser
import logging
import os

from core.server import Server

from shared.middleware.rabbit_mq import MessageMiddlewareQueueMQ
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

        config_params["routing_workers"] = int(os.getenv("ROUTING_WORKERS", config["ROUTING"]["WORKERS"]))
        config_params["routing_queue_size"] = int(os.getenv("ROUTING_QUEUE_SIZE", config["ROUTING"]["QUEUE_SIZE"]))

        config_params["middleware_host"] = os.getenv("MIDDLEWARE_HOST", config["MIDDLEWARE"]["MIDDLEWARE_HOST"])

        config_params["demux_queue"] = os.getenv("DEMUX_QUEUE", config["QUEUES"]["DEMUX_QUEUE"])
        config_params["results_queue"] = os.getenv("RESULTS_QUEUE", config["QUEUES"]["RESULTS_QUEUE"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting gateway".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting gateway".format(e))

    return config_params


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
    demux_queue = config_params["demux_queue"]
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
        server = Server(port, listen_backlog, rabbit, demux_queue, results_queue, shutdown_signal)

        logging.info(f"action: start_gateway | port: {port} | results_queue: {results_queue}")
        server.run()

    except Exception as e:
        logging.error(f"action: gateway | result: fail | error: {e}")


if __name__ == "__main__":
    main()

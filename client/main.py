#!/usr/bin/env python3

import configparser
import logging
import os
from pathlib import Path

from processing.analyzer import Analyzer, AnalyzerConfig, FolderConfig
from processing.batch import BatchConfig

from shared.protocol import MenuItemsBatch, StoreBatch, TransactionItemsBatch, TransactionsBatch, UsersBatch
from shared.shutdown import ShutdownSignal


PACKET_CREATORS = {
    "stores": (lambda rows, eof: StoreBatch(rows, eof), StoreBatch.UNIT_SIZE),
    "users": (lambda rows, eof: UsersBatch(rows, eof), UsersBatch.UNIT_SIZE),
    "transactions": (lambda rows, eof: TransactionsBatch(rows, eof), TransactionsBatch.UNIT_SIZE),
    "transaction_items": (lambda rows, eof: TransactionItemsBatch(rows, eof), TransactionItemsBatch.UNIT_SIZE),
    "menu_items": (lambda rows, eof: MenuItemsBatch(rows, eof), MenuItemsBatch.UNIT_SIZE),
}


def initialize_config():
    """
    parse env variables or config file to find program config params.
    throws KeyError if param not found, ValueError if parsing fails.
    """
    config = configparser.ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["gateway_host"] = os.getenv("GATEWAY_HOST", config["DEFAULT"]["GATEWAY_HOST"])
        config_params["gateway_port"] = int(os.getenv("GATEWAY_PORT", config["DEFAULT"]["GATEWAY_PORT"]))
        config_params["max_bytes"] = int(os.getenv("MAX_BYTES", config["BATCH"]["MAX_BYTES"]))
        config_params["max_rows"] = int(os.getenv("MAX_ROWS", config["BATCH"]["MAX_ROWS"]))
        config_params["data_dir"] = os.getenv("DATA_DIR", config["DATA"]["DATA_DIR"])
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL", config["LOGGING"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def load_folders(config_params) -> list[FolderConfig]:
    """build folder configurations from config parameters."""
    folders = []
    data_dir = Path(config_params["data_dir"])

    for folder_name, (packet_creator, packet_size) in PACKET_CREATORS.items():
        folder_path = data_dir / folder_name
        if folder_path.exists() and folder_path.is_dir():
            folder_config = FolderConfig(str(folder_path), packet_creator, packet_size)
            folders.append(folder_config)
        else:
            raise Exception("Folder {} does not exist".format(folder_path))

    return folders


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

    gateway_host = config_params["gateway_host"]
    gateway_port = config_params["gateway_port"]
    max_bytes = config_params["max_bytes"]
    max_rows = config_params["max_rows"]
    data_dir = config_params["data_dir"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | "
        f"gateway_host: {gateway_host} | "
        f"gateway_port: {gateway_port} | "
        f"max_bytes: {max_bytes} | max_rows: {max_rows} | "
        f"data_dir: {data_dir} |  "
        f"logging_level: {logging_level}"
    )

    try:
        batch_config = BatchConfig(max_bytes, max_rows if max_rows > 0 else None)
        analyzer_config = AnalyzerConfig(gateway_host, gateway_port, batch_config)

        folders = load_folders(config_params)
        if not folders:
            logging.error("action: load_folders | result: no_valid_folders")
            return

        shutdown_signal = ShutdownSignal()

        logging.info(f"action: start_analysis | folders: {len(folders)} | gateway: {gateway_host}:{gateway_port}")
        logging.getLogger("pika").setLevel(logging.WARNING)

        analyzer = Analyzer(analyzer_config, folders, shutdown_signal)
        analyzer.run()
    except Exception as e:
        logging.error(f"action: analysis | result: fail | error: {e}")


if __name__ == "__main__":
    main()

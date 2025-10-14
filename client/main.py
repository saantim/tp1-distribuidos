#!/usr/bin/env python3

import configparser
import logging
import os
from pathlib import Path

from processing.analyzer import Analyzer, AnalyzerConfig, FolderConfig

from shared.protocol import EntityType
from shared.shutdown import ShutdownSignal


ENTITY_FOLDERS = {
    "stores": EntityType.STORE,
    "users": EntityType.USER,
    "transactions": EntityType.TRANSACTION,
    "transaction_items": EntityType.TRANSACTION_ITEM,
    "menu_items": EntityType.MENU_ITEM,
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

        config_params["stores_batch_size"] = int(os.getenv("STORES_BATCH_SIZE", config["BATCH"]["STORES_BATCH_SIZE"]))
        config_params["users_batch_size"] = int(os.getenv("USERS_BATCH_SIZE", config["BATCH"]["USERS_BATCH_SIZE"]))
        config_params["transactions_batch_size"] = int(
            os.getenv("TRANSACTIONS_BATCH_SIZE", config["BATCH"]["TRANSACTIONS_BATCH_SIZE"])
        )
        config_params["transaction_items_batch_size"] = int(
            os.getenv("TRANSACTION_ITEMS_BATCH_SIZE", config["BATCH"]["TRANSACTION_ITEMS_BATCH_SIZE"])
        )
        config_params["menu_items_batch_size"] = int(
            os.getenv("MENU_ITEMS_BATCH_SIZE", config["BATCH"]["MENU_ITEMS_BATCH_SIZE"])
        )

        config_params["data_dir"] = os.getenv("DATA_DIR", config["DATA"]["DATA_DIR"])
        config_params["results_dir"] = os.getenv("RESULTS_DIR", config["DATA"]["RESULTS_DIR"])
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL", config["LOGGING"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting client")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting client")

    return config_params


def load_folders(config_params) -> list[FolderConfig]:
    """build folder configurations from config parameters."""
    folders = []
    data_dir = Path(config_params["data_dir"])

    batch_sizes = {
        EntityType.STORE: config_params["stores_batch_size"],
        EntityType.USER: config_params["users_batch_size"],
        EntityType.TRANSACTION: config_params["transactions_batch_size"],
        EntityType.TRANSACTION_ITEM: config_params["transaction_items_batch_size"],
        EntityType.MENU_ITEM: config_params["menu_items_batch_size"],
    }

    for folder_name, entity_type in ENTITY_FOLDERS.items():
        folder_path = data_dir / folder_name
        if folder_path.exists() and folder_path.is_dir():
            batch_size = batch_sizes[entity_type]
            folder_config = FolderConfig(str(folder_path), entity_type, batch_size)
            folders.append(folder_config)
            logging.debug(
                f"action: load_folder | folder: {folder_name} |"
                f" entity: {entity_type.name} | batch_size: {batch_size}"
            )
        else:
            raise Exception(f"Folder {folder_path} does not exist")

    return folders


def initialize_log(logging_level):
    """python custom logging initialization."""
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    try:
        config_params = initialize_config()
    except (KeyError, ValueError) as e:
        print(f"Configuration error: {e}")
        return

    initialize_log(config_params["logging_level"])

    logging.debug(
        f"action: config | result: success | "
        f"gateway: {config_params['gateway_host']}:{config_params['gateway_port']} | "
        f"data_dir: {config_params['data_dir']} | "
        f"results_dir: {config_params['results_dir']}"
    )

    try:
        analyzer_config = AnalyzerConfig(
            config_params["gateway_host"], config_params["gateway_port"], config_params["results_dir"]
        )

        folders = load_folders(config_params)
        if not folders:
            logging.error("action: load_folders | result: no_valid_folders")
            return

        shutdown_signal = ShutdownSignal()

        logging.info(
            f"action: start_analysis | folders: {len(folders)} |"
            f" gateway: {config_params['gateway_host']}:{config_params['gateway_port']}"
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

        analyzer = Analyzer(analyzer_config, folders, shutdown_signal)
        analyzer.run()
    except Exception as e:
        logging.error(f"action: analysis | result: fail | error: {e}")


if __name__ == "__main__":
    main()

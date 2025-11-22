import configparser
import logging
import os

from health_checker.server import HealthChecker
from shared.shutdown import ShutdownSignal


def initialize_config():
    config = configparser.ConfigParser(os.environ)
    config.read("config.ini")

    return {
        "port": int(os.getenv("PORT", config["DEFAULT"]["PORT"])),
        "check_interval": float(os.getenv("CHECK_INTERVAL", config["DEFAULT"]["CHECK_INTERVAL"])),
        "timeout_threshold": float(os.getenv("TIMEOUT_THRESHOLD", config["DEFAULT"]["TIMEOUT_THRESHOLD"])),
        "logging_level": os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]),
    }


def initialize_log(logging_level):
    logging.basicConfig(
        level=getattr(logging, logging_level.upper(), logging.DEBUG),
        format="HEALTH_CHECKER - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    config = initialize_config()
    initialize_log(config["logging_level"])

    logging.info(
        f"action: config | port: {config['port']} | check_interval: {config['check_interval']} | "
        f"timeout_threshold: {config['timeout_threshold']}"
    )

    shutdown_signal = ShutdownSignal()
    checker = HealthChecker(
        port=config["port"],
        check_interval=config["check_interval"],
        timeout_threshold=config["timeout_threshold"],
        shutdown_signal=shutdown_signal,
    )

    checker.run()


if __name__ == "__main__":
    main()

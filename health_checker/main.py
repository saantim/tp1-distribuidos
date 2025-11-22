import configparser
import logging
import os

from health_checker.server import HealthChecker
from shared.shutdown import ShutdownSignal


def initialize_config():
    config = configparser.ConfigParser(os.environ)
    config.read("config.ini")
    defaults = config["DEFAULT"]

    return {
        "replica_id": int(os.getenv("REPLICA_ID", "0")),
        "replicas": int(os.getenv("REPLICAS", "1")),
        "worker_port": int(os.getenv("WORKER_PORT", defaults["WORKER_PORT"])),
        "worker_timeout": float(os.getenv("WORKER_TIMEOUT", defaults["WORKER_TIMEOUT"])),
        "peer_port": int(os.getenv("PEER_PORT", defaults["PEER_PORT"])),
        "peer_heartbeat_interval": float(os.getenv("PEER_HEARTBEAT_INTERVAL", defaults["PEER_HEARTBEAT_INTERVAL"])),
        "peer_timeout": float(os.getenv("PEER_TIMEOUT", defaults["PEER_TIMEOUT"])),
        "check_interval": float(os.getenv("CHECK_INTERVAL", defaults["CHECK_INTERVAL"])),
        "persist_path": os.getenv("PERSIST_PATH", "/state/registry.json"),
        "logging_level": os.getenv("LOGGING_LEVEL", defaults["LOGGING_LEVEL"]),
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
        f"action: config | replica_id: {config['replica_id']} | replicas: {config['replicas']} | "
        f"worker_port: {config['worker_port']} | peer_port: {config['peer_port']} | "
        f"check_interval: {config['check_interval']} | worker_timeout: {config['worker_timeout']} | "
        f"peer_timeout: {config['peer_timeout']}"
    )

    shutdown_signal = ShutdownSignal()
    checker = HealthChecker(
        replica_id=config["replica_id"],
        replicas=config["replicas"],
        worker_port=config["worker_port"],
        peer_port=config["peer_port"],
        check_interval=config["check_interval"],
        worker_timeout=config["worker_timeout"],
        peer_heartbeat_interval=config["peer_heartbeat_interval"],
        peer_timeout=config["peer_timeout"],
        persist_path=config["persist_path"],
        shutdown_signal=shutdown_signal,
    )

    checker.run()


if __name__ == "__main__":
    main()

import configparser
import logging
import os

from health_checker.core import HealthChecker
from shared.shutdown import ShutdownSignal


def initialize_config():
    config = configparser.ConfigParser()
    config.read("config.ini")

    defaults = config["DEFAULT"]
    worker = config["worker"]
    peer = config["peer"]
    election = config["election"]

    return {
        "replica_id": int(os.getenv("REPLICA_ID", "0")),
        "replicas": int(os.getenv("REPLICAS", "1")),
        "check_interval": float(os.getenv("CHECK_INTERVAL", defaults["CHECK_INTERVAL"])),
        "logging_level": os.getenv("LOGGING_LEVEL", defaults["LOGGING_LEVEL"]),
        "persist_path": os.getenv("PERSIST_PATH", "/state/registry.json"),
        "worker": {
            "port": int(os.getenv("WORKER_PORT", worker["PORT"])),
            "timeout": float(os.getenv("WORKER_TIMEOUT", worker["TIMEOUT"])),
            "heartbeat_interval": float(os.getenv("WORKER_HEARTBEAT_INTERVAL", worker["HEARTBEAT_INTERVAL"])),
        },
        "peer": {
            "port": int(os.getenv("PEER_PORT", peer["PORT"])),
            "heartbeat_interval": float(os.getenv("PEER_HEARTBEAT_INTERVAL", peer["HEARTBEAT_INTERVAL"])),
            "timeout": float(os.getenv("PEER_TIMEOUT", peer["TIMEOUT"])),
        },
        "election": {
            "timeout": float(os.getenv("ELECTION_TIMEOUT", election["TIMEOUT"])),
            "coordinator_timeout": float(os.getenv("COORDINATOR_TIMEOUT", election["COORDINATOR_TIMEOUT"])),
        },
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
        f"check_interval: {config['check_interval']} | "
        f"worker: {config['worker']} | peer: {config['peer']} | election: {config['election']}"
    )

    shutdown_signal = ShutdownSignal()
    checker = HealthChecker(
        replica_id=config["replica_id"],
        replicas=config["replicas"],
        worker_port=config["worker"]["port"],
        peer_port=config["peer"]["port"],
        check_interval=config["check_interval"],
        worker_timeout=config["worker"]["timeout"],
        peer_heartbeat_interval=config["peer"]["heartbeat_interval"],
        peer_timeout=config["peer"]["timeout"],
        election_timeout=config["election"]["timeout"],
        coordinator_timeout=config["election"]["coordinator_timeout"],
        persist_path=config["persist_path"],
        shutdown_signal=shutdown_signal,
    )

    checker.run()


if __name__ == "__main__":
    main()

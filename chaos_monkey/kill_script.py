# /app/kill_script.py
import logging
import sys

from chaos_monkey.core.config import initialize_config
from chaos_monkey.core.service import ChaosMonkey
from chaos_monkey.main import initialize_log
from shared.shutdown import ShutdownSignal


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    prefixes_arg = sys.argv[1] if len(sys.argv) > 1 else ""
    prefixes = [p.strip() for p in prefixes_arg.split(",") if p.strip()]

    configuration = initialize_config()
    initialize_log(configuration.logging_level)
    signal_handler = ShutdownSignal()
    chaos_monkey = ChaosMonkey(configuration, signal_handler)

    if prefixes:
        chaos_monkey.kill_containers_by_prefix(prefixes)
    else:
        chaos_monkey.kill_all_containers()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted by user (Ctrl+C). Shutting down...")
        sys.exit(0)

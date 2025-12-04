# /app/kill_script.py
import logging
import sys
import time

from chaos_monkey.core.config import initialize_config
from chaos_monkey.core.service import ChaosMonkey
from chaos_monkey.main import initialize_log
from shared.shutdown import ShutdownSignal


def parse_args():
    args = {"prefixes": "", "loop": None}

    for arg in sys.argv[1:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            if key == "prefixes":
                args["prefixes"] = value
            elif key == "loop":
                args["loop"] = int(value)
        else:
            args["prefixes"] = arg

    return args


def run_loop(chaos_monkey, prefixes, interval):
    iteration = 0
    while True:
        iteration += 1
        logging.info(f"Loop iteration {iteration} - executing kill operation")

        if prefixes:
            chaos_monkey.kill_containers_by_prefix(prefixes)
        else:
            chaos_monkey.kill_all_containers()

        logging.info(f"Next execution in {interval} seconds...")
        time.sleep(interval)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()
    prefixes_arg = args["prefixes"]
    loop_interval = args["loop"]

    prefixes = [p.strip() for p in prefixes_arg.split(",") if p.strip()]

    configuration = initialize_config()
    initialize_log(configuration.logging_level)
    signal_handler = ShutdownSignal()
    chaos_monkey = ChaosMonkey(configuration, signal_handler)

    if loop_interval is not None:
        logging.info(f"Starting loop mode with interval={loop_interval}s")
        run_loop(chaos_monkey, prefixes, loop_interval)
    else:
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

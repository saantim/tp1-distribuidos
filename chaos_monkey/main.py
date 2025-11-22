import logging
from chaos_monkey.core.config import initialize_config
from chaos_monkey.core.service import ChaosMonkey
from shared.shutdown import ShutdownSignal

def initialize_log(logging_level):
    """
    Configure the root logger with a consistent format and level.

    Args:
        logging_level: Logging level name (e.g. ``\"DEBUG\"``, ``\"INFO\"``).
                       If the value is invalid, it defaults to ``DEBUG``.
    """
    level = getattr(logging, logging_level.upper(), logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="CHAOS_MONKEY - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger(__name__).info(
        "Logging initialized with level '%s'", logging.getLevelName(level)
    )

if __name__ == "__main__":
    configuration = initialize_config()
    initialize_log(configuration.logging_level)
    signal_handler = ShutdownSignal()
    chaos_monkey = ChaosMonkey(configuration, signal_handler)
    chaos_monkey.run()
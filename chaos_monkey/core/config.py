import configparser
import logging
import os

import pydantic


class ChaosMonkeyConfiguration(pydantic.BaseModel):
    """
    Runtime configuration for the Chaos Monkey process.

    Attributes:
        filter_prefix: List of container name prefixes that should never be terminated.
        full_enabled: Whether full mode (kill all containers) is enabled.
        full_interval: Number of seconds to wait between full kill attempts.
        single_enabled: Whether single mode (kill one random container) is enabled.
        single_interval: Number of seconds to wait between single kill attempts.
        start_delay: Number of seconds to wait before starting chaos monkey threads.
        logging_level: Logging level to use (e.g. ``\"DEBUG\"``, ``\"INFO\"``).
    """

    filter_prefix: list[str]
    full_enabled: bool
    full_interval: float
    single_enabled: bool
    single_interval: float
    start_delay: float
    logging_level: str


def initialize_config():
    """
    Load Chaos Monkey configuration from the environment and config file.

    The function reads the ``config.ini`` file and then allows environment
    variables to override specific keys. The following configuration entries
    are expected:

    - ``FILTER_PREFIX``: Comma-separated list of container name prefixes to exclude.
    - ``FULL_ENABLED``: Whether full mode is enabled (true/false).
    - ``FULL_INTERVAL``: Time in seconds between full kill attempts.
    - ``SINGLE_ENABLED``: Whether single mode is enabled (true/false).
    - ``SINGLE_INTERVAL``: Time in seconds between single kill attempts.
    - ``START_DELAY``: Time in seconds to wait before starting chaos monkey threads.
    - ``LOGGING_LEVEL``: Logging level string (e.g. ``\"INFO\"``, ``\"DEBUG\"``).

    Returns:
        A fully populated `ChaosMonkeyConfiguration` instance.

    Raises:
        KeyError: If required configuration keys are missing from the file
                  and not provided via environment variables.
        ValueError: If interval values cannot be converted to ``float`` or
                    enabled values cannot be converted to ``bool``.
    """
    CONFIG_FILE = "../config.ini"
    CONFIG_TYPE = "DEFAULT"
    FILTER_PREFIX_KEY = "FILTER_PREFIX"
    FULL_ENABLED_KEY = "FULL_ENABLED"
    FULL_INTERVAL_KEY = "FULL_INTERVAL"
    SINGLE_ENABLED_KEY = "SINGLE_ENABLED"
    SINGLE_INTERVAL_KEY = "SINGLE_INTERVAL"
    START_DELAY_KEY = "START_DELAY"
    LOGGING_LEVEL_KEY = "LOGGING_LEVEL"

    config = configparser.ConfigParser(os.environ)
    config.read(CONFIG_FILE)

    def str_to_bool(value: str) -> bool:
        """Convert string to boolean."""
        return value.lower() in ("true", "1", "yes")

    configuration = ChaosMonkeyConfiguration(
        filter_prefix=os.getenv(FILTER_PREFIX_KEY, config[CONFIG_TYPE][FILTER_PREFIX_KEY]).split(","),
        full_enabled=str_to_bool(os.getenv(FULL_ENABLED_KEY, config[CONFIG_TYPE][FULL_ENABLED_KEY])),
        full_interval=float(os.getenv(FULL_INTERVAL_KEY, config[CONFIG_TYPE][FULL_INTERVAL_KEY])),
        single_enabled=str_to_bool(os.getenv(SINGLE_ENABLED_KEY, config[CONFIG_TYPE][SINGLE_ENABLED_KEY])),
        single_interval=float(os.getenv(SINGLE_INTERVAL_KEY, config[CONFIG_TYPE][SINGLE_INTERVAL_KEY])),
        start_delay=float(os.getenv(START_DELAY_KEY, config[CONFIG_TYPE][START_DELAY_KEY])),
        logging_level=os.getenv(LOGGING_LEVEL_KEY, config[CONFIG_TYPE][LOGGING_LEVEL_KEY]),
    )

    logging.getLogger(__name__).debug("Chaos Monkey configuration loaded: %s", configuration)

    return configuration

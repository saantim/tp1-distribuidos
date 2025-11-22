import configparser
import logging
import os

import pydantic


class ChaosMonkeyConfiguration(pydantic.BaseModel):
    """
    Runtime configuration for the Chaos Monkey process.

    Attributes:
        containers_excluded: List of container names that should never be terminated.
        interval: Number of seconds to wait between kill attempts.
        logging_level: Logging level to use (e.g. ``\"DEBUG\"``, ``\"INFO\"``).
    """
    containers_excluded: list[str]
    interval: float
    logging_level: str


def initialize_config():
    """
    Load Chaos Monkey configuration from the environment and config file.

    The function reads the ``config.ini`` file and then allows environment
    variables to override specific keys. The following configuration entries
    are expected:

    - ``CONTAINERS_EXCLUDED``: Comma-separated list of container names.
    - ``KILL_INTERVAL``: Time in seconds between kill attempts.
    - ``LOGGING_INTERVAL``: Logging level string (e.g. ``\"INFO\"``, ``\"DEBUG\"``).

    Returns:
        A fully populated :class:`ChaosMonkeyConfiguration` instance.

    Raises:
        KeyError: If required configuration keys are missing from the file
                  and not provided via environment variables.
        ValueError: If the interval value cannot be converted to ``float``.
    """
    CONFIG_FILE = "../config.ini"
    CONFIG_TYPE = "DEFAULT"
    CONTAINERS_EXCLUDED_KEY = "CONTAINERS_EXCLUDED"
    KILL_INTERVAL_KEY = "KILL_INTERVAL"
    LOGGING_LEVEL_KEY = "LOGGING_LEVEL"

    config = configparser.ConfigParser(os.environ)
    config.read(CONFIG_FILE)

    configuration = ChaosMonkeyConfiguration(
        containers_excluded = os.getenv(CONTAINERS_EXCLUDED_KEY, config[CONFIG_TYPE][CONTAINERS_EXCLUDED_KEY]).split(","),
        interval = float(os.getenv(KILL_INTERVAL_KEY, config[CONFIG_TYPE][KILL_INTERVAL_KEY])),
        logging_level = os.getenv(LOGGING_LEVEL_KEY, config[CONFIG_TYPE][LOGGING_LEVEL_KEY]),
    )

    logging.getLogger(__name__).debug(
        "Chaos Monkey configuration loaded: %s", configuration
    )

    return configuration

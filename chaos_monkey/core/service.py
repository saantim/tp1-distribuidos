import logging
import random
from typing import Optional

from chaos_monkey.core.config import ChaosMonkeyConfiguration
from chaos_monkey.core.docker_manager import Container, DockerManager
from shared.shutdown import ShutdownSignal


class ChaosMonkey:
    """
    Periodically kills random Docker containers to simulate failures.

    The Chaos Monkey runs in a loop until a :class:`ShutdownSignal` indicates
    that it should stop. At each iteration, it waits for the configured
    interval, picks a random eligible container, and attempts to stop it.

    Attributes:
        _config: Configuration parameters controlling the monkey's behavior.
        _shutdown_signal: Cooperative shutdown signal used to stop the loop.
    """

    def __init__(self, config: ChaosMonkeyConfiguration, shutdown_signal: ShutdownSignal):
        """
        Initialize a new Chaos Monkey instance.

        Args:
            config: Configuration object specifying intervals and exclusions.
            shutdown_signal: Signal to check periodically to decide when to stop.
        """
        self._config = config
        self._shutdown_signal = shutdown_signal

    def run(self):
        """
        Start the Chaos Monkey main loop.

        The loop sleeps for the configured interval, checks the shutdown signal,
        and, if still active, selects and kills a random container (if any are
        eligible). Any unexpected exceptions are logged and the loop continues
        until shutdown is requested.
        """
        logging.info(
            f"Chaos Monkey started (interval={self._config.interval},"
            f" excluded_containers={self._config.containers_excluded})",
        )

        while not self._shutdown_signal.wait(timeout=self._config.interval):
            try:
                container = self._select_container_to_kill()
                if container:
                    self._kill_container(container)
            except Exception:
                logging.exception("Unhandled error in Chaos Monkey main loop")

        logging.info("Shutdown signal received, stopping Chaos Monkey loop")

        logging.info("Main loop stopped")

    def _select_container_to_kill(self) -> Optional[Container]:
        """
        Select a random container to kill, excluding configured names.

        Returns:
            A randomly selected :class:`Container` instance, or ``None`` if
            there are no eligible containers.
        """
        containers: list[Container] = DockerManager.get_containers()

        logging.debug(
            "Total containers before exclusion filter: %d", len(containers)
        )

        containers_filtered = []

        for container in containers:
            excluded = False
            for prefix in self._config.containers_excluded:
                if prefix in container.Names:
                    excluded = True
                    break
            if not excluded:
                containers_filtered.append(container)

        logging.debug(
            "Eligible containers after exclusion filter: %d", len(containers_filtered)
        )

        if containers_filtered:
            selected = random.choice(containers_filtered)
            logging.info(f"Container selected for termination: name={selected.Names!r} id={selected.ID!r}")
            return selected

        logging.warning("No eligible containers found to kill")
        return None

    def _kill_container(self, container: Container) -> None:
        """
        Kill the given container using the :class:`DockerManager`.

        Args:
            container: Container to be terminated.
        """
        logging.debug(
            f"Attempting to kill container name={container.Names!r} id={container.ID!r}",
        )
        DockerManager.kill_container(container)
        logging.info(
            f"Container name={container.Names!r} id={container.ID!r} stopped successfully",
        )

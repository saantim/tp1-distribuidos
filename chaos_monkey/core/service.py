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
    HEALTH_PREFIX = 'health_checker'

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
                    self._safe_kill_container(container)
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

    def kill_containers_by_prefix(self, prefixes: list[str]) -> None:
        """
        Kill all running containers whose name contains any of the given prefixes,
        excluding those configured in `containers_excluded`.
        """
        logging.info("!!! Killing containers matching prefixes: %s", ", ".join(prefixes))

        containers: list[Container] = DockerManager.get_containers()
        killed = 0

        for container in containers:
            name = container.Names

            excluded = any(ex_prefix in name for ex_prefix in self._config.containers_excluded)
            if excluded:
                continue

            matches_prefix = any(prefix in name for prefix in prefixes)
            if prefixes and not matches_prefix:
                continue

            try:
                killed = killed + (1 if self._safe_kill_container(container) else 0)
            except Exception as e:
                logging.error(f"Failed to kill container {name}: {e}")

        logging.info(
            "Killed %d containers matching prefixes %s",
            killed,
            ", ".join(prefixes),
        )

    def kill_all_containers(self) -> None:
        """
        Kill all running containers, excluding those in the exclusion list.
        """
        logging.info("!!! Killing all containers")

        containers: list[Container] = DockerManager.get_containers()
        killed = 0

        for container in containers:
            excluded = any(prefix in container.Names for prefix in self._config.containers_excluded)
            if not excluded:
                try:
                    killed = killed + (1 if self._safe_kill_container(container) else 0)
                except Exception as e:
                    logging.error(f"Failed to kill container {container.Names}: {str(e)}")

        logging.info(f"Killed {killed} containers")


    def _safe_kill_container(self, container: Container) -> bool:
        """
        Safely kill a container after performing necessary validations.
        
        Args:
            container: The container to be killed
            containers: List of all containers for validation
            
        Returns:
            bool: True if the container was killed, False otherwise
        """
        if self.HEALTH_PREFIX in container.Names:
            running_health_checkers = [
                c for c in DockerManager.get_containers()
                if self.HEALTH_PREFIX in c.Names 
                and c.State == 'running'
            ]
            
            if len(running_health_checkers) <= 1:
                logging.warning("Avoid killing last health checker")
                return False

        self._kill_container(container)
        return True

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

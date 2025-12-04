import logging
import random
import threading
from typing import Optional

from chaos_monkey.core.config import ChaosMonkeyConfiguration
from chaos_monkey.core.docker_manager import Container, DockerManager
from shared.shutdown import ShutdownSignal


class ChaosMonkey:
    """
    Periodically kills random Docker containers to simulate failures.

    The Chaos Monkey runs in a loop until a `ShutdownSignal` indicates
    that it should stop. At each iteration, it waits for the configured
    interval, picks a random eligible container, and attempts to stop it.

    Attributes:
        _config: Configuration parameters controlling the monkey's behavior.
        _shutdown_signal: Cooperative shutdown signal used to stop the loop.
    """

    HEALTH_PREFIX = "health_checker"

    def __init__(self, config: ChaosMonkeyConfiguration, shutdown_signal: ShutdownSignal):
        """
        Initialize a new Chaos Monkey instance.

        Args:
            config: Configuration object specifying intervals and exclusions.
            shutdown_signal: Signal to check periodically to decide when to stop.
        """
        self._config = config
        self._shutdown_signal = shutdown_signal
        self._docker_lock = threading.RLock()

    def start(self):
        """
        Start the Chaos Monkey threads based on configuration.

        Spawns threads for enabled modes (single and/or full) and waits for shutdown signal.
        """
        threads = []

        if not self._config.single_enabled and not self._config.full_enabled:
            logging.warning("Chaos Monkey enabled but no modes are active. Exiting.")
            return

        if self._config.start_delay > 0:
            logging.info(f"Waiting {self._config.start_delay} seconds before starting chaos monkey threads")
            if self._shutdown_signal.wait(timeout=self._config.start_delay):
                logging.info("Shutdown signal received during start delay, exiting")
                return

        if self._config.single_enabled:
            logging.info(
                f"Starting single mode (interval={self._config.single_interval}s, "
                f"filter_prefix={self._config.filter_prefix})"
            )
            single_thread = threading.Thread(target=self.run_single_mode, name="SINGLE")
            single_thread.daemon = False
            single_thread.start()
            threads.append(single_thread)

        if self._config.full_enabled:
            logging.info(
                f"Starting full mode (interval={self._config.full_interval}s, "
                f"filter_prefix={self._config.filter_prefix})"
            )
            full_thread = threading.Thread(target=self.run_full_mode, name="FULL")
            full_thread.daemon = False
            full_thread.start()
            threads.append(full_thread)

        self._shutdown_signal.wait()
        logging.info("Shutdown signal received, waiting for threads to finish")

        for thread in threads:
            thread.join()

        logging.info("All Chaos Monkey threads stopped")

    def run_single_mode(self):
        """
        Run single mode: kill one random container per interval.

        The loop sleeps for the configured single interval, checks the shutdown signal,
        and, if still active, selects and kills a random container (if any are eligible).
        """
        logging.info("Single mode thread started")

        while not self._shutdown_signal.wait(timeout=self._config.single_interval):
            try:
                container = self._select_container_to_kill()
                if container:
                    self._kill_container(container)
            except Exception:
                logging.exception("Unhandled error in single mode loop")

        logging.info("Single mode thread stopped")

    def run_full_mode(self):
        """
        Run full mode: kill all eligible containers per interval.

        The loop sleeps for the configured full interval, checks the shutdown signal,
        and, if still active, kills all eligible containers.
        """
        logging.info("Full mode thread started")

        while not self._shutdown_signal.wait(timeout=self._config.full_interval):
            try:
                self.kill_all_containers()
            except Exception:
                logging.exception("Unhandled error in full mode loop")

        logging.info("Full mode thread stopped")

    def _select_container_to_kill(self) -> Optional[Container]:
        """
        Select a random container to kill, excluding configured names.

        Returns:
            A randomly selected `Container` instance, or ``None`` if
            there are no eligible containers.
        """
        with self._docker_lock:
            containers: list[Container] = DockerManager.get_containers()

            logging.debug("Total containers before exclusion filter: %d", len(containers))

            containers_filtered = []

            spared_id = self._get_spared_health_check_id(containers)

            for container in containers:

                if container.ID == spared_id:
                    continue

                excluded = False
                for prefix in self._config.filter_prefix:
                    if prefix in container.Names:
                        excluded = True
                        break

                if not excluded:
                    containers_filtered.append(container)

            logging.debug("Eligible containers after exclusion filter: %d", len(containers_filtered))

            if containers_filtered:
                selected = random.choice(containers_filtered)
                logging.info(f"Container selected for termination: name={selected.Names!r} id={selected.ID!r}")
                return selected

            logging.warning("No eligible containers found to kill")
            return None

    def kill_containers_by_prefix(self, prefixes: list[str]) -> None:
        """
        Kill all running containers whose name contains any of the given prefixes,
        excluding those configured in `filter_prefix`.
        """
        logging.info("!!! Killing containers matching prefixes: %s", ", ".join(prefixes))

        with self._docker_lock:
            containers: list[Container] = DockerManager.get_containers()
            killed = 0

            spared_id = self._get_spared_health_check_id(containers)

            for container in containers:

                if container.ID == spared_id:
                    continue

                name = container.Names

                excluded = any(ex_prefix in name for ex_prefix in self._config.filter_prefix)
                if excluded:
                    continue

                matches_prefix = any(prefix in name for prefix in prefixes)
                if prefixes and not matches_prefix:
                    continue

                try:
                    self._kill_container(container)
                    killed += 1
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

        with self._docker_lock:
            containers: list[Container] = DockerManager.get_containers()
            killed = 0

            spared_id = self._get_spared_health_check_id(containers)

            for container in containers:

                if container.ID == spared_id:
                    continue

                excluded = any(prefix in container.Names for prefix in self._config.filter_prefix)
                if not excluded:
                    try:
                        self._kill_container(container)
                        killed += 1
                    except Exception as e:
                        logging.error(f"Failed to kill container {container.Names}: {str(e)}")

            logging.info(f"Killed {killed} containers")

    def _kill_container(self, container: Container) -> None:
        """
        Kill the given container using the `DockerManager`.

        Args:
            container: Container to be terminated.
        """
        logging.debug(
            f"Attempting to kill container name={container.Names!r} id={container.ID!r}",
        )
        with self._docker_lock:
            DockerManager.kill_container(container)
        logging.info(
            f"Container name={container.Names!r} id={container.ID!r} stopped successfully",
        )

    def _get_spared_health_check_id(self, containers: list[Container]) -> Optional[str]:
        """
        Identify a health checker container to spare from termination.

        Args:
            containers: List of current containers.

        Returns:
            The ID of the container to spare, or None if no sparing is needed.
        """
        health_checkers = [c for c in containers if self.HEALTH_PREFIX in c.Names]
        if health_checkers:
            random.shuffle(health_checkers)
            spared_container = health_checkers[0]
            logging.info(f"Sparing health checker: {spared_container.Names}")
            return spared_container.ID
        return None

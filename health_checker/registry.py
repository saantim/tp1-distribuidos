import threading
import time
from dataclasses import dataclass


@dataclass
class WorkerInfo:
    """Information about a registered worker."""

    container_name: str
    last_heartbeat: float


class WorkerRegistry:
    """Thread-safe registry for tracking worker heartbeats."""

    def __init__(self):
        self._workers: dict[str, WorkerInfo] = {}
        self._lock = threading.Lock()

    def update(self, container_name: str, timestamp: float) -> None:
        """Register or update a worker's heartbeat."""
        with self._lock:
            self._workers[container_name] = WorkerInfo(
                container_name=container_name,
                last_heartbeat=timestamp,
            )

    def get_dead_workers(self, timeout_threshold: float) -> list[WorkerInfo]:
        """Return list of workers that haven't sent a heartbeat within threshold."""
        now = time.time()
        dead = []
        with self._lock:
            for worker in self._workers.values():
                if now - worker.last_heartbeat > timeout_threshold:
                    dead.append(worker)
        return dead

    def get_all_workers(self) -> list[WorkerInfo]:
        """Return list of all registered workers."""
        with self._lock:
            return list(self._workers.values())

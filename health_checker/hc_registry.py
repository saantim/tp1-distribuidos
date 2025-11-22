import threading
import time
from dataclasses import dataclass


@dataclass
class HCInfo:
    """Information about a registered health checker."""

    hc_id: int
    last_heartbeat: float


class HCRegistry:
    """Thread-safe registry for tracking health checker heartbeats."""

    def __init__(self, my_id: int):
        """
        Initialize the registry.

        Args:
            my_id: This health checker's ID (excluded from alive/dead queries).
        """
        self._my_id = my_id
        self._health_checkers: dict[int, HCInfo] = {}
        self._lock = threading.Lock()

    def update(self, hc_id: int, timestamp: float) -> None:
        """Register or update a health checker's heartbeat."""
        if hc_id == self._my_id:
            return  # Don't track ourselves

        with self._lock:
            self._health_checkers[hc_id] = HCInfo(
                hc_id=hc_id,
                last_heartbeat=timestamp,
            )

    def get_alive_ids(self, timeout: float) -> list[int]:
        """Return list of HC IDs that have sent a heartbeat within timeout."""
        now = time.time()
        alive = []
        with self._lock:
            for hc in self._health_checkers.values():
                if now - hc.last_heartbeat <= timeout:
                    alive.append(hc.hc_id)
        return alive

    def get_dead_ids(self, timeout: float) -> list[int]:
        """Return list of HC IDs that haven't sent a heartbeat within timeout."""
        now = time.time()
        dead = []
        with self._lock:
            for hc in self._health_checkers.values():
                if now - hc.last_heartbeat > timeout:
                    dead.append(hc.hc_id)
        return dead

    def remove(self, hc_id: int) -> None:
        """Remove an HC from the registry (e.g., after revival)."""
        with self._lock:
            self._health_checkers.pop(hc_id, None)

    def get_all(self) -> list[HCInfo]:
        """Return list of all registered health checkers."""
        with self._lock:
            return list(self._health_checkers.values())

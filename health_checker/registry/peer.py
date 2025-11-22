"""Thread-safe registry for tracking peer health checker heartbeats."""

import threading
import time
from dataclasses import dataclass


@dataclass
class PeerInfo:
    """Information about a peer health checker."""

    peer_id: int
    last_heartbeat: float


class PeerRegistry:
    """Thread-safe registry for tracking peer health checker heartbeats."""

    def __init__(self, my_id: int):
        """
        Initialize the registry.

        Args:
            my_id: This health checker's ID (excluded from alive/dead queries).
        """
        self._my_id = my_id
        self._peers: dict[int, PeerInfo] = {}
        self._lock = threading.Lock()

    def update(self, peer_id: int, timestamp: float) -> None:
        """Register or update a peer's heartbeat."""
        if peer_id == self._my_id:
            return

        with self._lock:
            self._peers[peer_id] = PeerInfo(
                peer_id=peer_id,
                last_heartbeat=timestamp,
            )

    def get_alive_ids(self, timeout: float) -> list[int]:
        """Return list of peer IDs that have sent a heartbeat within timeout."""
        now = time.time()
        alive = []
        with self._lock:
            for peer in self._peers.values():
                if now - peer.last_heartbeat <= timeout:
                    alive.append(peer.peer_id)
        return alive

    def get_dead_ids(self, timeout: float) -> list[int]:
        """Return list of peer IDs that haven't sent a heartbeat within timeout."""
        now = time.time()
        dead = []
        with self._lock:
            for peer in self._peers.values():
                if now - peer.last_heartbeat > timeout:
                    dead.append(peer.peer_id)
        return dead

    def remove(self, peer_id: int) -> None:
        """Remove a peer from the registry."""
        with self._lock:
            self._peers.pop(peer_id, None)

    def get_all(self) -> list[PeerInfo]:
        """Return list of all registered peers."""
        with self._lock:
            return list(self._peers.values())

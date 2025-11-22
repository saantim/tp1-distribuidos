"""
Leader election for health checkers.

The leader is the HC with the lowest ID among those alive. A revived HC becomes
standby until the current leader dies, preventing recently revived HCs with
incomplete state from immediately taking leadership.
"""

import logging

from health_checker.registry.peer import PeerRegistry


class LeaderElection:
    """Determines which health checker is the leader."""

    def __init__(self, my_id: int, peer_registry: PeerRegistry, peer_timeout: float):
        self._my_id = my_id
        self._peer_registry = peer_registry
        self._peer_timeout = peer_timeout
        self._current_leader: int | None = None

    def am_i_leader(self) -> bool:
        """Check if this HC is the current leader."""
        alive_ids = self._peer_registry.get_alive_ids(self._peer_timeout)
        alive_ids.append(self._my_id)

        if self._current_leader is not None and self._current_leader in alive_ids:
            return self._my_id == self._current_leader

        new_leader = min(alive_ids)
        if new_leader != self._current_leader:
            logging.info(f"action: leader_change | previous: {self._current_leader} | new: {new_leader}")
            self._current_leader = new_leader

        return self._my_id == self._current_leader

    def get_current_leader(self) -> int | None:
        """Return the current leader ID, or None if not yet determined."""
        return self._current_leader

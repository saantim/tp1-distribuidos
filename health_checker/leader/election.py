"""
Bully algorithm implementation for leader election among health checkers.

Algorithm:
1. Highest ID wins (coordinator)
2. On startup or leader death: start election
3. Send ELECTION to all higher-ID processes
4. If any higher-ID responds with OK: wait for COORDINATOR
5. If no OK within timeout: become leader, broadcast COORDINATOR
6. On receiving COORDINATOR: accept sender as leader
"""

import logging
import threading
import time
from enum import auto, Enum
from typing import Callable


class ElectionState(Enum):
    """States in the Bully election state machine."""

    IDLE = auto()  # No election in progress
    ELECTING = auto()  # Sent ELECTION, waiting for OK
    WAITING_COORDINATOR = auto()  # Received OK, waiting for COORDINATOR


class BullyElection:
    """
    Bully algorithm leader election.

    Thread-safe implementation using locks and condition variables.
    Requires injection of send callbacks for network communication.
    """

    def __init__(
        self,
        my_id: int,
        total_replicas: int,
        election_timeout: float,
        coordinator_timeout: float,
        send_election_fn: Callable[[int], None],
        send_ok_fn: Callable[[int], None],
        send_coordinator_fn: Callable[[], None],
    ):
        """
        Args:
            my_id: This health checker's replica ID
            total_replicas: Total number of health checker replicas
            election_timeout: Seconds to wait for OK responses
            coordinator_timeout: Seconds to wait for COORDINATOR after receiving OK
            send_election_fn: Callback to send ELECTION to a specific peer ID
            send_ok_fn: Callback to send OK to a specific peer ID
            send_coordinator_fn: Callback to broadcast COORDINATOR to all peers
        """
        self._my_id = my_id
        self._total_replicas = total_replicas
        self._election_timeout = election_timeout
        self._coordinator_timeout = coordinator_timeout

        self._send_election = send_election_fn
        self._send_ok = send_ok_fn
        self._send_coordinator = send_coordinator_fn

        self._lock = threading.Lock()
        self._state = ElectionState.IDLE
        self._current_leader: int | None = None
        self._election_start_time: float | None = None
        self._ok_received = False

        # For waking up waiting threads
        self._state_changed = threading.Condition(self._lock)

    def start_election(self) -> None:
        """
        Initiate an election. Call this on startup or when leader is detected dead.

        If we have the highest ID, we immediately become leader.
        Otherwise, we send ELECTION to all higher-ID processes and wait for responses.
        """
        with self._lock:
            if self._state != ElectionState.IDLE:
                logging.debug(f"action: election_skip | reason: already_in_progress | state: {self._state.name}")
                return

            logging.info(f"action: election_start | my_id: {self._my_id}")
            self._state = ElectionState.ELECTING
            self._election_start_time = time.time()
            self._ok_received = False

        # Send ELECTION to all higher-ID processes
        higher_ids = list(range(self._my_id + 1, self._total_replicas))

        if not higher_ids:
            # We have the highest ID, immediately become leader
            self._become_leader()
            return

        for peer_id in higher_ids:
            try:
                self._send_election(peer_id)
            except Exception as e:
                logging.debug(f"action: send_election_fail | peer: {peer_id} | error: {e}")

        # Start timeout thread to check for OK responses
        threading.Thread(target=self._election_timeout_handler, daemon=True).start()

    def _election_timeout_handler(self) -> None:
        """Handle election timeout - if no OK received, become leader."""
        time.sleep(self._election_timeout)

        with self._lock:
            if self._state != ElectionState.ELECTING:
                return

            if self._ok_received:
                # Transition to waiting for coordinator
                self._state = ElectionState.WAITING_COORDINATOR
                self._state_changed.notify_all()
                logging.debug("action: election_ok_received | waiting_for_coordinator: true")
            else:
                # No OK received, we win
                self._state = ElectionState.IDLE
                self._state_changed.notify_all()

        if not self._ok_received:
            self._become_leader()
        else:
            # Start coordinator timeout
            threading.Thread(target=self._coordinator_timeout_handler, daemon=True).start()

    def _coordinator_timeout_handler(self) -> None:
        """Handle coordinator timeout - if no COORDINATOR received, restart election."""
        time.sleep(self._coordinator_timeout)

        with self._lock:
            if self._state != ElectionState.WAITING_COORDINATOR:
                return

            logging.warning("action: coordinator_timeout | restarting_election: true")
            self._state = ElectionState.IDLE
            self._state_changed.notify_all()

        # Restart election
        self.start_election()

    def _become_leader(self) -> None:
        """Become the leader and broadcast COORDINATOR to all."""
        with self._lock:
            self._current_leader = self._my_id
            self._state = ElectionState.IDLE
            self._state_changed.notify_all()

        logging.info(f"action: become_leader | leader_id: {self._my_id}")

        # Broadcast COORDINATOR to all peers
        try:
            self._send_coordinator()
        except Exception as e:
            logging.error(f"action: send_coordinator_fail | error: {e}")

    def handle_election(self, from_id: int) -> None:
        """
        Handle received ELECTION message from a lower-ID process.

        We respond with OK and start our own election (if not already in progress).
        """
        logging.debug(f"action: received_election | from: {from_id}")

        # Send OK to the sender
        try:
            self._send_ok(from_id)
        except Exception as e:
            logging.debug(f"action: send_ok_fail | to: {from_id} | error: {e}")

        # Start our own election (will be skipped if already in progress)
        self.start_election()

    def handle_ok(self, from_id: int) -> None:
        """
        Handle received OK message from a higher-ID process.

        This means a higher-ID process is alive and will take over.
        """
        logging.debug(f"action: received_ok | from: {from_id}")

        with self._lock:
            if self._state == ElectionState.ELECTING:
                self._ok_received = True

    def handle_coordinator(self, leader_id: int) -> None:
        """
        Handle received COORDINATOR message.

        Accept the sender as the new leader.
        """
        with self._lock:
            old_leader = self._current_leader
            self._current_leader = leader_id
            self._state = ElectionState.IDLE
            self._state_changed.notify_all()

        if old_leader != leader_id:
            logging.info(f"action: accept_leader | leader: {leader_id}")

    def am_i_leader(self) -> bool:
        """Check if this HC is the current leader."""
        with self._lock:
            return self._current_leader == self._my_id

    def get_current_leader(self) -> int | None:
        """Return the current leader ID, or None if not yet determined."""
        with self._lock:
            return self._current_leader

    def has_leader(self) -> bool:
        """Check if a leader has been elected."""
        with self._lock:
            return self._current_leader is not None

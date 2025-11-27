import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from socket import socket
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4


class SessionState(Enum):
    """Session state for coordinating socket ownership between threads."""

    UPLOADING = "uploading"
    READY_FOR_RESULTS = "ready"
    COMPLETED = "completed"


@dataclass
class SessionData:

    id: UUID
    socket: socket
    client_address: Tuple[str, int]

    files_expected: Set[str] = field(
        default_factory=lambda: {"STORE", "USER", "TRANSACTION", "TRANSACTION_ITEM", "MENU_ITEM"}
    )
    files_eof_received: Set[str] = field(default_factory=set)

    queries_expected: Set[str] = field(default_factory=set)
    query_eofs_count: int = 0

    results: list = field(default_factory=list)  # [(query_id, body), ...] - kept for future resilience

    # State coordination for race condition fix
    state: SessionState = SessionState.UPLOADING

    # Buffered results (only used during UPLOADING state)
    # Format: List[(query_id: str, result_body: bytes)]
    buffered_results: List[Tuple[str, bytes]] = field(default_factory=list)

    # Track which queries have buffered results (need EOF when flushing)
    queries_with_buffered_results: Set[str] = field(default_factory=set)

    def all_file_eofs_received(self) -> bool:
        return self.files_eof_received == self.files_expected

    def all_query_eofs_received(self) -> bool:
        return self.query_eofs_count >= len(self.queries_expected)


class SessionManager:
    """
    Manages multiple concurrent client sessions.
    Thread-safe for concurrent access.
    """

    def __init__(self, enabled_queries: list[str]):
        self.sessions: Dict[UUID, SessionData] = {}
        self.enabled_queries = {q.upper() for q in enabled_queries}
        self._lock = threading.Lock()

    def create_session(self, client_socket: socket, client_address) -> UUID:
        """Create a new session and return its UUID."""
        session_id = uuid4()

        with self._lock:
            session_info = SessionData(
                id=session_id,
                socket=client_socket,
                client_address=client_address,
                queries_expected=self.enabled_queries.copy(),
            )
            self.sessions[session_id] = session_info

        logging.info(f"action: session_created | session_id: {session_id} | " f"client: {client_address}")
        return session_id

    def get_session(self, session_id: UUID) -> Optional[SessionData]:
        """Get session info by ID."""
        with self._lock:
            return self.sessions.get(session_id)

    def track_eof_received(self, session_id: UUID, entity_type: str):
        """Track that EOF has been received for an entity type."""
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                session.files_eof_received.add(entity_type)
                logging.debug(
                    f"action: eof_tracked | session_id: {session_id} | "
                    f"entity: {entity_type} | "
                    f"progress: {len(session.files_eof_received)}/{len(session.files_expected)}"
                )

    def transition_to_ready_for_results(self, session_id: UUID) -> Tuple[List[Tuple[str, bytes]], Set[str]]:
        """
        Transition from UPLOADING to READY_FOR_RESULTS.
        Returns buffered results and queries needing EOF.
        Thread-safe.

        Returns:
            (buffered_results, queries_needing_eof)
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if not session or session.state != SessionState.UPLOADING:
                return ([], set())

            session.state = SessionState.READY_FOR_RESULTS
            buffered = session.buffered_results.copy()
            queries_needing_eof = session.queries_with_buffered_results.copy()

            session.buffered_results.clear()
            session.queries_with_buffered_results.clear()

            logging.info(
                f"action: state_transition | from: UPLOADING | to: READY_FOR_RESULTS | "
                f"session_id: {session_id} | buffered: {len(buffered)} | "
                f"queries_needing_eof: {queries_needing_eof}"
            )
            return (buffered, queries_needing_eof)

    def add_result(self, session_id: UUID, query_id: str, result_body: bytes) -> bool:
        """
        Add result - buffers if UPLOADING, returns False if should send directly.

        Returns:
            True = buffered (caller should NOT send, including EOF)
            False = should send directly (state is READY_FOR_RESULTS, caller sends result + EOF)
        """
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                return False

            # Always store for resilience
            session.results.append((query_id, result_body))

            if session.state == SessionState.UPLOADING:
                session.buffered_results.append((query_id, result_body))
                session.queries_with_buffered_results.add(query_id)  # Track for EOF
                logging.debug(
                    f"action: query_result_buffered | session_id: {session_id} | "
                    f"query: {query_id} | size: {len(result_body)} bytes"
                )
                return True  # Buffered, do NOT send (including EOF)
            elif session.state == SessionState.READY_FOR_RESULTS:
                logging.info(
                    f"action: query_result | session_id: {session_id} | "
                    f"query: {query_id} | size: {len(result_body)} bytes"
                )
                return False  # Send directly (including EOF)
            else:
                # COMPLETED state, discard
                return True

    def increment_query_eof(self, session_id: UUID):
        """Increment EOF counter for session."""
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                session.query_eofs_count += 1
                logging.info(
                    f"action: query_eof_received | session_id: {session_id} | "
                    f"progress: {session.query_eofs_count}/{len(session.queries_expected)}"
                )

    def get_session_state(self, session_id: UUID) -> Optional[SessionState]:
        """Get current session state (thread-safe)."""
        with self._lock:
            session = self.sessions.get(session_id)
            return session.state if session else None

    def is_session_complete(self, session_id: UUID) -> bool:
        """
        Session complete only if:
        1. State is READY_FOR_RESULTS (upload finished)
        2. All query EOFs received
        """
        session = self.get_session(session_id)
        if not session:
            return False

        return session.state == SessionState.READY_FOR_RESULTS and session.all_query_eofs_received()

    def close_session(self, session_id: UUID):
        """Close and cleanup session."""
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                try:
                    session.socket.close()
                except Exception as e:
                    logging.debug(f"action: socket_close | error: {e}")

                del self.sessions[session_id]

                logging.info(
                    f"action: session_closed | session_id: {session_id} | " f"client: {session.client_address}"
                )

    def get_active_session_count(self) -> int:
        """Get number of active sessions."""
        with self._lock:
            return len(self.sessions)

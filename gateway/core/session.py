import logging
import threading
from dataclasses import dataclass, field
from socket import socket
from typing import Dict, Optional, Set, Tuple
from uuid import UUID, uuid4


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

    def add_result(self, session_id: UUID, query_id: str, result_body: bytes):
        """Add a query result for a session (stored for future resilience)."""
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                session.results.append((query_id, result_body))
                logging.info(
                    f"action: query_result | session_id: {session_id} | "
                    f"query: {query_id} | "
                    f"size: {len(result_body)} bytes"
                )

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

    def is_session_complete(self, session_id: UUID) -> bool:
        """Check if session has received all query EOFs."""
        session = self.get_session(session_id)
        if not session:
            return False
        return session.all_query_eofs_received()

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

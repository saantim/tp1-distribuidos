import logging
import uuid
from typing import Callable

from worker.session.storage import Session, SessionStorage


class SessionManager:
    """
    Manages session lifecycle for a worker stage.

    Creates sessions on demand, invokes start/end callbacks, determines when
    sessions can be flushed based on EOF markers, and delegates persistence
    to a SessionStorage backend.

    Leader mode: session flushes after receiving EOF from all instances.
    Follower mode: session flushes after receiving a single EOF.
    """

    def __init__(
        self,
        stage_name: str,
        on_start_of_session: Callable,
        on_end_of_session: Callable,
        instances: int,
        is_leader: bool,
        session_storage: SessionStorage,
    ):
        """
        Args:
            stage_name: Stage name for logging.
            on_start_of_session: Callback when session is created.
            on_end_of_session: Callback when session is flushed.
            instances: Number of upstream instances (used for EOF counting).
            is_leader: If True, wait for EOF from all instances. If False, flush after one EOF.
            session_storage: Storage backend for persisting sessions.
        """
        self._sessions: dict[uuid.UUID, Session] = {}
        self._session_storage: SessionStorage = session_storage
        self._on_start_of_session = on_start_of_session
        self._on_end_of_session = on_end_of_session
        self._stage_name = stage_name
        self._instances = instances
        self._is_leader = is_leader
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging and suppress verbose pika output."""
        logging.basicConfig(
            level=logging.INFO,
            format=f"{self.__class__.__name__} - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        """
        Get session by ID, creating it if it doesn't exist.

        Invokes on_start_of_session callback for new sessions.
        """
        if session_id not in self._sessions:
            self._sessions[session_id] = self._session_storage.create_session(session_id)
            self._on_start_of_session(self._sessions[session_id])
            if self._is_leader:
                logging.info(f"action: create_session | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    def get_sessions(self) -> dict[uuid.UUID, Session]:
        return self._sessions

    def try_to_flush(self, session: Session) -> bool:
        """
        Flush session if it has collected enough EOFs.

        Invokes on_end_of_session callback and removes from registry.

        Returns:
            True if session was flushed, False otherwise.
        """
        if self._is_flushable(session):
            self._on_end_of_session(session)
            self._sessions.pop(session.session_id, None)
            # TODO: Aca habria que remover la session del SessionStorage. Problema de atomicidad?
            return True
        return False

    def _is_flushable(self, session: Session) -> bool:
        """
        Check if session has enough EOFs to flush.

        Leader mode: needs EOF from all instances.
        Follower mode: needs one EOF.
        """
        return len(session.get_eof_collected()) >= (self._instances if self._is_leader else 1)

    def save_sessions(self) -> None:
        """Save all active sessions to storage (used during shutdown)."""
        for session in self._sessions.values():
            self._session_storage.save_session(session)

    def save_session(self, session: Session) -> None:
        """Save a single session to storage."""
        self._session_storage.save_session(session)

    def load_sessions(self) -> None:
        """Load all persisted sessions from storage into memory (used during startup)."""
        sessions = self._session_storage.load_sessions()

        for session in sessions:
            self._sessions[session.session_id] = session

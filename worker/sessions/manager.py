import logging
import uuid
from typing import Callable

from worker.sessions.session import Session
from worker.sessions.storage import SessionStorage


class SessionManager:
    """
    Manages session lifecycle, including creation, flushing (EOF handling), and persistence.
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
        Initialize SessionManager.

        Args:
            stage_name: Name of the worker stage.
            on_start_of_session: Callback for session creation.
            on_end_of_session: Callback for session completion (flush).
            instances: Total expected worker instances (for EOF counting).
            is_leader: If True, waits for all instances to send EOF. If False, one EOF is enough.
            session_storage: Backend for session persistence.
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
        """Configure logging format and suppress noisy pika logs."""
        logging.basicConfig(
            level=logging.INFO,
            format=f"{self.__class__.__name__} - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        """Get existing session or create a new one and trigger start callback."""
        if session_id not in self._sessions:
            self._sessions[session_id] = Session(session_id=session_id)
            self._on_start_of_session(self._sessions[session_id])
            if self._is_leader:
                logging.info(f"action: create_session | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    def try_to_flush(self, session: Session) -> bool:
        """
        Check if session is complete (enough EOFs). If so, flush, delete storage, and cleanup.
        Returns True if flushed.
        """
        if self._is_flushable(session):
            self._on_end_of_session(session)
            self._sessions.pop(session.session_id, None)
            self._session_storage.delete_session(session.session_id.hex)
            return True
        return False

    def _is_flushable(self, session: Session) -> bool:
        """Check if session has received required number of EOFs based on leader/follower mode."""
        return len(session.get_eof_collected()) >= (self._instances if self._is_leader else 1)

    def save_sessions(self) -> None:
        """Persist all active sessions to storage."""
        for session in self._sessions.values():
            self._session_storage.save_session(session)

    def save_session(self, session: Session) -> None:
        """Persist a single session to storage."""
        self._session_storage.save_session(session)

    def load_sessions(self) -> None:
        """Load all sessions from storage into memory."""
        sessions = self._session_storage.load_sessions()

        for session in sessions:
            self._sessions[session.session_id] = session

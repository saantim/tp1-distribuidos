import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from worker.session.session import Session


class SessionStorage(ABC):
    """
    Base class for session persistence backends.

    Handles saving/loading sessions to disk. Creates save directory and
    temp directory for atomic writes.
    """

    def __init__(self, save_dir: str = "./sessions/saves"):
        """
        Initialize storage with a save directory.

        Creates save_dir and tmp subdirectory if they don't exist.

        Raises:
            NotADirectoryError: If save_dir exists but is not a directory.
        """
        self._save_dir: Path = Path(save_dir)
        self._temporal_save_dir: Path = self._save_dir / "tmp"

        if self._save_dir.exists() and not self._save_dir.is_dir():
            raise NotADirectoryError(f"El path de sesiones no es un directorio: {self._save_dir}")

        self._save_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
        self._temporal_save_dir.mkdir(parents=True, exist_ok=True, mode=0o755)

    @abstractmethod
    def save_session(self, session: Session) -> Path:
        """
        Persist session to storage.

        Returns:
            Path where session was saved.
        """
        ...

    @abstractmethod
    def load_session(self, session_id: str) -> Session:
        """
        Load a single session by ID.

        Args:
            session_id: Hex UUID string.

        Raises:
            FileNotFoundError: If session doesn't exist.
        """
        ...

    @abstractmethod
    def load_sessions(self) -> List[Session]:
        """Load all sessions from storage."""
        ...

    def create_session(self, session_id: uuid.UUID) -> Session:
        """Create a new empty session."""
        return Session(session_id=session_id)

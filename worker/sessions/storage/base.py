from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from worker.sessions.session import Session


class SessionStorage(ABC):
    """
    Abstract base class for session persistence.
    Handles directory setup and defines the interface for saving/loading sessions.
    """

    def __init__(self, save_dir: str = "./sessions/saves"):
        """
        Initialize storage directory.
        Creates 'save_dir' and a 'tmp' subdirectory for atomic writes.
        """
        self._save_dir: Path = Path(save_dir)
        self._temporal_save_dir: Path = self._save_dir / "tmp"

        if self._save_dir.exists() and not self._save_dir.is_dir():
            raise NotADirectoryError(f"El path de sesiones no es un directorio: {self._save_dir}")

        self._save_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
        self._temporal_save_dir.mkdir(parents=True, exist_ok=True, mode=0o755)

    @abstractmethod
    def save_session(self, session: Session) -> Path:
        """Persist the session state."""
        ...

    @abstractmethod
    def load_session(self, session_id: str) -> Session:
        """Load a single session by ID."""
        ...

    @abstractmethod
    def load_sessions(self) -> List[Session]:
        """Load all available sessions."""
        ...

    @abstractmethod
    def delete_session(self, session_id: str) -> None:
        """Delete persisted data for a session."""
        ...

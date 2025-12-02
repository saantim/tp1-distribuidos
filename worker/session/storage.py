import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from worker.session.session import Session


class SessionStorage(ABC):
    """
    Abstract base class for session persistence backends.

    Concrete implementations are responsible for:
    - Persisting individual sessions.
    - Loading individual sessions by their identifier.
    - Loading all persisted sessions from a given storage location.

    This base class also takes care of preparing the directory structure
    used by file-based implementations, including a separate temporary
    directory for atomic writes.
    """

    def __init__(self, save_dir: str = "./sessions/saves"):
        """
        Initialize the storage with a base directory for session files.

        The constructor ensures that the ``save_dir`` exists and is a
        directory, and creates a ``tmp`` subdirectory that can be used
        for temporary files when performing atomic writes.

        Args:
            save_dir: Base directory where session data will be stored.
                Implementations are free to choose their own layout
                under this directory.

        Raises:
            NotADirectoryError: If ``save_dir`` exists and is not a
                directory.
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
        Persist the given session and return the path where it was saved.

        Implementations may choose how to serialize the session and how
        to organize files on disk or in any other backing store.

        Args:
            session: Session instance whose state should be persisted.

        Returns:
            Path pointing to the final location of the persisted session
            representation.
        """
        ...

    @abstractmethod
    def load_session(self, session_id: str) -> Session:
        """
        Load a single session by its identifier.

        Args:
            session_id: String representation of the session identifier
                used by the storage backend (typically the UUID in hex
                form without dashes).

        Returns:
            The reconstructed Session instance.

        Raises:
            FileNotFoundError: If there is no persisted session with the
                given identifier.
            Exception: Any error that occurs while reading or parsing
                the persisted data.
        """
        ...

    @abstractmethod
    def load_sessions(self) -> List[Session]:
        """
        Load all sessions available in the underlying storage.

        Implementations should discover and reconstruct every session
        that can be found in their backing store and return them as a
        list. The exact discovery mechanism depends on the concrete
        storage layout (e.g. one file per session, multiple deltas per
        session, etc.).

        Returns:
            A list of reconstructed Session instances.
        """
        ...

    def create_session(self, session_id: uuid.UUID) -> Session:
        """
        Create a new session instance.

        Args:
            session_id: Unique identifier for the new session.

        Returns:
            A new Session instance.
        """
        return Session(session_id=session_id)

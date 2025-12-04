import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from worker.session import Session


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


class SnapshotFileSessionStorage(SessionStorage):
    """
    Session storage implementation that writes full JSON snapshots.

    Each session is persisted as a single JSON file named after the
    session UUID in hex form, e.g::

        <session_id_hex>.json

    On each save, the entire session object is serialized and written
    to disk using an atomic write pattern (temporary file + fsync +
    os.replace).
    """

    def save_session(self, session: Session) -> Path:
        """
        Serialize and persist a complete snapshot of the given session.

        The session is serialized using ``model_dump(mode="json")`` and
        saved as a pretty-printed JSON file named
        ``<session_id_hex>.json`` in the base directory. The write is
        performed atomically by writing to a temporary file under
        ``tmp`` and then replacing the final file.

        Args:
            session: Session instance whose state should be persisted.

        Returns:
            Path to the final JSON file containing the session snapshot.

        Raises:
            Exception: Any error encountered while writing the file.
        """
        data = session.model_dump(mode="json")

        serialized = json.dumps(data)
        final_path = self._save_dir / f"{session.session_id.hex}.json"
        tmp_path = self._temporal_save_dir / f"{session.session_id.hex}.json"

        try:
            with open(tmp_path, "w") as f:
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, final_path)
            return final_path
        except Exception as e:
            logging.exception(
                f"[Session] Error saving session {session.session_id}. " f"Temp file kept at: {tmp_path} - Error: {e}"
            )
            raise

    def load_session(self, session_id: str) -> Session:
        """
        Load a session snapshot from its JSON file.

        This method expects a file named ``<session_id>.json`` in the
        storage directory, where ``session_id`` is the hex string of the
        UUID. The JSON payload is parsed and validated using
        ``Session.model_validate``.

        Args:
            session_id: Hex string of the session UUID used as the file
                stem (without extension).

        Returns:
            The reconstructed Session instance.

        Raises:
            FileNotFoundError: If the session file does not exist.
            Exception: Any error encountered while reading or parsing
                the JSON content.
        """
        session_file = self._save_dir / f"{session_id}.json"

        if session_file.exists():
            try:
                with open(session_file, "r") as f:
                    data = json.load(f)
                return Session.model_validate(data)
            except Exception as e:
                logging.exception(f"[Session] Error loading session {session_id}: {e}")
                raise e

        raise FileNotFoundError()

    def load_sessions(self) -> List[Session]:
        """
        Load all session snapshots from the storage directory.

        This method scans the base directory for ``*.json`` files,
        assumes each file represents a full session snapshot, and uses
        `load_session` to reconstruct them.

        Returns:
            A list of Session instances recovered from disk.
        """
        sessions = []

        for session_file in self._save_dir.glob("*.json"):
            session_id_str = session_file.stem
            sessions.append(self.load_session(session_id_str))

        logging.info(f"[SnapshotFileSessionStorage] {len(sessions)} Sessions recovered from: {self._save_dir}")

        return sessions

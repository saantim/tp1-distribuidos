import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Type, TypeVar

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class Session(BaseModel):
    """
    Represent the lifecycle and bookkeeping data of a worker session.

    A session is identified by a UUID and keeps track of:
    - Which workers have already sent an EOF marker.
    - Which message IDs have been processed, allowing duplicate detection.
    - An arbitrary typed storage payload, serialized as a JSON-compatible dict.

    The `storage` field is intended to hold a single logical payload at a time,
    which can be read and written using `set_storage` / `get_storage`.
    """

    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: Optional[Any] = None

    def get_storage(self, data_type: Type[T]) -> T:
        """
        Deserialize the internal storage payload into a typed Pydantic model.

        The method assumes that ``self.storage`` contains a JSON-compatible
        dictionary previously produced by :meth:`set_storage`. It uses the
        provided Pydantic model class (a subclass of ``BaseModel``) to
        validate and instantiate the typed payload.

        Args:
            data_type: Pydantic model class (subclass of ``BaseModel``) that
                will be used to validate and construct the storage object.

        Returns:
            An instance of the given ``data_type`` built from the current
            ``storage`` dictionary.

        Raises:
            pydantic.ValidationError: If the stored data is not compatible with
                the given ``data_type`` schema.
        """
        raw = self.storage

        if isinstance(raw, data_type):
            return raw

        if raw is None:
            obj = data_type()

        elif isinstance(raw, dict):
            obj = data_type.model_validate(raw)

        else:
            obj = data_type.model_validate(raw)

        self.storage = obj
        return obj

    def set_storage(self, storage: BaseModel) -> None:
        """
        Replace the current storage payload with the given Pydantic model.

        The provided model is serialized using ``model_dump(mode="json")`` so
        that the internal representation is JSON-compatible and safe to persist
        or send over the wire.

        Args:
            storage: Pydantic model instance representing the new storage
                payload for this session.
        """
        self.storage = storage

    def add_eof(self, worker_id: str) -> None:
        """
        Mark that the given worker has sent its EOF for this session.

        The worker ID is added to the ``eof_collected`` set, allowing the
        caller to know which workers already signaled the end of their stream.

        Args:
            worker_id: Identifier of the worker that has produced EOF.
        """
        self.eof_collected.add(worker_id)

    def get_eof_collected(self) -> set[str]:
        """
        Return the set of workers that have already reported EOF.

        Returns:
            A set of worker IDs that have previously been registered via
            :meth:`add_eof`.
        """
        return self.eof_collected

    def add_msg_received(self, msg_id: str) -> None:
        """
        Register that a message with the given ID has been processed.

        The message ID is added to the ``msgs_received`` set so that subsequent
        calls to :meth:`is_duplicated_msg` can be used to detect duplicates.

        Args:
            msg_id: Unique identifier of the message that has just been
                processed.
        """
        self.msgs_received.add(msg_id)

    def is_duplicated_msg(self, msg_id: str) -> bool:
        """
        Check whether a message ID has already been processed in this session.

        Args:
            msg_id: Identifier of the message to check.

        Returns:
            True if the given message ID is already present in
            ``msgs_received`` (i.e. it has been processed before),
            False otherwise.
        """
        return msg_id in self.msgs_received


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

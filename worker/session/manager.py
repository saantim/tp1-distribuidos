import logging
import uuid
from typing import Callable

from worker.session import Session
from worker.storage import SessionStorage


class SessionManager:
    """
    Manage the lifecycle and in-memory registry of Session objects for a stage.

    The SessionManager is responsible for:
    - Creating sessions on demand when a new ``session_id`` is seen.
    - Invoking lifecycle callbacks on session start and end.
    - Deciding when a session is eligible to be flushed (based on EOF markers).
    - Delegating persistence to a concrete ``SessionStorage`` implementation.

    The manager can operate in two modes:

    * Leader mode (``is_leader=True``): a session is considered flushable only
      after EOF has been collected from all configured instances.
    * Follower mode (``is_leader=False``): a session is flushable as soon as
      a single EOF is collected.
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
        Initialize a new SessionManager instance.

        Args:
            stage_name: Human-readable name of the processing stage that owns
                this manager. Used in logs to identify where events originate.
            on_start_of_session: Callback invoked whenever a new Session object
                is created. Typically used to perform initialization or side
                effects (metrics, logging, etc.).
            on_end_of_session: Callback invoked when a session becomes
                flushable and is about to be removed from the in-memory
                registry. Typically used to perform cleanup or final side
                effects.
            instances: Total number of worker instances that are expected to
                participate in a session when running in leader mode. This
                value is used to compute the EOF threshold.
            is_leader: If True, this manager enforces that EOF must be
                collected from all ``instances`` before a session is
                considered flushable. If False, a single EOF is sufficient.
            session_storage: Concrete SessionStorage backend responsible for
                persisting and loading session state.
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
        """
        Configure logging for the SessionManager and its dependencies.

        This method sets a basic logging configuration for the process,
        including a standard format that prefixes log entries with the class
        name. It also reduces the verbosity of the ``pika`` logger to WARNING
        to avoid noisy output from the messaging layer.
        """
        logging.basicConfig(
            level=logging.INFO,
            format=f"{self.__class__.__name__} - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        """
        Retrieve an existing session or create a new one if it does not exist.

        If the given ``session_id`` is not present in the internal registry,
        this method will:
        - Instantiate a new Session.
        - Store it in ``_sessions``.
        - Invoke the ``on_start_of_session`` callback.

        Args:
            session_id: Unique identifier of the session to retrieve or create.

        Returns:
            The Session instance associated with the given ``session_id``.
        """
        if session_id not in self._sessions:
            self._sessions[session_id] = Session(session_id=session_id)
            self._on_start_of_session(self._sessions[session_id])
            if self._is_leader:
                logging.info(f"action: create_session | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    def try_to_flush(self, session: Session) -> bool:
        """
        Attempt to flush a session if it meets the flushability criteria.

        A session is considered flushable when :meth:`_is_flushable` returns
        True. When that happens, this method will:

        - Invoke the ``on_end_of_session`` callback.
        - Remove the session from the in-memory registry.

        Args:
            session: The Session instance to evaluate and potentially flush.

        Returns:
            True if the session was flushable and has been removed from the
            registry, False otherwise.
        """
        if self._is_flushable(session):
            self._on_end_of_session(session)
            self._sessions.pop(session.session_id, None)
            # TODO: Aca habria que remover la session del SessionStorage. Problema de atomicidad?
            return True
        return False

    def _is_flushable(self, session: Session) -> bool:
        """
        Determine whether a session has collected enough EOF markers.

        In leader mode, a session is flushable only when the number of EOF
        markers is greater than or equal to ``instances``. In follower mode,
        a single EOF marker is sufficient.

        Args:
            session: The Session instance to inspect.

        Returns:
            True if the session meets the EOF threshold for the current mode
            (leader or follower), False otherwise.
        """
        return len(session.get_eof_collected()) >= (self._instances if self._is_leader else 1)

    def save_sessions(self) -> None:
        """
        Persist all active sessions using the configured SessionStorage.

        This method iterates over the in-memory registry and delegates the
        persistence of each Session to the underlying storage backend. It is
        typically called during graceful shutdown or periodic checkpoints.
        """
        for session in self._sessions.values():
            self._session_storage.save_session(session)

    def save_session(self, session: Session) -> None:
        """
        Persist a single session using the configured SessionStorage.

        This is a convenience wrapper around ``SessionStorage.save_session``
        that allows callers to explicitly checkpoint an individual Session
        after significant state changes.

        Args:
            session: The Session instance to persist.
        """
        self._session_storage.save_session(session)

    def load_sessions(self) -> None:
        """
        Load previously persisted sessions into the in-memory registry.

        The method asks the configured SessionStorage to load all stored
        sessions and repopulates the internal ``_sessions`` mapping. Existing
        entries with the same ``session_id`` will be overwritten.

        This is typically used during startup or recovery to resume processing
        from previously saved state.
        """
        sessions = self._session_storage.load_sessions()

        for session in sessions:
            self._sessions[session.session_id] = session

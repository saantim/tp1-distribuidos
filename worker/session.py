import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Callable, Optional, Type, TypeVar, Union

from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class Session(BaseModel):
    session_id: uuid.UUID
    eof_collected: set[str] = set()
    msgs_received: set[str] = set()
    storage: Optional[Any] = None

    def get_storage(self, data_type: Type[T]) -> T:
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

    def set_storage(self, storage: BaseModel):
        self.storage = storage

    def add_eof(self, worker_id: str):
        self.eof_collected.add(worker_id)

    def get_eof_collected(self):
        return self.eof_collected

    def add_msg_received(self, msg_id: str):
        self.msgs_received.add(msg_id)

    def is_duplicated_msg(self, msg_id: str):
        return msg_id in self.msgs_received

    def save(self, save_dir: str = "./sessions/saves") -> None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        tmp_dir = save_dir / "tmp"
        tmp_dir.mkdir(exist_ok=True, mode=0o755)

        data = self.model_dump(mode="json")

        serialized = json.dumps(data, indent=2)
        session_file = save_dir / f"{self.session_id}.json"
        tmp_path = tmp_dir / f"{self.session_id}.json"

        try:
            with open(tmp_path, "w") as f:
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, session_file)

        except Exception as e:
            logging.error(
                f"[Session] Error saving session {self.session_id}. " f"Temp file kept at: {tmp_path} - Error: {e}"
            )
            raise

    @classmethod
    def load(cls, session_id: Union[str, uuid.UUID], save_dir: Union[str, Path]) -> Optional["Session"]:
        save_dir = Path(save_dir)
        session_id = str(session_id)
        session_file = save_dir / f"{session_id}.json"

        if session_file.exists():
            try:
                with open(session_file, "r") as f:
                    data = json.load(f)
                if "eof_collected" in data:
                    data["eof_collected"] = set(data["eof_collected"])
                if "msgs_received" in data:
                    data["msgs_received"] = set(data["msgs_received"])
                return cls(**data)
            except Exception as e:
                logging.error(f"[Session] Error loading session {session_id}: {e}")
                return None
        return None


class SessionManager:
    def __init__(
        self,
        stage_name: str,
        on_start_of_session: Callable,
        on_end_of_session: Callable,
        instances: int,
        is_leader: bool,
    ):
        self._sessions: dict[uuid.UUID, Session] = {}
        self._on_start_of_session = on_start_of_session
        self._on_end_of_session = on_end_of_session
        self._stage_name = stage_name
        self._instances = instances
        self._is_leader = is_leader
        self._setup_logging()

    def _setup_logging(self):
        """Configura el logging para la clase."""
        logging.basicConfig(
            level=logging.INFO,
            format=f"{self.__class__.__name__} - %(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("pika").setLevel(logging.WARNING)

    def get_or_initialize(self, session_id: uuid.UUID) -> Session:
        if session_id not in self._sessions:
            self._sessions[session_id] = Session(session_id=session_id)
            self._on_start_of_session(self._sessions[session_id])
            if self._is_leader:
                logging.info(f"action: create_session | stage: {self._stage_name} | session: {session_id.hex[:8]}")
        current_session = self._sessions.get(session_id, None)
        return current_session

    def try_to_flush(self, session: Session) -> bool:
        if self._is_flushable(session):
            self._on_end_of_session(session)
            self._sessions.pop(session.session_id, None)
            return True
        return False

    def _is_flushable(self, session: Session) -> bool:
        return len(session.get_eof_collected()) >= (self._instances if self._is_leader else 1)

    def save_sessions(self, path: Union[str, Path] = "./sessions/saves") -> None:
        for session in self._sessions.values():
            session.save(path)

    def save_session(self, session: Session, path: Union[str, Path] = "./sessions/saves") -> None:
        session = self._sessions.get(session.session_id, None)
        if session:
            session.save(path)

    def load_sessions(self, path: Union[str, Path] = "./sessions/saves") -> None:
        path = Path(path)
        if not path.exists():
            logging.info(f"[SessionManager] No sessions directory found at: {path}")
            return
        if not path.is_dir():
            raise NotADirectoryError(f"El path de sesiones no es un directorio: {path}")

        for session_file in path.glob("*.json"):
            session_id_str = session_file.stem
            try:
                session = Session.load(session_id_str, path)
                if session:
                    self._sessions[session.session_id] = session
            except Exception as e:
                logging.debug(f"[SessionManager] Ignorando sesión inválida {session_file}: {e}")

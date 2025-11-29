import json
import logging
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import List

from deepdiff import DeepDiff, Delta
from deepdiff.serialization import json_dumps, json_loads

from worker.session import Session


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


class SnapshotFileSessionStorage(SessionStorage):
    """
    Persists sessions as full JSON snapshots.
    File format: <session_id>.json
    """

    def save_session(self, session: Session) -> Path:
        """
        Save full session state as a JSON file.
        Uses atomic write (write to tmp -> rename).
        """
        data = session.model_dump(mode="json")

        serialized = json.dumps(data, indent=2)
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
        """Load session from its JSON snapshot file."""
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
        """Load all .json session files."""
        sessions = []

        for session_file in self._save_dir.glob("*.json"):
            session_id_str = session_file.stem
            sessions.append(self.load_session(session_id_str))

        logging.info(f"[SnapshotFileSessionStorage] {len(sessions)} Sessions recovered from: {self._save_dir}")

        return sessions

    def delete_session(self, session_id: str) -> None:
        session_file = self._save_dir / f"{session_id}.json"
        if session_file.exists():
            session_file.unlink()
            logging.info(f"[SnapshotFileSessionStorage] Deleted session {session_id}")


class DeltaFileSessionStorage(SessionStorage):
    """
    Persists sessions as incremental JSON deltas (Append-Only File).
    File format: <session_id>.jsonl
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_session_snapshot: defaultdict[str, dict] = defaultdict(dict)

    def save_session(self, session: Session) -> Path:
        """
        Append the diff between current state and last snapshot to the .jsonl file.
        """
        session_id: str = session.session_id.hex

        last_snapshot: dict = self._last_session_snapshot[session_id]
        current_snapshot: dict = session.model_dump(mode="json")

        diff: DeepDiff = DeepDiff(last_snapshot, current_snapshot)
        # We use json_dumps to ensure the delta is serialized to a JSON-compatible string
        delta: Delta = Delta(diff, serializer=json_dumps)

        final_path = self._save_dir / f"{session_id}.jsonl"

        # Create the record to append
        record = {"ts": time.time(), "delta": delta.dumps()}  # This returns the serialized string

        # Append to file
        with open(final_path, "a") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
            os.fsync(f.fileno())

        self._last_session_snapshot[session_id] = current_snapshot

        return final_path

    def load_session(self, session_id: str) -> Session:
        """
        Reconstruct session by replaying all deltas from the .jsonl file.
        """
        session_file = self._save_dir / f"{session_id}.jsonl"

        if not session_file.exists():
            raise FileNotFoundError(f"No snapshot file found for session_id={session_id}")

        session_data: dict = {}

        with open(session_file, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                # The 'delta' field contains the serialized delta string
                delta_str = record["delta"]
                # We need to reconstruct the Delta object.
                # Since we serialized with json_dumps, we deserialize with json_loads
                delta = Delta(delta_str, deserializer=json_loads)
                session_data = session_data + delta

        self._last_session_snapshot[session_id] = session_data

        return Session.model_validate(session_data)

    def load_sessions(self) -> List[Session]:
        """Load all .jsonl session files."""
        sessions = []
        for session_file in self._save_dir.glob("*.jsonl"):
            session_id = session_file.stem
            try:
                sessions.append(self.load_session(session_id))
            except Exception as e:
                logging.error(f"Failed to load session {session_id}: {e}")

        return sessions

    def delete_session(self, session_id: str) -> None:
        """Delete the .jsonl file and clear memory cache."""
        session_file = self._save_dir / f"{session_id}.jsonl"
        if session_file.exists():
            session_file.unlink()
            logging.info(f"[DeltaFileSessionStorage] Deleted session {session_id}")

        if session_id in self._last_session_snapshot:
            del self._last_session_snapshot[session_id]

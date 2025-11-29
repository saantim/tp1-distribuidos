import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import List

from deepdiff import DeepDiff, Delta
from deepdiff.serialization import json_dumps, json_loads

from worker.sessions.session import Session
from worker.sessions.storage.base import SessionStorage


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

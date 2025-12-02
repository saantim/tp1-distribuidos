import json
import logging
import os
from pathlib import Path
from typing import List

from worker.session import Session
from worker.storage import SessionStorage


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
        """
        sessions = []

        for session_file in self._save_dir.glob("*.json"):
            session_id_str = session_file.stem
            sessions.append(self.load_session(session_id_str))

        logging.info(f"[SnapshotFileSessionStorage] {len(sessions)} Sessions recovered from: {self._save_dir}")

        return sessions

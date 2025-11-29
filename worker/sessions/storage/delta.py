import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List

from deepdiff import DeepDiff, Delta
from deepdiff.serialization import json_dumps, json_loads

from worker.sessions.session import Session
from worker.sessions.storage.base import SessionStorage


class DeltaFileSessionStorage(SessionStorage):
    """
    Session storage implementation based on incremental JSON deltas.

    Instead of writing full snapshots on every save, this storage
    computes a :class:`DeepDiff` between the last known session
    snapshot and the current state, and persists the corresponding
    :class:`Delta` to a timestamped JSON file.

    Each save produces a file named::

        <session_id_hex>_<YYYY-MM-DD-HH-MM-SS-ffffff>.json

    To reconstruct a session, all delta files for a given ``session_id``
    are loaded in chronological order and applied on top of an initially
    empty state.
    """

    SPLITER = "_"

    def __init__(self, *args, **kwargs):
        """
        Initialize the delta-based session storage.

        The constructor delegates directory preparation to the base
        :class:`SessionStorage` and initializes an in-memory cache
        mapping each session ID to its last full snapshot (as a dict),
        used as the base state when computing new deltas.
        """
        super().__init__(*args, **kwargs)
        self._last_session_snapshot: defaultdict[str, dict] = defaultdict(dict)

    def save_session(self, session: Session) -> Path:
        """
        Compute and persist an incremental delta for the given session.

        The method compares the current session state against the last
        in-memory snapshot for that ``session_id`` using DeepDiff, then
        wraps the diff in a :class:`Delta` and writes it to a new JSON
        file with a timestamped name under the storage directory. The
        in-memory snapshot is updated to the current state after a
        successful write.

        Args:
            session: Session instance whose changes should be persisted
                as a delta.

        Returns:
            Path to the JSON file containing the newly written delta.
        """
        session_id: str = session.session_id.hex

        last_snapshot: dict = self._last_session_snapshot[session_id]
        current_snapshot: dict = session.model_dump(mode="json")

        diff: DeepDiff = DeepDiff(last_snapshot, current_snapshot)
        delta: Delta = Delta(diff, serializer=json_dumps)

        tmp_path = self._build_snapshot_temporal_filepath(session_id)
        final_path = self._build_snapshot_filepath(session_id)

        with open(tmp_path, "w") as tmp_file:
            delta.dump(tmp_file)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())

        self._last_session_snapshot[session_id] = current_snapshot

        os.replace(tmp_path, final_path)
        return final_path

    def load_session(self, session_id: str) -> Session:
        """
        Reconstruct a session by applying all stored deltas in order.

        This method finds all delta files that belong to the given
        ``session_id``, sorts them by their timestamp (as encoded in the
        filename), and successively applies each :class:`Delta` on top
        of an initially empty dict. The resulting state is then
        validated using ``Session.model_validate``.

        Args:
            session_id: Hex string of the session UUID used as the file
                prefix before the splitter.

        Returns:
            The reconstructed Session instance obtained by replaying all
            deltas for the given session.

        Raises:
            FileNotFoundError: If no delta files are found for the given
                ``session_id``.
            Exception: Any error encountered while reading or applying
                delta files.
        """
        snapshots_files: list[Path] = list(self._save_dir.glob(f"{session_id}{self.SPLITER}*.json"))

        if not snapshots_files:
            raise FileNotFoundError(f"No snapshots found for session_id={session_id}")

        snapshots_files.sort(key=lambda file: file.stem.split(self.SPLITER)[1])

        session_data: dict = {}

        for snapshot_file in snapshots_files:
            with open(snapshot_file, "r") as snapshot:
                delta = Delta(delta_file=snapshot, deserializer=json_loads)
                session_data = session_data + delta

        self._last_session_snapshot[session_id] = session_data

        return Session.model_validate(session_data)

    def load_sessions(self) -> List[Session]:
        """
        Discover and load all sessions represented by delta files.

        The method scans the storage directory for ``*.json`` files,
        infers the session identifier from the portion of the filename
        before the splitter, and then calls :meth:`load_session` for
        each distinct session ID.

        Returns:
            A list of Session instances reconstructed from all available
            deltas on disk.
        """
        session_ids: set[str] = set()

        for snapshot_file in self._save_dir.glob("*.json"):
            # Skip if it doesn't match the splitter pattern (e.g. snapshot files)
            # But wait, Snapshot storage also uses .json.
            # We need to distinguish them?
            # The user code assumes all .json files are deltas if they have the splitter.
            if self.SPLITER in snapshot_file.stem:
                session_id = snapshot_file.stem.split(self.SPLITER)[0]
                session_ids.add(session_id)

        return [self.load_session(session_id) for session_id in session_ids]

    def delete_session(self, session_id: str) -> None:
        """Delete all delta files for the session."""
        for snapshot_file in self._save_dir.glob(f"{session_id}{self.SPLITER}*.json"):
            snapshot_file.unlink()

        if session_id in self._last_session_snapshot:
            del self._last_session_snapshot[session_id]

    def _build_snapshot_filepath(self, session_id: str) -> Path:
        """
        Build the final filesystem path for a new delta file.

        Args:
            session_id: Hex string of the session UUID used as the prefix
                in the filename.

        Returns:
            A Path instance pointing to the location in ``_save_dir``
            where the next delta file should be written.
        """
        filename: str = self._build_snapshot_filename(session_id)
        return self._save_dir / filename

    def _build_snapshot_temporal_filepath(self, session_id: str) -> Path:
        """
        Build the temporary filesystem path for a new delta file.

        The returned path is located under the ``tmp`` subdirectory and
        is intended to be used as the target of the initial write before
        atomically moving it into its final location.

        Args:
            session_id: Hex string of the session UUID used as the prefix
                in the filename.

        Returns:
            A Path instance pointing to the temporary location for the
            next delta file.
        """
        tmp_dir = self._save_dir / "tmp"
        filename: str = self._build_snapshot_filename(session_id)
        return tmp_dir / filename

    def _build_snapshot_filename(self, session_id: str) -> str:
        """
        Build a timestamped filename for a new delta file.

        The filename encodes both the session identifier and the current
        timestamp down to microseconds in the following format::

            <session_id_hex>_<YYYY-MM-DD-HH-MM-SS-ffffff>.json

        Args:
            session_id: Hex string of the session UUID used as the prefix
                in the filename.

        Returns:
            A string representing the new delta file name.
        """
        ts = time.time()
        dt = datetime.fromtimestamp(ts)
        stamp = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")
        return f"{session_id}{self.SPLITER}{stamp}.json"

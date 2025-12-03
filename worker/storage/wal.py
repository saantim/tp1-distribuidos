import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, List, Optional, Type

from pydantic import Field

from worker.session import BaseOp, Session, SessionStorage, SysCommitOp, SysEofOp, SysMsgOp


class WALSession(Session):
    """
    Session subclass with Write-Ahead Log support via reducer pattern.
    """

    pending_ops: list[BaseOp] = Field(default_factory=list, exclude=True)
    reducer: Optional[Callable[[Any, BaseOp], Any]] = Field(default=None, exclude=True)

    def bind_reducer(self, reducer: Callable[[Any, BaseOp], Any]) -> None:
        """
        Bind a reducer function for applying operations to storage.
        """
        self.reducer = reducer

    def apply(self, op: BaseOp) -> None:
        """
        Apply operation to session via reducer and track for persistence.
        """
        if isinstance(op, SysEofOp):
            self.add_eof(op.worker_id)
        elif isinstance(op, SysMsgOp):
            self.add_msg_received(op.msg_id)
        else:
            if self.reducer:
                self.storage = self.reducer(self.storage, op)

        self.pending_ops.append(op)


class WALFileSessionStorage(SessionStorage):
    """
    Session storage implementation based on Write-Ahead Log (WAL) with periodic snapshots.

    Combines fast WAL appends with periodic snapshot compaction:
    - Normal operation: Appends operations to WAL (fast writes)
    - Every N batches: Creates snapshot + truncates WAL (prevents unbounded growth)
    - Recovery: Loads latest snapshot + replays remaining WAL ops

    File structure per session::

        <session_id_hex>.snapshot.json  # Latest full state snapshot (replaced on compaction)
        <session_id_hex>.wal            # Append-only operation log since last snapshot

    The file format for WAL is JSON Lines, where each line represents a single
    operation dictionary.
    """

    def __init__(
        self,
        save_dir: str = "./sessions/saves",
        reducer: Optional[Callable[[Any, BaseOp], Any]] = None,
        op_types: list[Type[BaseOp]] | None = None,
        snapshot_threshold: int = 100,
    ):
        super().__init__(save_dir)
        self._reducer = reducer
        self._snapshot_threshold = snapshot_threshold
        self._batch_counts: dict[str, int] = {}
        if op_types is None:
            op_types = []

        # custom types
        self._op_types_map = {op.get_type(): op for op in op_types}

        # default "system" types
        self._op_types_map[SysEofOp.get_type()] = SysEofOp
        self._op_types_map[SysMsgOp.get_type()] = SysMsgOp
        self._op_types_map[SysCommitOp.get_type()] = SysCommitOp

    def create_session(self, session_id: uuid.UUID) -> Session:
        """Create a new WALSession instance."""
        session = WALSession(session_id=session_id)
        if self._reducer:
            session.bind_reducer(self._reducer)
        return session

    def save_session(self, session: Session) -> Path:
        session_id = session.session_id.hex
        wal_path = self._get_wal_path(session_id)

        if session.pending_ops:
            msg_id = next((op.msg_id for op in session.pending_ops if isinstance(op, SysMsgOp)), None)
            if msg_id is None:
                msg_id = uuid.uuid4().hex
                logging.warning(
                    f"[WAL] No SysMsgOp found in pending_ops | session: {session_id[:8]} | "
                    f"using fallback batch_id: {msg_id}"
                )

            ops_buffer = "".join(op.model_dump_json() + "\n" for op in session.pending_ops)
            commit_marker = SysCommitOp(batch_id=msg_id).model_dump_json() + "\n"
            buffer = ops_buffer + commit_marker

            with open(wal_path, "a") as f:
                f.write(buffer)
                f.flush()
                os.fsync(f.fileno())

            session.pending_ops.clear()

            if session_id not in self._batch_counts:
                self._batch_counts[session_id] = 0
            self._batch_counts[session_id] += 1

            if self._batch_counts[session_id] >= self._snapshot_threshold:
                self._compact_session(session)

        return wal_path

    def load_session(self, session_id: str) -> Session:
        """
        Load session from snapshot + WAL replay.

        Steps:
        1. Load snapshot if exists (full state)
        2. Replay WAL ops on top
        3. Return reconstructed session
        """
        snapshot_path = self._get_snapshot_path(session_id)
        wal_path = self._get_wal_path(session_id)

        if snapshot_path.exists():
            start_time = time.time()
            with open(snapshot_path, "r") as f:
                data = json.load(f)
            session = WALSession.model_validate(data)
            duration = time.time() - start_time
            logging.info(f"metric: snapshot_load_time | duration: {duration:.6f}s | " f"session: {session_id[:8]}")
        else:
            session = WALSession(session_id=uuid.UUID(session_id))

        if self._reducer:
            session.bind_reducer(self._reducer)

        if wal_path.exists():
            start_time = time.time()
            op_count = 0
            skip_count = 0
            batch_buffer = []

            with open(wal_path, "r") as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        raw_op = json.loads(line)
                        op_type_str = raw_op.get("type")
                        op_cls = self._op_types_map.get(op_type_str)

                        if op_cls:
                            op = op_cls.model_validate(raw_op)

                            if isinstance(op, SysCommitOp):
                                for buffered_op in batch_buffer:
                                    session.apply(buffered_op)
                                    op_count += 1
                                batch_buffer.clear()
                            else:
                                batch_buffer.append(op)
                        else:
                            logging.warning(
                                f"[WAL] Unknown op type '{op_type_str}' at line {line_num} | "
                                f"session: {session_id[:8]} | SKIPPING"
                            )
                            skip_count += 1

                    except json.JSONDecodeError as e:
                        logging.error(
                            f"[WAL] Corrupt JSON at line {line_num} | session: {session_id[:8]} | "
                            f"error: {e} | data: {line[:100]} | SKIPPING"
                        )
                        skip_count += 1
                    except Exception as e:
                        logging.error(
                            f"[WAL] Failed to apply op at line {line_num} | session: {session_id[:8]} | "
                            f"error: {e} | SKIPPING"
                        )
                        skip_count += 1

            if batch_buffer:
                batch_ids = [op.msg_id for op in batch_buffer if isinstance(op, SysMsgOp)]
                batch_id = batch_ids[0] if batch_ids else "unknown"
                logging.warning(
                    f"[WAL] Discarding uncommitted batch | session: {session_id[:8]} | "
                    f"batch_id: {batch_id} | ops_discarded: {len(batch_buffer)}"
                )
                batch_buffer.clear()

            if skip_count > 0:
                logging.warning(
                    f"[WAL] Replay completed with errors | session: {session_id[:8]} | "
                    f"ops_applied: {op_count} | ops_skipped: {skip_count}"
                )

            self._batch_counts[session_id] = 0
            duration = time.time() - start_time
            logging.info(
                f"metric: wal_replay_time | duration: {duration:.6f}s | "
                f"ops_replayed: {op_count} | session: {session_id[:8]}"
            )

        session.pending_ops.clear()
        return session

    def load_sessions(self) -> List[Session]:
        """Load all sessions from snapshots + WAL files."""
        sessions = []

        snapshot_files = set(self._save_dir.glob("*.snapshot.json"))
        wal_files = set(self._save_dir.glob("*.wal"))

        session_ids = {f.stem.replace(".snapshot", "") for f in snapshot_files}
        session_ids.update({f.stem for f in wal_files})

        for sid in session_ids:
            try:
                sessions.append(self.load_session(sid))
            except Exception as e:
                logging.error(f"Failed to load session {sid}: {e}")

        logging.info(f"[WALFileSessionStorage] {len(sessions)} Sessions recovered from: {self._save_dir}")
        return sessions

    def delete_session(self, session_id: str) -> None:
        """Delete snapshot and WAL files for session."""
        snapshot_path = self._get_snapshot_path(session_id)
        wal_path = self._get_wal_path(session_id)

        if snapshot_path.exists():
            snapshot_path.unlink()
        if wal_path.exists():
            wal_path.unlink()

        self._batch_counts.pop(session_id, None)

        logging.info(f"[WAL] Deleted session files: {session_id[:8]}")

    def _compact_session(self, session: Session) -> None:
        """
        Create snapshot of current session state and truncate WAL.

        Steps:
        1. Serialize full session to snapshot file (atomic write)
        2. Truncate WAL file (empty it)
        3. Reset batch counter
        """
        session_id = session.session_id.hex
        snapshot_path = self._get_snapshot_path(session_id)
        wal_path = self._get_wal_path(session_id)
        tmp_snapshot_path = self._temporal_save_dir / f"{session_id}.snapshot.json"

        start_time = time.time()

        data = session.model_dump(mode="json")
        serialized = json.dumps(data)

        with open(tmp_snapshot_path, "w") as f:
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_snapshot_path, snapshot_path)

        with open(wal_path, "w") as f:
            f.flush()
            os.fsync(f.fileno())

        self._batch_counts[session_id] = 0

        duration = time.time() - start_time
        logging.info(
            f"metric: wal_compact_time | duration: {duration:.6f}s | "
            f"batches_since_last: {self._snapshot_threshold} | "
            f"session: {session_id[:8]}"
        )

    def _get_wal_path(self, session_id: str) -> Path:
        return self._save_dir / f"{session_id}.wal"

    def _get_snapshot_path(self, session_id: str) -> Path:
        return self._save_dir / f"{session_id}.snapshot.json"

"""
Test that Session.apply() properly records system operations (EOF, message IDs) in WAL.
"""

import json
import tempfile
import uuid
from pathlib import Path

import pytest

from worker.session import SysEofOp, SysMsgOp
from worker.storage.wal import WALFileSessionStorage


def test_session_apply_records_eof_in_wal():
    """Verify that applying SysEofOp writes to WAL file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = WALFileSessionStorage(save_dir=tmpdir)
        session_id = uuid.uuid4()
        session = storage.create_session(session_id)

        # Apply EOF operations
        session.apply(SysEofOp(worker_id="worker_0"))
        session.apply(SysEofOp(worker_id="worker_1"))

        # Save to WAL
        storage.save_session(session)

        # Verify WAL file contains EOF operations
        wal_path = Path(tmpdir) / f"{session_id.hex}.wal"
        assert wal_path.exists(), "WAL file should exist"

        with open(wal_path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 2, f"Expected 2 lines in WAL, got {len(lines)}"

        # Parse and verify operations
        op1 = json.loads(lines[0].strip())
        op2 = json.loads(lines[1].strip())

        assert op1["type"] == "__sys_eof"
        assert op1["worker_id"] == "worker_0"
        assert op2["type"] == "__sys_eof"
        assert op2["worker_id"] == "worker_1"

        # Verify in-memory state
        assert "worker_0" in session.eof_collected
        assert "worker_1" in session.eof_collected


def test_session_apply_records_message_ids_in_wal():
    """Verify that applying SysMsgOp writes to WAL file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = WALFileSessionStorage(save_dir=tmpdir)
        session_id = uuid.uuid4()
        session = storage.create_session(session_id)

        # Apply message ID operations
        session.apply(SysMsgOp(msg_id="msg_abc123"))
        session.apply(SysMsgOp(msg_id="msg_def456"))

        # Save to WAL
        storage.save_session(session)

        # Verify WAL file contains message operations
        wal_path = Path(tmpdir) / f"{session_id.hex}.wal"
        assert wal_path.exists(), "WAL file should exist"

        with open(wal_path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 2, f"Expected 2 lines in WAL, got {len(lines)}"

        # Parse and verify operations
        op1 = json.loads(lines[0].strip())
        op2 = json.loads(lines[1].strip())

        assert op1["type"] == "__sys_msg"
        assert op1["msg_id"] == "msg_abc123"
        assert op2["type"] == "__sys_msg"
        assert op2["msg_id"] == "msg_def456"

        # Verify in-memory state
        assert "msg_abc123" in session.msgs_received
        assert "msg_def456" in session.msgs_received


def test_session_replay_restores_eof_and_messages():
    """Verify that replaying WAL restores both EOF and message tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = WALFileSessionStorage(save_dir=tmpdir)
        session_id = uuid.uuid4()

        # Create session and apply operations
        session1 = storage.create_session(session_id)
        session1.apply(SysEofOp(worker_id="worker_0"))
        session1.apply(SysEofOp(worker_id="worker_1"))
        session1.apply(SysMsgOp(msg_id="msg_001"))
        session1.apply(SysMsgOp(msg_id="msg_002"))
        storage.save_session(session1)

        # Load session from WAL
        session2 = storage.load_session(session_id.hex)

        # Verify all state was restored
        assert session2.session_id == session_id
        assert "worker_0" in session2.eof_collected
        assert "worker_1" in session2.eof_collected
        assert "msg_001" in session2.msgs_received
        assert "msg_002" in session2.msgs_received


def test_crash_recovery_prevents_duplicates():
    """
    Simulate a crash scenario where worker processes messages, crashes before ACK,
    and RabbitMQ redelivers. Verify that message ID tracking prevents duplicates.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = WALFileSessionStorage(save_dir=tmpdir)
        session_id = uuid.uuid4()

        # Worker processes first message
        session = storage.create_session(session_id)
        msg_id = "batch_abc123"
        session.apply(SysMsgOp(msg_id=msg_id))
        storage.save_session(session)

        # Simulate crash: worker restarts and loads session
        recovered_session = storage.load_session(session_id.hex)

        # RabbitMQ redelivers the same message
        # Worker checks if it's a duplicate
        assert recovered_session.is_duplicated_msg(msg_id), "Should detect duplicate after crash recovery"

        # Worker should NOT process the message again
        # This prevents the duplicate aggregation that was causing Q3/Q4 failures


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

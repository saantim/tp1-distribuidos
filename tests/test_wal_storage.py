"""
Comprehensive test suite for WAL (Write-Ahead Log) storage implementation.

Tests cover:
- Basic WAL functionality (append, replay, compaction)
- Corruption handling (malformed JSON, unknown ops)
- Edge cases (empty files, missing snapshots/WAL)
- Delta-based operations (aggregator ops)
- Growth control via snapshots
"""

import uuid
from typing import Optional

import pytest

from shared.entity import ItemId
from tests.base import BaseSessionStorageTest
from worker.aggregator.ops import AggregateItemOp
from worker.storage.wal import BaseOp, SysMsgOp, WALFileSessionStorage, WALSession


class TestWALStorage(BaseSessionStorageTest):
    """Test suite for WAL storage implementation."""

    @pytest.fixture
    def storage_class(self):
        """Override base test fixture to use WAL storage."""
        return WALFileSessionStorage

    @pytest.fixture
    def simple_reducer(self):
        """Simple reducer for testing - counts messages."""

        def reducer(storage: Optional[dict], op: BaseOp) -> dict:
            if storage is None:
                storage = {"msg_count": 0}
            if isinstance(op, SysMsgOp):
                storage["msg_count"] = storage.get("msg_count", 0) + 1
            return storage

        return reducer

    @pytest.fixture
    def storage_with_reducer(self, temp_dir, simple_reducer):
        """Storage instance with bound reducer."""
        return WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

    def test_wal_append_only_growth(self, temp_dir, session_id, simple_reducer):
        """Verify WAL file grows with appends (3 batches → 6 lines: 3 ops + 3 commits)."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        for i in range(3):
            session.apply(SysMsgOp(msg_id=f"msg-{i}"))
            storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        assert wal_path.exists(), "WAL file should exist"

        with open(wal_path) as f:
            lines = f.readlines()
        assert len(lines) == 6, f"Expected 6 lines in WAL (3 ops + 3 commits), got {len(lines)}"

    def test_wal_replay_correctness(self, temp_dir, session_id, simple_reducer):
        """Apply 5 SysMsgOp, load in new instance, verify state correct."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        # Create session, apply ops
        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        for i in range(5):
            session.apply(SysMsgOp(msg_id=f"msg-{i}"))
        storage.save_session(session)

        # Load in new storage instance
        storage2 = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])
        loaded = storage2.load_session(session_id.hex)

        # System ops update session metadata, not storage (clean separation of concerns)
        assert len(loaded.msgs_received) == 5, "Should have tracked 5 message IDs"
        assert all(f"msg-{i}" in loaded.msgs_received for i in range(5))

    def test_custom_operation_types(self, temp_dir, session_id):
        """Register custom IncrementOp, verify replay works."""
        from typing import Literal

        class IncrementOp(BaseOp):
            type: Literal["increment"] = "increment"
            amount: int

        def reducer(state: Optional[int], op: BaseOp) -> int:
            if state is None:
                state = 0
            if isinstance(op, IncrementOp):
                state += op.amount
            return state

        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=reducer, op_types=[IncrementOp])

        session = WALSession(session_id=session_id)
        session.bind_reducer(reducer)
        session.apply(IncrementOp(amount=10))
        session.apply(IncrementOp(amount=5))
        storage.save_session(session)

        # Reload and verify
        loaded = storage.load_session(session_id.hex)
        assert loaded.storage == 15, "10 + 5 should equal 15"

    def test_snapshot_compaction(self, temp_dir, session_id, simple_reducer):
        """After threshold batches, snapshot created, WAL truncated."""
        storage = WALFileSessionStorage(
            save_dir=str(temp_dir),
            reducer=simple_reducer,
            op_types=[],
            snapshot_threshold=3,  # Compact after 3 batches
        )

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        # Simulate 3 batches (should trigger compaction)
        for batch in range(3):
            session.apply(SysMsgOp(msg_id=f"batch-{batch}"))
            storage.save_session(session)

        snapshot_path = temp_dir / f"{session_id.hex}.snapshot.json"
        wal_path = temp_dir / f"{session_id.hex}.wal"

        assert snapshot_path.exists(), "Snapshot should exist after threshold"

        # WAL should be truncated (empty)
        with open(wal_path) as f:
            lines = f.readlines()
        assert len(lines) == 0, "WAL should be truncated after compaction"

    def test_hybrid_snapshot_plus_wal(self, temp_dir, session_id, simple_reducer):
        """Load combines snapshot (5 ops) + WAL delta (2 ops) = 7 total."""
        storage = WALFileSessionStorage(
            save_dir=str(temp_dir),
            reducer=simple_reducer,
            op_types=[],
            snapshot_threshold=5,
        )

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        # Create 5 ops (triggers snapshot)
        for i in range(5):
            session.apply(SysMsgOp(msg_id=f"pre-{i}"))
            storage.save_session(session)

        # Add 2 more ops (in WAL only)
        for i in range(2):
            session.apply(SysMsgOp(msg_id=f"post-{i}"))
            storage.save_session(session)

        # Load should combine snapshot + WAL
        loaded = storage.load_session(session_id.hex)
        assert len(loaded.msgs_received) == 7, "Should have 5 from snapshot + 2 from WAL"

    def test_corrupt_wal_line_skipped(self, temp_dir, session_id, simple_reducer, caplog):
        """Verify corrupt lines are skipped with warning logged."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        session.apply(SysMsgOp(msg_id="msg-1"))
        storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path, "a") as f:
            f.write("{invalid json line}\n")
            f.write('{"type": "__sys_msg", "msg_id": "msg-2"}\n')
            f.write('{"type": "__sys_commit", "batch_id": "msg-2"}\n')

        loaded = storage.load_session(session_id.hex)
        assert "msg-1" in loaded.msgs_received, "Should have msg-1 from committed batch"
        assert "msg-2" in loaded.msgs_received, "Should have msg-2 after skipping corrupt line"
        assert "Corrupt JSON" in caplog.text, "Should log corruption warning"

    def test_empty_wal_and_no_snapshot(self, temp_dir, simple_reducer):
        """Load non-existent session creates empty session."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session_id = uuid.uuid4()
        # Load non-existent session should create empty session
        loaded = storage.load_session(session_id.hex)
        assert loaded.session_id == session_id
        assert loaded.storage is None or loaded.storage == {"msg_count": 0}

    def test_delete_removes_snapshot_and_wal(self, temp_dir, session_id, simple_reducer):
        """Verify delete removes both snapshot and WAL files."""
        storage = WALFileSessionStorage(
            save_dir=str(temp_dir),
            reducer=simple_reducer,
            op_types=[],
            snapshot_threshold=1,
        )

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        session.apply(SysMsgOp(msg_id="msg-1"))
        storage.save_session(session)  # Creates snapshot

        snapshot_path = temp_dir / f"{session_id.hex}.snapshot.json"
        wal_path = temp_dir / f"{session_id.hex}.wal"

        assert snapshot_path.exists(), "Snapshot should exist before delete"

        storage.delete_session(session_id.hex)

        assert not snapshot_path.exists(), "Snapshot should be deleted"
        assert not wal_path.exists(), "WAL should be deleted"

    def test_delta_ops_aggregation(self, temp_dir, session_id):
        """Test AggregateItemOp reducer - apply deltas, verify sums correct."""

        def delta_reducer(state: Optional[dict], op: BaseOp) -> dict:
            if state is None:
                state = {"items": {}, "count": 0}
            if isinstance(op, AggregateItemOp):
                item = state["items"].setdefault(op.item_id, {"qty": 0, "amt": 0.0})
                item["qty"] += op.quantity_delta
                item["amt"] += op.amount_delta
                state["count"] += 1
            return state

        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=delta_reducer, op_types=[AggregateItemOp])

        session = WALSession(session_id=session_id)
        session.bind_reducer(delta_reducer)

        # Apply deltas: item 1 gets 2 ops, item 2 gets 1 op
        session.apply(AggregateItemOp(period="2024-01", item_id=ItemId(1), quantity_delta=5, amount_delta=10.0))
        session.apply(AggregateItemOp(period="2024-01", item_id=ItemId(1), quantity_delta=3, amount_delta=7.5))
        session.apply(AggregateItemOp(period="2024-01", item_id=ItemId(2), quantity_delta=2, amount_delta=4.0))
        storage.save_session(session)

        # Verify sums
        loaded = storage.load_session(session_id.hex)
        assert loaded.storage["items"][1]["qty"] == 8, "5 + 3 should equal 8"
        assert loaded.storage["items"][1]["amt"] == 17.5, "10.0 + 7.5 should equal 17.5"
        assert loaded.storage["items"][2]["qty"] == 2
        assert loaded.storage["count"] == 3, "Should have processed 3 ops"

    def test_wal_growth_controlled_by_snapshots(self, temp_dir, session_id, simple_reducer):
        """30 batches with threshold=10 → WAL has <10 lines."""
        storage = WALFileSessionStorage(
            save_dir=str(temp_dir),
            reducer=simple_reducer,
            op_types=[],
            snapshot_threshold=10,
        )

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        # Simulate 30 batches (should trigger 3 compactions)
        for i in range(30):
            session.apply(SysMsgOp(msg_id=f"msg-{i}"))
            storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"

        # WAL should be small (not 30 lines)
        if wal_path.exists():
            with open(wal_path) as f:
                lines = f.readlines()
            assert len(lines) < 10, f"WAL should be compacted, got {len(lines)} lines (expected <10)"

    def test_unknown_op_type_skipped(self, temp_dir, session_id, simple_reducer, caplog):
        """Verify unknown operation types are skipped with warning."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        session.apply(SysMsgOp(msg_id="msg-1"))
        storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path, "a") as f:
            f.write('{"type": "unknown_op", "data": "test"}\n')
            f.write('{"type": "__sys_msg", "msg_id": "msg-2"}\n')
            f.write('{"type": "__sys_commit", "batch_id": "msg-2"}\n')

        loaded = storage.load_session(session_id.hex)
        assert "msg-1" in loaded.msgs_received
        assert "msg-2" in loaded.msgs_received
        assert "Unknown op type" in caplog.text, "Should log unknown op warning"

    def test_uncommitted_batch_discarded(self, temp_dir, session_id, simple_reducer, caplog):
        """Verify incomplete batch without commit marker is discarded on recovery."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        session.apply(SysMsgOp(msg_id="msg-1"))
        storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path, "a") as f:
            f.write('{"type": "__sys_msg", "msg_id": "msg-2"}\n')
            f.write('{"type": "__sys_msg", "msg_id": "msg-3"}\n')

        loaded = storage.load_session(session_id.hex)
        assert "msg-1" in loaded.msgs_received, "Committed batch should be applied"
        assert "msg-2" not in loaded.msgs_received, "Uncommitted batch should be discarded"
        assert "msg-3" not in loaded.msgs_received, "Uncommitted batch should be discarded"
        assert "Discarding uncommitted batch" in caplog.text

    def test_partial_write_with_message_id_recovery(self, temp_dir, session_id, simple_reducer, caplog):
        """Simulate crash after writing SysMsgOp but before commit marker."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)
        session.apply(SysMsgOp(msg_id="batch-1"))
        storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path, "a") as f:
            f.write('{"type": "__sys_msg", "msg_id": "batch-2"}\n')

        loaded = storage.load_session(session_id.hex)
        assert "batch-1" in loaded.msgs_received, "Committed batch should be applied"
        assert "batch-2" not in loaded.msgs_received, "Message ID without commit should be discarded"
        assert "batch-2" in caplog.text, "Should log the discarded batch ID"

    def test_commit_markers_present_in_wal(self, temp_dir, session_id, simple_reducer):
        """Verify every save appends commit marker."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        for i in range(3):
            session.apply(SysMsgOp(msg_id=f"msg-{i}"))
            storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path) as f:
            lines = f.readlines()

        commit_count = sum(1 for line in lines if '"type":"__sys_commit"' in line or '"type": "__sys_commit"' in line)
        assert commit_count == 3, f"Expected 3 commit markers, found {commit_count}"

    def test_mixed_committed_uncommitted_batches(self, temp_dir, session_id, simple_reducer, caplog):
        """Multiple committed batches followed by incomplete batch."""
        storage = WALFileSessionStorage(save_dir=str(temp_dir), reducer=simple_reducer, op_types=[])

        session = WALSession(session_id=session_id)
        session.bind_reducer(simple_reducer)

        for i in range(3):
            session.apply(SysMsgOp(msg_id=f"committed-{i}"))
            storage.save_session(session)

        wal_path = temp_dir / f"{session_id.hex}.wal"
        with open(wal_path, "a") as f:
            f.write('{"type": "__sys_msg", "msg_id": "uncommitted-1"}\n')
            f.write('{"type": "__sys_msg", "msg_id": "uncommitted-2"}\n')

        loaded = storage.load_session(session_id.hex)
        assert len(loaded.msgs_received) == 3, "Should only have 3 committed batches"
        for i in range(3):
            assert f"committed-{i}" in loaded.msgs_received
        assert "uncommitted-1" not in loaded.msgs_received
        assert "uncommitted-2" not in loaded.msgs_received
        assert "Discarding uncommitted batch" in caplog.text

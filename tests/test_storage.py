import json
import uuid

import pytest
from pydantic import BaseModel

from worker.sessions.session import Session
from worker.sessions.storage.delta import DeltaFileSessionStorage
from worker.sessions.storage.snapshot import SnapshotFileSessionStorage


class TestModel(BaseModel):
    count: int
    items: list[str]


@pytest.fixture
def session():
    s = Session(session_id=uuid.uuid4())
    s.set_storage(TestModel(count=1, items=["a"]))
    s.add_eof("worker1")
    s.add_msg_received("msg1")
    return s


def test_snapshot_storage_lifecycle(tmp_path, session):
    storage = SnapshotFileSessionStorage(save_dir=str(tmp_path))

    # Save
    path = storage.save_session(session)
    assert path.exists()
    assert path.name == f"{session.session_id.hex}.json"

    with open(path) as f:
        data = json.load(f)
        assert data["session_id"] == str(session.session_id)
        assert data["storage"]["count"] == 1
        assert "worker1" in data["eof_collected"]

    # Load
    loaded = storage.load_session(session.session_id.hex)
    assert loaded.session_id == session.session_id
    assert loaded.get_storage(TestModel).count == 1
    assert loaded.get_eof_collected() == {"worker1"}

    # Delete
    storage.delete_session(session.session_id.hex)
    assert not path.exists()


def test_delta_storage_lifecycle(tmp_path, session):
    storage = DeltaFileSessionStorage(save_dir=str(tmp_path))

    # Initial Save
    path = storage.save_session(session)
    assert path.exists()
    assert path.name == f"{session.session_id.hex}.jsonl"

    with open(path) as f:
        lines = f.readlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert "delta" in record

    # Update and Save
    session.set_storage(TestModel(count=2, items=["a", "b"]))
    session.add_eof("worker2")
    storage.save_session(session)

    with open(path) as f:
        lines = f.readlines()
        assert len(lines) == 2

    # Load
    loaded = storage.load_session(session.session_id.hex)
    assert loaded.session_id == session.session_id
    assert loaded.get_storage(TestModel).count == 2
    assert loaded.get_storage(TestModel).items == ["a", "b"]
    assert loaded.get_eof_collected() == {"worker1", "worker2"}

    # Delete
    storage.delete_session(session.session_id.hex)
    assert not path.exists()

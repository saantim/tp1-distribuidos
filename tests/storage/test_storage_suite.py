import json
import os
import uuid
from pathlib import Path
from typing import Generator, Type

import pytest
from pydantic import BaseModel, Field

from worker.sessions.session import Session
from worker.sessions.storage.base import SessionStorage
from worker.sessions.storage.delta import DeltaFileSessionStorage
from worker.sessions.storage.snapshot import SnapshotFileSessionStorage


class ComplexModel(BaseModel):
    id: str
    count: int
    items: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    score: float = 0.0


@pytest.fixture
def temp_dir(tmp_path) -> Generator[Path, None, None]:
    yield tmp_path


@pytest.fixture(params=[SnapshotFileSessionStorage, DeltaFileSessionStorage])
def storage_class(request) -> Type[SessionStorage]:
    return request.param


@pytest.fixture
def storage(storage_class, temp_dir) -> SessionStorage:
    return storage_class(save_dir=str(temp_dir))


@pytest.fixture
def session_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def complex_data() -> ComplexModel:
    return ComplexModel(id="test-1", count=42, items=["apple", "banana"], metadata={"key": "value"}, score=3.14)


def test_save_load_roundtrip(storage: SessionStorage, session_id: uuid.UUID, complex_data: ComplexModel):
    session = Session(session_id=session_id)
    session.set_storage(complex_data)

    storage.save_session(session)

    loaded_session = storage.load_session(session_id.hex)
    loaded_data = loaded_session.get_storage(ComplexModel)

    assert loaded_data == complex_data
    assert loaded_data.items == ["apple", "banana"]
    assert loaded_data.score == 3.14


def test_persistence_across_instances(
    storage_class: Type[SessionStorage], temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel
):
    # Instance A
    storage_a = storage_class(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage_a.save_session(session)

    # Instance B (same dir)
    storage_b = storage_class(save_dir=str(temp_dir))
    loaded_session = storage_b.load_session(session_id.hex)
    loaded_data = loaded_session.get_storage(ComplexModel)

    assert loaded_data == complex_data


def test_load_non_existent(storage: SessionStorage):
    with pytest.raises(FileNotFoundError):
        storage.load_session(uuid.uuid4().hex)


def test_delete_session(storage: SessionStorage, session_id: uuid.UUID, complex_data: ComplexModel):
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    assert storage.load_session(session_id.hex) is not None

    storage.delete_session(session_id.hex)

    with pytest.raises(FileNotFoundError):
        storage.load_session(session_id.hex)


def test_load_all_sessions(storage: SessionStorage, complex_data: ComplexModel):
    ids = [uuid.uuid4() for _ in range(5)]

    for sid in ids:
        session = Session(session_id=sid)
        session.set_storage(complex_data)
        storage.save_session(session)

    loaded_sessions = storage.load_sessions()
    loaded_ids = {s.session_id for s in loaded_sessions}

    assert len(loaded_sessions) == 5
    assert loaded_ids == set(ids)


def test_snapshot_json_format(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    file_path = temp_dir / f"{session_id.hex}.json"
    assert file_path.exists()

    with open(file_path, "r") as f:
        content = json.load(f)

    assert content["session_id"] == session_id.hex
    assert content["storage"]["id"] == complex_data.id


def test_snapshot_atomic_write_crash(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel, monkeypatch):
    """Verify that a crash during write doesn't corrupt the existing file."""
    storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))

    # Initial save
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    # Modify data
    complex_data.count = 999
    session.set_storage(complex_data)

    # Mock os.replace to simulate crash BEFORE the swap
    def mock_replace(src, dst):
        raise OSError("Simulated Crash")

    monkeypatch.setattr(os, "replace", mock_replace)

    with pytest.raises(OSError, match="Simulated Crash"):
        storage.save_session(session)

    # Verify original file is untouched
    loaded_session = storage.load_session(session_id.hex)
    loaded_data = loaded_session.get_storage(ComplexModel)
    assert loaded_data.count == 42


def test_delta_incremental_updates(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = DeltaFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)

    session.set_storage(complex_data)
    storage.save_session(session)

    complex_data.count = 100
    session.set_storage(complex_data)
    storage.save_session(session)

    complex_data.items.append("cherry")
    session.set_storage(complex_data)
    storage.save_session(session)

    file_path = temp_dir / f"{session_id.hex}.jsonl"
    with open(file_path, "r") as f:
        lines = f.readlines()

    assert len(lines) == 3

    loaded_session = storage.load_session(session_id.hex)
    loaded_data = loaded_session.get_storage(ComplexModel)

    assert loaded_data.count == 100
    assert loaded_data.items == ["apple", "banana", "cherry"]


def test_delta_append_only_behavior(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = DeltaFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)

    storage.save_session(session)
    file_path = temp_dir / f"{session_id.hex}.jsonl"
    inode_1 = file_path.stat().st_ino
    size_1 = file_path.stat().st_size

    complex_data.count += 1
    session.set_storage(complex_data)
    storage.save_session(session)

    inode_2 = file_path.stat().st_ino
    size_2 = file_path.stat().st_size

    assert inode_1 == inode_2
    assert size_2 > size_1


def test_delta_reconstruction_fidelity(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = DeltaFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    complex_data.metadata["new_key"] = "new_value"
    session.set_storage(complex_data)
    storage.save_session(session)

    complex_data.items.pop(0)
    session.set_storage(complex_data)
    storage.save_session(session)

    complex_data.score = 99.99
    session.set_storage(complex_data)
    storage.save_session(session)

    loaded_session = storage.load_session(session_id.hex)
    loaded_data = loaded_session.get_storage(ComplexModel)

    assert loaded_data.metadata == {"key": "value", "new_key": "new_value"}
    assert loaded_data.items == ["banana"]
    assert loaded_data.score == 99.99


# --- Robustness Tests ---


def test_snapshot_corrupted_file(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    # Corrupt file
    file_path = temp_dir / f"{session_id.hex}.json"
    with open(file_path, "w") as f:
        f.write("{invalid_json")

    with pytest.raises(json.JSONDecodeError):
        storage.load_session(session_id.hex)


def test_delta_corrupted_last_line(temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
    storage = DeltaFileSessionStorage(save_dir=str(temp_dir))
    session = Session(session_id=session_id)
    session.set_storage(complex_data)
    storage.save_session(session)

    # Corrupt last line
    file_path = temp_dir / f"{session_id.hex}.jsonl"
    with open(file_path, "a") as f:
        f.write("\n{invalid_json_line")

    with pytest.raises(json.JSONDecodeError):
        storage.load_session(session_id.hex)

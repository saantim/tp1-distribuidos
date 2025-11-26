import os
import uuid
from pathlib import Path

import pytest

from worker.base import Session


def test_session_serialization():
    session_id = uuid.uuid4()
    session = Session(session_id=session_id)

    session_dict = session.model_dump()

    assert str(session_dict["session_id"]) == str(session_id)  # Comparar como strings
    assert "storage" in session_dict
    assert session_dict["storage"] is None
    assert "eof_collected" in session_dict
    assert session_dict["eof_collected"] == set()

    assert "_storage" not in session_dict
    assert "_eof_collected" not in session_dict


def test_session_deserialization():
    session_id = uuid.uuid4()
    data = {"session_id": session_id, "storage": {"key": "value"}, "eof_collected": {"worker1", "worker2"}}

    session = Session(**data)

    assert session.session_id == session_id
    assert session.storage == {"key": "value"}
    assert session.eof_collected == {"worker1", "worker2"}


def test_session_roundtrip():
    session_id = uuid.uuid4()
    original_session = Session(session_id=session_id, storage={"key": "value"}, eof_collected={"worker1", "worker2"})

    session_dict = original_session.model_dump()

    new_session = Session(**session_dict)

    assert new_session.session_id == original_session.session_id
    assert new_session.storage == original_session.storage
    assert new_session.eof_collected == original_session.eof_collected


def test_session_save_and_load(tmp_path):
    """Test that a session can be saved and loaded correctly."""
    save_dir = "./sessions"

    session_id = uuid.uuid4()
    session = Session(session_id=session_id)
    session.add_eof("worker1")
    session.add_msg_received("msg1")
    session.storage = {"data": [1, 2, 3], "data2": [1, 2, 3]}
    session.save(save_dir)

    session_file = Path(save_dir + f"/{session_id}.json")
    assert session_file.exists()
    loaded_session = Session.load(session_id, save_dir)

    assert loaded_session is not None
    assert loaded_session.session_id == session.session_id
    assert loaded_session.get_eof_collected() == {"worker1"}
    assert loaded_session.is_duplicated_msg("msg1")


def test_session_load_nonexistent(tmp_path):
    """Test loading a non-existent session returns None."""
    non_existent_id = uuid.uuid4()
    loaded = Session.load(non_existent_id, tmp_path)
    assert loaded is None


def test_session_save_atomicity(tmp_path):
    """Test that session save is atomic (either fully saved or not at all)."""
    session = Session(session_id=uuid.uuid4())

    # Create a read-only directory to force a save error
    read_only_dir = tmp_path / "readonly"
    read_only_dir.mkdir()
    os.chmod(read_only_dir, 0o555)  # Read-only permissions

    # Try to save to read-only directory (should fail)
    with pytest.raises(PermissionError):
        session.save(read_only_dir)

    # Verify no session file was created
    assert not any(read_only_dir.glob("*.json"))


def test_session_save_load_with_storage(tmp_path):
    """Test that session storage is properly saved and loaded."""
    session = Session(session_id=uuid.uuid4())
    test_storage = {"key1": "value1", "key2": [1, 2, 3]}
    session.set_storage(test_storage)

    # Save and load
    session.save(tmp_path)
    loaded = Session.load(session.session_id, tmp_path)

    # Verify storage was preserved
    assert loaded is not None
    assert loaded.get_storage() == test_storage

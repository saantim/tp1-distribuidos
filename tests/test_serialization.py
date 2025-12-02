import uuid

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

import os
import uuid
import tempfile
from pathlib import Path
import json
from worker.base import Session, SessionManager
import pytest

def test_session_serialization():
    session_id = uuid.uuid4()
    session = Session(session_id=session_id)
    
    session_dict = session.model_dump()
    
    assert str(session_dict['session_id']) == str(session_id)  # Comparar como strings
    assert 'storage' in session_dict
    assert session_dict['storage'] is None
    assert 'eof_collected' in session_dict
    assert session_dict['eof_collected'] == set()
    
    assert '_storage' not in session_dict
    assert '_eof_collected' not in session_dict

def test_session_deserialization():
    session_id = uuid.uuid4()
    data = {
        'session_id': session_id,
        'storage': {'key': 'value'},
        'eof_collected': {'worker1', 'worker2'}
    }
    
    session = Session(**data)
    
    assert session.session_id == session_id
    assert session.storage == {'key': 'value'}
    assert session.eof_collected == {'worker1', 'worker2'}

def test_session_manager_serialization():
    def on_start(session):
        pass
        
    def on_end(session):
        pass
    
    manager = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=3,
        is_leader=True
    )
    
    manager_dict = manager.model_dump()
    
    assert manager_dict['stage_name'] == "test_stage"
    assert manager_dict['instances'] == 3
    assert manager_dict['is_leader'] is True
    assert manager_dict['sessions'] == {}
    
    assert '_on_start_of_session' not in manager_dict
    assert '_on_end_of_session' not in manager_dict
    assert '_sessions' not in manager_dict

def test_session_roundtrip():
    session_id = uuid.uuid4()
    original_session = Session(
        session_id=session_id,
        storage={'key': 'value'},
        eof_collected={'worker1', 'worker2'}
    )
    
    session_dict = original_session.model_dump()
    
    new_session = Session(**session_dict)
    
    assert new_session.session_id == original_session.session_id
    assert new_session.storage == original_session.storage
    assert new_session.eof_collected == original_session.eof_collected

def test_session_manager_roundtrip():
    def on_start(session):
        pass
        
    def on_end(session):
        pass
    
    original_manager = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=3,
        is_leader=True
    )
    
    session_id = uuid.uuid4()
    session = Session(session_id=session_id)
    original_manager.sessions[session_id] = session
    
    manager_dict = original_manager.model_dump()
    
    new_manager = SessionManager(
        **{
            **manager_dict,
            'on_start_of_session': on_start,
            'on_end_of_session': on_end
        }
    )
    
    assert new_manager.stage_name == original_manager.stage_name
    assert new_manager.instances == original_manager.instances
    assert new_manager.is_leader == original_manager.is_leader
    
    assert len(new_manager.sessions) == 1
    assert str(session_id) in [str(sid) for sid in new_manager.sessions.keys()]
    assert isinstance(new_manager.sessions[session_id], Session)


def test_save_and_load_sessions(tmp_path):
    def on_start(session):
        pass
        
    def on_end(session):
        pass
    
    manager = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=3,
        is_leader=True
    )
    
    session1_id = uuid.uuid4()
    session2_id = uuid.uuid4()
    
    session1 = Session(session_id=session1_id)
    session1.storage = {"data": [1, 2, 3]}
    session1.eof_collected = {"worker1"}
    
    session2 = Session(session_id=session2_id)
    session2.storage = {"data": [4, 5, 6]}
    session2.eof_collected = {"worker1", "worker2"}
    
    manager.sessions[session1_id] = session1
    manager.sessions[session2_id] = session2
    
    test_file = "./tests/sessions.json"
    manager.save_sessions(test_file)
    
    new_manager = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=3,
        is_leader=True
    )
    
    new_manager.load_sessions(test_file)

    assert manager.sessions == new_manager.sessions

    assert len(new_manager.sessions) == 2
    
    assert str(session1_id) in [str(k) for k in new_manager.sessions.keys()]
    loaded_session1 = new_manager.sessions[session1_id]
    assert loaded_session1.storage == {"data": [1, 2, 3]}
    assert loaded_session1.eof_collected == {"worker1"}
    
    assert str(session2_id) in [str(k) for k in new_manager.sessions.keys()]
    loaded_session2 = new_manager.sessions[session2_id]
    assert loaded_session2.storage == {"data": [4, 5, 6]}
    assert loaded_session2.eof_collected == {"worker1", "worker2"}

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
    assert loaded_session.is_msg_received("msg1")

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

def test_session_load_from_tmp_on_crash(tmp_path):
    """Test that a session can be recovered from tmp after a crash during save."""
    session_id = uuid.uuid4()
    save_dir = tmp_path / "sessions"
    
    # Create a session and save it
    session = Session(session_id=session_id)
    session.add_eof("worker1")
    
    # Manually create a tmp file as if a crash occurred during save
    tmp_dir = save_dir / "tmp"
    tmp_dir.mkdir(parents=True)
    
    # Create a temporary file with session data
    tmp_file = tmp_dir / f"{session_id}.12345.tmp"
    with open(tmp_file, 'w') as f:
        json.dump(session.to_dict(), f)
    
    # Now try to load - should recover from tmp file
    loaded = Session.load(session_id, save_dir)
    
    # Verify the session was loaded from tmp and moved to main dir
    assert loaded is not None
    assert loaded.session_id == session_id
    assert loaded.get_eof_collected() == {"worker1"}
    assert (save_dir / f"{session_id}.json").exists()
    assert not tmp_file.exists()  # Should have been moved

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

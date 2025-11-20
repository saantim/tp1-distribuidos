import uuid
from worker.base import Session, SessionManager

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

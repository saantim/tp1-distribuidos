import uuid
from pathlib import Path

from worker.base import SessionManager


def test_session_manager_save_and_load_directory(tmp_path):
    # Callbacks no-op
    def on_start(session):
        pass

    def on_end(session):
        pass

    save_dir = tmp_path / "sessions"

    m1 = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=2,
        is_leader=True,
    )

    s1_id = uuid.uuid4()
    s2_id = uuid.uuid4()

    s1 = m1.get_or_initialize(s1_id)
    s1.set_storage({"data": [1, 2, 3]})
    s1.add_eof("worker1")

    s2 = m1.get_or_initialize(s2_id)
    s2.set_storage({"data": [4, 5, 6]})
    s2.add_eof("worker1")
    s2.add_eof("worker2")

    m1.save_sessions(save_dir)

    assert (save_dir / f"{s1_id}.json").exists()
    assert (save_dir / f"{s2_id}.json").exists()

    m2 = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=2,
        is_leader=True,
    )

    m2.load_sessions(save_dir)

    assert len(m2._sessions) == 2
    assert s1_id in m2._sessions
    assert s2_id in m2._sessions

    ls1 = m2._sessions[s1_id]
    ls2 = m2._sessions[s2_id]

    assert ls1.get_storage() == {"data": [1, 2, 3]}
    assert ls1.get_eof_collected() == {"worker1"}

    assert ls2.get_storage() == {"data": [4, 5, 6]}
    assert ls2.get_eof_collected() == {"worker1", "worker2"}

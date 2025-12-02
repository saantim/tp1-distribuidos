import uuid

from pydantic import BaseModel

from worker.session.manager import SessionManager
from worker.storage import SnapshotFileSessionStorage


class TestModel(BaseModel):
    data: list[int]


def test_session_manager_save_and_load_directory(tmp_path):
    def on_start(session):
        pass

    def on_end(session):
        pass

    save_dir = tmp_path / "sessions"
    storage = SnapshotFileSessionStorage(save_dir=str(save_dir))

    m1 = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=2,
        is_leader=True,
        session_storage=storage,
    )

    s1_id = uuid.uuid4()
    s2_id = uuid.uuid4()

    s1 = m1.get_or_initialize(s1_id)
    s1.set_storage(TestModel(data=[1, 2, 3]))
    s1.add_eof("worker1")

    s2 = m1.get_or_initialize(s2_id)
    s2.set_storage(TestModel(data=[4, 5, 6]))
    s2.add_eof("worker1")
    s2.add_eof("worker2")

    m1.save_sessions()

    assert (save_dir / f"{s1_id.hex}.json").exists()
    assert (save_dir / f"{s2_id.hex}.json").exists()

    m2 = SessionManager(
        stage_name="test_stage",
        on_start_of_session=on_start,
        on_end_of_session=on_end,
        instances=2,
        is_leader=True,
        session_storage=storage,
    )

    m2.load_sessions()

    assert len(m2._sessions) == 2
    assert s1_id in m2._sessions
    assert s2_id in m2._sessions

    ls1 = m2._sessions[s1_id]
    ls2 = m2._sessions[s2_id]

    assert ls1.get_storage(TestModel).data == [1, 2, 3]
    assert ls1.get_eof_collected() == {"worker1"}

    assert ls2.get_storage(TestModel).data == [4, 5, 6]
    assert ls2.get_eof_collected() == {"worker1", "worker2"}

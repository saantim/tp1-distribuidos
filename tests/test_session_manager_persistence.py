import os
import uuid

import pytest
from pydantic import BaseModel

from worker.sessions.manager import SessionManager
from worker.sessions.storage import SnapshotFileSessionStorage


class TestModel(BaseModel):
    data: list[int]


@pytest.fixture
def session_manager_factory(tmp_path):
    def _create(save_dir=None):
        if save_dir is None:
            save_dir = tmp_path / "sessions"

        storage = SnapshotFileSessionStorage(save_dir=str(save_dir))

        return (
            SessionManager(
                stage_name="test_stage",
                on_start_of_session=lambda s: None,
                on_end_of_session=lambda s: None,
                instances=2,
                is_leader=True,
                session_storage=storage,
            ),
            save_dir,
        )

    return _create


def test_session_manager_save_and_load_directory(session_manager_factory):
    """
    Verify that SessionManager can save multiple sessions to disk and reload them correctly.
    """
    m1, save_dir = session_manager_factory()

    s1_id = uuid.uuid4()
    s2_id = uuid.uuid4()

    # Create and populate session 1
    s1 = m1.get_or_initialize(s1_id)
    s1.set_storage(TestModel(data=[1, 2, 3]))
    s1.add_eof("worker1")

    # Create and populate session 2
    s2 = m1.get_or_initialize(s2_id)
    s2.set_storage(TestModel(data=[4, 5, 6]))
    s2.add_eof("worker1")
    s2.add_eof("worker2")

    # Persist all sessions
    m1.save_sessions()

    # Verify files exist
    assert (save_dir / f"{s1_id.hex}.json").exists()
    assert (save_dir / f"{s2_id.hex}.json").exists()

    # Create a new manager and load sessions
    m2, _ = session_manager_factory(save_dir)
    m2.load_sessions()

    # Verify loaded state
    assert len(m2._sessions) == 2
    assert s1_id in m2._sessions
    assert s2_id in m2._sessions

    ls1 = m2._sessions[s1_id]
    ls2 = m2._sessions[s2_id]

    assert ls1.get_storage(TestModel).data == [1, 2, 3]
    assert ls1.get_eof_collected() == {"worker1"}

    assert ls2.get_storage(TestModel).data == [4, 5, 6]
    assert ls2.get_eof_collected() == {"worker1", "worker2"}


def test_session_manager_atomic_write_crash(session_manager_factory, monkeypatch):
    """
    Simulate a crash during the atomic write process (after writing to tmp but before rename).
    Verify that the partial/tmp file is ignored or handled, and valid data is preserved.

    Note: SnapshotFileSessionStorage doesn't currently have auto-recovery from tmp files
    implemented in load_sessions (it only looks for .json files).
    However, the atomic write ensures that we don't end up with corrupted .json files.
    """
    m1, save_dir = session_manager_factory()

    s1_id = uuid.uuid4()
    s2_id = uuid.uuid4()

    # Session 1: Saved successfully
    s1 = m1.get_or_initialize(s1_id)
    s1.set_storage(TestModel(data=[1, 2, 3]))
    m1.save_session(s1)

    # Session 2: Fails during save (simulated crash)
    s2 = m1.get_or_initialize(s2_id)
    s2.set_storage(TestModel(data=[4, 5, 6]))

    # Mock os.replace to simulate crash after writing tmp file
    orig_replace = os.replace

    def crash_replace(src, dst):
        raise RuntimeError("simulated crash during save")

    monkeypatch.setattr(os, "replace", crash_replace)

    with pytest.raises(RuntimeError):
        m1.save_session(s2)

    monkeypatch.setattr(os, "replace", orig_replace)

    # Verify file state
    assert (save_dir / f"{s1_id.hex}.json").exists()
    # s2 should NOT have a .json file
    assert not (save_dir / f"{s2_id.hex}.json").exists()
    # s2 SHOULD have a tmp file
    assert (save_dir / "tmp" / f"{s2_id.hex}.json").exists()

    # Reload
    m2, _ = session_manager_factory(save_dir)
    m2.load_sessions()

    # Verify s1 is loaded, s2 is not (since it wasn't successfully committed)
    assert s1_id in m2._sessions
    assert s2_id not in m2._sessions

    ls1 = m2._sessions[s1_id]
    assert ls1.get_storage(TestModel).data == [1, 2, 3]

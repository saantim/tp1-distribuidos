import json
import os
import uuid
from pathlib import Path
from typing import Type

import pytest

from tests.base import BaseSessionStorageTest, ComplexModel
from worker.session_storage import SessionStorage, SnapshotFileSessionStorage


class TestSnapshotStorage(BaseSessionStorageTest):

    def test_save_load_roundtrip(self, storage: SessionStorage, session_id: uuid.UUID, complex_data: ComplexModel):
        session = self.create_session(session_id, complex_data)
        storage.save_session(session)

        loaded_session = storage.load_session(session_id.hex)
        loaded_data = loaded_session.get_storage(ComplexModel)

        assert loaded_data == complex_data
        assert loaded_data.items == ["apple", "banana"]
        assert loaded_data.score == 3.14

    def test_persistence_across_instances(
        self, storage_class: Type[SessionStorage], temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel
    ):
        # Instance A
        storage_a = storage_class(save_dir=str(temp_dir))
        session = self.create_session(session_id, complex_data)
        storage_a.save_session(session)

        # Instance B (same dir)
        storage_b = storage_class(save_dir=str(temp_dir))
        loaded_session = storage_b.load_session(session_id.hex)
        loaded_data = loaded_session.get_storage(ComplexModel)

        assert loaded_data == complex_data

    def test_load_non_existent(self, storage: SessionStorage):
        with pytest.raises(FileNotFoundError):
            storage.load_session(uuid.uuid4().hex)

    def test_load_all_sessions(self, storage: SessionStorage, complex_data: ComplexModel):
        ids = [uuid.uuid4() for _ in range(5)]

        for sid in ids:
            session = self.create_session(sid, complex_data)
            storage.save_session(session)

        loaded_sessions = storage.load_sessions()
        loaded_ids = {s.session_id for s in loaded_sessions}

        assert len(loaded_sessions) == 5
        assert loaded_ids == set(ids)

    def test_snapshot_json_format(self, temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
        storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))
        session = self.create_session(session_id, complex_data)
        storage.save_session(session)

        file_path = temp_dir / f"{session_id.hex}.json"
        assert file_path.exists()

        with open(file_path, "r") as f:
            content = json.load(f)

        assert content["session_id"] == str(session_id)
        assert content["storage"]["id"] == complex_data.id

    def test_snapshot_atomic_write_crash(
        self, temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel, monkeypatch
    ):
        """Verify that a crash during write doesn't corrupt the existing file."""
        storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))

        # Initial save
        session = self.create_session(session_id, complex_data)
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

    def test_snapshot_corrupted_file(self, temp_dir: Path, session_id: uuid.UUID, complex_data: ComplexModel):
        storage = SnapshotFileSessionStorage(save_dir=str(temp_dir))
        session = self.create_session(session_id, complex_data)
        storage.save_session(session)

        # Corrupt file
        file_path = temp_dir / f"{session_id.hex}.json"
        with open(file_path, "w") as f:
            f.write("{invalid_json")

        with pytest.raises(json.JSONDecodeError):
            storage.load_session(session_id.hex)

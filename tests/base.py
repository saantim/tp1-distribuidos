import uuid
from pathlib import Path
from typing import Type

import pytest
from pydantic import BaseModel, Field

from worker.session import Session, SessionStorage
from worker.storage import SnapshotFileSessionStorage


class ComplexModel(BaseModel):
    id: str
    count: int
    items: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    score: float = 0.0


class BaseSessionStorageTest:
    @pytest.fixture
    def temp_dir(self, tmp_path) -> Path:
        return tmp_path

    @pytest.fixture
    def storage_class(self) -> Type[SessionStorage]:
        return SnapshotFileSessionStorage

    @pytest.fixture
    def storage(self, storage_class, temp_dir) -> SessionStorage:
        return storage_class(save_dir=str(temp_dir))

    @pytest.fixture
    def session_id(self) -> uuid.UUID:
        return uuid.uuid4()

    @pytest.fixture
    def complex_data(self) -> ComplexModel:
        return ComplexModel(id="test-1", count=42, items=["apple", "banana"], metadata={"key": "value"}, score=3.14)

    def create_session(self, session_id: uuid.UUID, data: BaseModel) -> Session:
        session = Session(session_id=session_id)
        session.set_storage(data)
        return session

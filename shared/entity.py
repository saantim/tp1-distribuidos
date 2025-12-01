import json
from datetime import date, datetime
from typing import Any, NewType, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict, model_serializer, model_validator, ValidationError


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def __str__(self):
        return str(self.model_dump())

    def serialize(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        return cls(**data)

    @classmethod
    def deserialize(cls, payload: bytes):
        json_str = payload.decode()
        return cls.model_validate_json(json_str)

    @classmethod
    def is_type(cls, payload: bytes) -> bool:
        try:
            json_str = payload.decode()
            data = json.loads(json_str)
            obj = cls.model_validate(data, strict=True)
            return type(obj) is cls
        except (ValidationError, json.JSONDecodeError, UnicodeDecodeError):
            return False


T = TypeVar("T", bound="ListSerializable")


class ListSerializable(BaseModel):
    @model_serializer(mode="plain")
    def _serialize_as_list(self) -> list[Any]:
        return list(self.model_dump().values())

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls: Type[T], value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            field_names = list(cls.model_fields.keys())
            if len(value) != len(field_names):
                raise ValueError(f"Expected {len(field_names)} items, got {len(value)}")
            return dict(zip(field_names, value))
        return value


class EOF(Message):
    type: str = "EOF"


class WorkerEOF(Message):
    worker_id: str


class Heartbeat(Message):
    container_name: str
    timestamp: float


class RawMessage(Message):
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )
    raw_data: bytes


ItemId = NewType("ItemId", int)
ItemName = NewType("ItemName", str)


class MenuItem(Message):
    item_id: ItemId
    item_name: ItemName


StoreId = NewType("StoreId", int)
StoreName = NewType("StoreName", str)


class Store(Message):
    store_id: StoreId
    store_name: StoreName


Quantity = NewType("Quantity", int)
Subtotal = NewType("Subtotal", float)
CreatedAt = NewType("CreatedAt", datetime)


class TransactionItem(Message, ListSerializable):
    item_id: ItemId
    quantity: Quantity
    subtotal: Subtotal
    created_at: CreatedAt


UserId = NewType("UserId", int)
Birthdate = NewType("Birthdate", date)


class User(Message, ListSerializable):
    user_id: UserId
    birthdate: Birthdate


TransactionId = NewType("TransactionId", str)
FinalAmount = NewType("FinalAmount", float)


class Transaction(Message, ListSerializable):
    id: TransactionId
    store_id: StoreId
    user_id: Optional[UserId]
    final_amount: FinalAmount
    created_at: CreatedAt

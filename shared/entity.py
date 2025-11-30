import json
from datetime import datetime, date
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict, ValidationError, model_serializer, model_validator


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


class TransactionItem(Message):
    item_id: ItemId
    quantity: Quantity
    subtotal: Subtotal
    created_at: CreatedAt

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.item_id, self.quantity, self.subtotal, self.created_at]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 4:
                raise ValueError
            return {"item_id": value[0], "quantity": value[1], "subtotal": value[2], "created_at": value[3]}
        return value


UserId = NewType("UserId", int)
Birthdate = NewType("Birthdate", date)


class User(Message):
    user_id: UserId
    birthdate: Birthdate

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.user_id, self.birthdate]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 2:
                raise ValueError
            return {"user_id": value[0], "birthdate": value[1]}
        return value


TransactionId = NewType("TransactionId", str)
FinalAmount = NewType("FinalAmount", float)


class Transaction(Message):
    id: TransactionId
    store_id: StoreId
    user_id: Optional[UserId]
    final_amount: FinalAmount
    created_at: CreatedAt

    @model_serializer(mode="plain")
    def _serialize_as_list(self):
        return [self.id, self.store_id, self.user_id, self.final_amount, self.created_at]

    @model_validator(mode="before")
    @classmethod
    def _parse_from_list(cls, value):
        if isinstance(value, (list, tuple)):
            if len(value) != 5:
                raise ValueError
            return {"id": value[0], "store_id": value[1], "user_id": value[2], "final_amount": value[3], "created_at": value[4]}
        return value

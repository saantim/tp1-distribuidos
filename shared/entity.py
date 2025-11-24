from datetime import datetime
from typing import NewType, Optional

from pydantic import BaseModel, ConfigDict


class Message(BaseModel):

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
    def is_type(cls, payload: bytes):
        try:
            json_str = payload.decode()
            cls.model_validate_json(json_str)
            return True
        except Exception as e:
            _ = e
            return False


class EOF(Message):
    pass


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


UserId = NewType("UserId", int)
Birthdate = NewType("Birthdate", datetime)


class User(Message):
    user_id: UserId
    birthdate: Birthdate


TransactionId = NewType("TransactionId", str)
FinalAmount = NewType("FinalAmount", float)


class Transaction(Message):
    id: TransactionId
    store_id: StoreId
    user_id: Optional[UserId]
    final_amount: FinalAmount
    created_at: CreatedAt

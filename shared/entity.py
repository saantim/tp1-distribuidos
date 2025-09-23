from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Store:
    """Store entity representation"""

    store_id: int
    store_name: str
    street: str
    postal_code: int
    city: str
    state: str
    latitude: float
    longitude: float


@dataclass
class User:
    """User entity representation"""

    user_id: int
    gender: str
    birthdate: datetime
    registered_at: datetime


@dataclass
class Transaction:
    """Transaction entity representation"""

    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: Optional[str]
    user_id: Optional[int]
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: datetime


@dataclass
class TransactionItem:
    """Transaction Item entity representation"""

    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: datetime


@dataclass
class MenuItem:
    """Menu Item entity representation"""

    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: float
    available_to: float

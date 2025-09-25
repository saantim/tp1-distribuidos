from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict


EPOCH = datetime(1970, 1, 1)


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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Store":
        """Create Store from CSV dict with proper defaults for empty values"""
        return cls(
            store_id=_safe_int(data.get("store_id"), default=0),
            store_name=_safe_str(data.get("store_name"), default="Unknown Store"),
            street=_safe_str(data.get("street"), default="Unknown Street"),
            postal_code=_safe_int(data.get("postal_code"), default=0),
            city=_safe_str(data.get("city"), default="Unknown City"),
            state=_safe_str(data.get("state"), default="Unknown State"),
            latitude=_safe_float(data.get("latitude"), default=0.0),
            longitude=_safe_float(data.get("longitude"), default=0.0),
        )

    @classmethod
    def estimated_size(cls):
        """
        Fixed: store_id(1) + postal_code(4) + lat(8) + lon(8) = 21
        Based on CSVs used by the data set.

        Variable strings have a +1 for their prefix.
            - store_name: ~25 chars avg ("G Coffee @ USJ 89q")
            - street: ~20 chars avg ("Jalan Dewan Bahasa 5/9")
            - city: ~15 chars avg ("USJ 89q")
            - state: ~18 chars avg ("Selangor Darul Ehsan")
        """
        return 21 + (1 + 25) + (1 + 20) + (1 + 15) + (1 + 18)


@dataclass
class User:
    """User entity representation"""

    user_id: int
    gender: str
    birthdate: datetime
    registered_at: datetime

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "User":
        """Create User from CSV dict with proper defaults for empty values"""
        return cls(
            user_id=_safe_int(data.get("user_id"), default=0),
            gender=_safe_str(data.get("gender"), default="unknown"),
            birthdate=_safe_date(data.get("birthdate"), default=EPOCH),
            registered_at=_safe_datetime(data.get("registered_at"), EPOCH),
        )

    @classmethod
    def estimated_size(cls):
        """
        Fixed: user_id(4) + birthdate(8) + registered_at(8) = 20

        Based on CSVs used by the data set.
        Variable strings have a +1 for their prefix.
            - gender (~6 chars avg "female")
        """
        return 20 + (1 + 6)


@dataclass
class Transaction:
    """Transaction entity representation"""

    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: int  # 0 for empty
    user_id: int  # 0 for empty
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: datetime

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Transaction":
        """Create Transaction from CSV dict with proper defaults for empty values"""
        return cls(
            transaction_id=_safe_str(data.get("transaction_id"), default="unknown-transaction"),
            store_id=_safe_int(data.get("store_id"), default=0),
            payment_method_id=_safe_int(data.get("payment_method_id"), default=0),
            voucher_id=_safe_int(data.get("voucher_id"), default=0),
            user_id=_safe_int(data.get("user_id"), default=0),
            original_amount=_safe_float(data.get("original_amount"), default=0.0),
            discount_applied=_safe_float(data.get("discount_applied"), default=0.0),
            final_amount=_safe_float(data.get("final_amount"), default=0.0),
            created_at=_safe_datetime(data.get("created_at"), EPOCH),
        )

    @classmethod
    def estimated_size(cls):
        """
        Fixed: store_id(1) + payment_id(4) + voucher_id(4) + user_id(4) + amounts(24) + created_at(8) = 45

        Based on CSVs used by the data set.
        Variable strings have a +1 for their prefix.
            - transaction_id (36 chars UUID)
        """
        return 45 + (1 + 36)


@dataclass
class TransactionItem:
    """Transaction Item entity representation"""

    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: datetime

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransactionItem":
        """Create TransactionItem from CSV dict with proper defaults for empty values"""
        return cls(
            transaction_id=_safe_str(data.get("transaction_id"), default="unknown-transaction"),
            item_id=_safe_int(data.get("item_id"), default=0),
            quantity=_safe_int(data.get("quantity"), default=0),
            unit_price=_safe_float(data.get("unit_price"), default=0.0),
            subtotal=_safe_float(data.get("subtotal"), default=0.0),
            created_at=_safe_datetime(data.get("created_at"), EPOCH),
        )

    @classmethod
    def estimated_size(cls):
        """
        Fixed: item_id(1) + quantity(4) + unit_price(8) + subtotal(8) + created_at(8) = 29

        Based on CSVs used by the data set.
        Variable strings have a +1 for their prefix.
            - transaction_id (36 chars UUID)
        """
        return 29 + (1 + 36)


@dataclass
class MenuItem:
    """Menu Item entity representation"""

    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: datetime  # epoch for empty
    available_to: datetime  # epoch for empty

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MenuItem":
        """Create MenuItem from CSV dict with proper defaults for empty values"""
        return cls(
            item_id=_safe_int(data.get("item_id"), default=0),
            item_name=_safe_str(data.get("item_name"), default="Unknown Item"),
            category=_safe_str(data.get("category"), default="unknown"),
            price=_safe_float(data.get("price"), default=0.0),
            is_seasonal=_safe_bool(data.get("is_seasonal"), default=False),
            available_from=_safe_datetime(data.get("available_from"), EPOCH),
            available_to=_safe_datetime(data.get("available_to"), EPOCH),
        )

    @classmethod
    def estimated_size(cls):
        """
        Fixed: item_id(1) + price(8) + is_seasonal(1) + available_from(8) + available_to(8) = 26

        Based on CSVs used by the data set.
        Variable strings have a +1 for their prefix.
            - item_name: ~12 chars avg ("Matcha Latte")
            - category: ~8 chars avg ("non-coffee")
        """
        return 26 + (1 + 12) + (1 + 8)


def _safe_str(value: Any, default: str = "") -> str:
    """Convert to string, handling None and empty values"""
    if value is None or value == "":
        return default
    return str(value).strip()


def _safe_int(value: Any, default: int = 0) -> int:
    """Convert to int, handling None, empty strings, and floats"""
    if value is None or value == "":
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert to float, handling None and empty values"""
    if value is None or value == "":
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    """Convert to bool, handling various string representations"""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1")
    return bool(value)


def _safe_date(value: Any, default: datetime) -> datetime:
    """Convert to date (YYYY-MM-DD format)"""
    if value is None or value == "":
        return default
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d")
        except ValueError:
            return default
    return default


def _safe_datetime(value: Any, default: datetime) -> datetime:
    """Convert to datetime (YYYY-MM-DD HH:MM:SS format)"""
    if value is None or value == "":
        return default
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return default
    return default

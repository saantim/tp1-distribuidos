"""
User entity transformer module.

Expected CSV format:
user_id,gender,birthdate,registered_at

We only extract:
- user_id
- birthdate
"""

from datetime import datetime

from shared.entity import User


def parse_csv_row(csv_row: str) -> dict:
    """
    Parse CSV row string into dictionary.
    """
    parts = csv_row.split(",")

    if len(parts) < 3:
        raise ValueError(f"Expected at least 3 fields, got {len(parts)}")

    birthdate_str = parts[2].strip()
    birthdate = datetime.strptime(birthdate_str, "%Y-%m-%d") if birthdate_str else None

    return {
        "user_id": int(parts[0].strip()),
        "birthdate": birthdate,
    }


def create_entity(row_dict: dict) -> User:
    """
    Create User entity from parsed row dictionary.

    Args:
        row_dict: Dictionary with user_id and birthdate

    Returns:
        User entity
    """
    return User(
        user_id=row_dict["user_id"],
        birthdate=row_dict["birthdate"],
    )

from typing import Literal


BIG_ENDIAN: Literal["big"] = "big"


class ByteReader:
    """Utility class for reading bytes with automatic offset management."""

    def __init__(self, data: bytes, offset: int = 0):
        self.data = data
        self.offset = offset

    def read_uint8(self) -> int:
        """Read uint8 and advance offset."""
        value = int.from_bytes(self.data[self.offset : self.offset + 1], BIG_ENDIAN)
        self.offset += 1
        return value

    def read_uint16(self) -> int:
        """Read uint16 and advance offset."""
        value = int.from_bytes(self.data[self.offset : self.offset + 2], BIG_ENDIAN)
        self.offset += 2
        return value

    def read_uint32(self) -> int:
        """Read uint32 and advance offset."""
        value = int.from_bytes(self.data[self.offset : self.offset + 4], BIG_ENDIAN)
        self.offset += 4
        return value

    def read_bytes(self, length: int) -> bytes:
        """Read arbitrary bytes and advance offset."""
        value = self.data[self.offset : self.offset + length]
        self.offset += length
        return value

    def read_string(self) -> str:
        """Read length-prefixed string (uint8 length + string bytes)."""
        length = self.read_uint8()
        string_bytes = self.read_bytes(length)
        return string_bytes.decode("utf-8")


class ByteWriter:
    """Utility class for writing bytes with automatic concatenation."""

    def __init__(self):
        self.data = b""

    def write_uint8(self, value: int) -> "ByteWriter":
        """Write uint8 and return self."""
        self.data += value.to_bytes(1, BIG_ENDIAN)
        return self

    def write_uint16(self, value: int) -> "ByteWriter":
        """Write uint16 and return self."""
        self.data += value.to_bytes(2, BIG_ENDIAN)
        return self

    def write_uint32(self, value: int) -> "ByteWriter":
        """Write uint32 and return self."""
        self.data += value.to_bytes(4, BIG_ENDIAN)
        return self

    def write_bytes(self, value: bytes) -> "ByteWriter":
        """Write arbitrary bytes and return self."""
        self.data += value
        return self

    def write_string(self, value: str) -> "ByteWriter":
        """Write length-prefixed string (uint8 length + string bytes)."""
        encoded = value.encode("utf-8")
        self.write_uint8(len(encoded))
        self.write_bytes(encoded)
        return self

    def get_bytes(self) -> bytes:
        """Get the final byte array."""
        return self.data

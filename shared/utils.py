from typing import List


class ByteWriter:
    """Efficient byte writer using list accumulation instead of string concatenation."""

    def __init__(self):
        self.chunks: List[bytes] = []

    def write_uint8(self, value: int) -> "ByteWriter":
        """Write uint8 and return self for chaining."""
        self.chunks.append(value.to_bytes(1))
        return self

    def write_uint32(self, value: int) -> "ByteWriter":
        """Write uint32 and return self for chaining."""
        self.chunks.append(value.to_bytes(4))
        return self

    def write_int64(self, value: int) -> "ByteWriter":
        """Write signed int64 and return self for chaining."""
        self.chunks.append(value.to_bytes(8, signed=True))
        return self

    def write_uint64(self, value: int) -> "ByteWriter":
        """Write unsigned int64 and return self for chaining."""
        self.chunks.append(value.to_bytes(8))
        return self

    def write_uint128(self, value: int) -> "ByteWriter":
        """Write unsigned 128-bit integer (16 bytes) for UUID storage."""
        self.chunks.append(value.to_bytes(16, byteorder="big"))
        return self

    def write_float64(self, value: float) -> "ByteWriter":
        """Write double precision float and return self for chaining."""
        import struct

        self.chunks.append(struct.pack(">d", value))
        return self

    def write_bytes(self, data: bytes) -> "ByteWriter":
        """Write raw bytes and return self for chaining."""
        self.chunks.append(data)
        return self

    def write_string(self, value: str) -> "ByteWriter":
        """Write length-prefixed string (uint8 length + UTF-8 bytes)."""
        encoded = value.encode("utf-8")
        if len(encoded) > 255:
            encoded = encoded[:255]  # Truncate to fit in uint8 length
        self.chunks.append(len(encoded).to_bytes(1))
        self.chunks.append(encoded)
        return self

    def get_bytes(self) -> bytes:
        """Get the final byte array by joining all chunks."""
        return b"".join(self.chunks)


class ByteReader:
    """Utility class for reading bytes with automatic offset management."""

    def __init__(self, data: bytes, offset: int = 0):
        self.data = data
        self.offset = offset
        self.length = len(data)

    def has_bytes(self, count: int) -> bool:
        """Check if there are enough bytes remaining."""
        return self.offset + count <= self.length

    def read_uint8(self) -> int:
        """Read uint8 and advance offset."""
        if not self.has_bytes(1):
            raise ValueError(f"Not enough bytes: need 1, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 1])
        self.offset += 1
        return value

    def read_uint16(self) -> int:
        """Read uint16 and advance offset."""
        if not self.has_bytes(2):
            raise ValueError(f"Not enough bytes: need 2, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 2])
        self.offset += 2
        return value

    def read_uint32(self) -> int:
        """Read uint32 and advance offset."""
        if not self.has_bytes(4):
            raise ValueError(f"Not enough bytes: need 4, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 4])
        self.offset += 4
        return value

    def read_int64(self) -> int:
        """Read signed int64 and advance offset."""
        if not self.has_bytes(8):
            raise ValueError(f"Not enough bytes: need 8, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 8], signed=True)
        self.offset += 8
        return value

    def read_uint64(self) -> int:
        """Read unsigned int64 and advance offset."""
        if not self.has_bytes(8):
            raise ValueError(f"Not enough bytes: need 8, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 8])
        self.offset += 8
        return value

    def read_uint128(self) -> int:
        """Read unsigned 128-bit integer (16 bytes) for UUID storage."""
        if not self.has_bytes(16):
            raise ValueError(f"Not enough bytes: need 16, have {self.length - self.offset}")
        value = int.from_bytes(self.data[self.offset : self.offset + 16], byteorder="big")
        self.offset += 16
        return value

    def read_float64(self) -> float:
        """Read double precision float and advance offset."""
        if not self.has_bytes(8):
            raise ValueError(f"Not enough bytes: need 8, have {self.length - self.offset}")
        import struct

        value = struct.unpack(">d", self.data[self.offset : self.offset + 8])[0]
        self.offset += 8
        return value

    def read_bytes(self, length: int) -> bytes:
        """Read arbitrary bytes and advance offset."""
        if not self.has_bytes(length):
            raise ValueError(f"Not enough bytes: need {length}, have {self.length - self.offset}")
        value = self.data[self.offset : self.offset + length]
        self.offset += length
        return value

    def read_string(self) -> str:
        """Read length-prefixed string (uint8 length + UTF-8 bytes)."""
        length = self.read_uint8()
        string_bytes = self.read_bytes(length)
        return string_bytes.decode("utf-8")


def unpack_result_batch(data: bytes) -> List[bytes]:
    """
    Unpacks EntityBatch format into list of raw entity bytes.

    EntityBatch format:
    - entity_count: uint32
    - For each entity:
      - entity_length: uint32
      - entity_bytes: variable length

    Args:
        data: Serialized EntityBatch bytes

    Returns:
        List of raw entity bytes
    """
    reader = ByteReader(data)
    entity_count = reader.read_uint32()
    entities = []

    for _ in range(entity_count):
        entity_length = reader.read_uint32()
        entity_bytes = reader.read_bytes(entity_length)
        entities.append(entity_bytes)

    return entities

"""
LEB128 Encoding/Decoding
Variable-length encoding for efficient integer storage
"""

from typing import List, Tuple, Optional

def encode_leb128(value: int) -> bytes:
    """
    Encode unsigned integer using LEB128 (Little Endian Base 128)
    
    Args:
        value: Unsigned integer to encode
        
    Returns:
        Encoded bytes
        
    Raises:
        ValueError: If value is negative
    """
    if value < 0:
        raise ValueError(f"Cannot encode negative value {value} as unsigned LEB128")
    
    result = bytearray()
    
    while True:
        byte = value & 0x7F  # Take lower 7 bits
        value >>= 7
        
        if value != 0:
            # More bytes to come, set continuation bit
            byte |= 0x80
        
        result.append(byte)
        
        if value == 0:
            break
    
    return bytes(result)


def encode_signed_leb128(value: int) -> bytes:
    """
    Encode signed integer using signed LEB128
    
    Args:
        value: Signed integer to encode
        
    Returns:
        Encoded bytes
    """
    result = bytearray()
    
    while True:
        byte = value & 0x7F  # Take lower 7 bits
        value >>= 7
        
        # Sign bit is the 6th bit of the current byte
        sign_bit = byte & 0x40
        
        # Check if we're done:
        # - For positive: value == 0 and sign bit clear
        # - For negative: value == -1 and sign bit set
        if (value == 0 and sign_bit == 0) or (value == -1 and sign_bit != 0):
            result.append(byte)
            break
        else:
            # More bytes to come
            byte |= 0x80
            result.append(byte)
    
    return bytes(result)


def decode_leb128(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode unsigned LEB128 integer
    
    Args:
        data: Bytes to decode
        offset: Starting offset in data
        
    Returns:
        Tuple of (decoded value, bytes consumed)
    """
    result = 0
    shift = 0
    consumed = 0
    
    while offset + consumed < len(data):
        byte = data[offset + consumed]
        consumed += 1
        
        result |= (byte & 0x7F) << shift
        
        if byte & 0x80 == 0:
            break
        
        shift += 7
        
        # Prevent infinite loops on malformed data
        if shift > 63:
            raise ValueError("LEB128 value too large (>64 bits)")
    
    return result, consumed


def decode_signed_leb128(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode signed LEB128 integer
    
    Args:
        data: Bytes to decode
        offset: Starting offset in data
        
    Returns:
        Tuple of (decoded value, bytes consumed)
    """
    result = 0
    shift = 0
    consumed = 0
    
    while offset + consumed < len(data):
        byte = data[offset + consumed]
        consumed += 1
        
        result |= (byte & 0x7F) << shift
        shift += 7
        
        if byte & 0x80 == 0:
            # Last byte - check sign
            if shift < 64 and (byte & 0x40):
                # Sign extend
                result |= -(1 << shift)
            break
        
        # Prevent infinite loops
        if shift > 63:
            raise ValueError("Signed LEB128 value too large (>64 bits)")
    
    return result, consumed


class LEB128Encoder:
    """Helper class for encoding multiple values"""
    
    def __init__(self):
        self._buffer = bytearray()
    
    def write_unsigned(self, value: int):
        """Write unsigned LEB128 value"""
        self._buffer.extend(encode_leb128(value))
    
    def write_signed(self, value: int):
        """Write signed LEB128 value"""
        self._buffer.extend(encode_signed_leb128(value))
    
    def write_string(self, value: str):
        """Write length-prefixed string"""
        encoded = value.encode('utf-8')
        self.write_unsigned(len(encoded))
        self._buffer.extend(encoded)
    
    def write_bytes(self, value: bytes):
        """Write length-prefixed bytes"""
        self.write_unsigned(len(value))
        self._buffer.extend(value)
    
    def write_float(self, value: float):
        """Write float as 8 bytes (double precision)"""
        import struct
        self._buffer.extend(struct.pack('<d', value))
    
    def write_bool(self, value: bool):
        """Write boolean as single byte"""
        self._buffer.append(1 if value else 0)
    
    def get_bytes(self) -> bytes:
        """Get encoded bytes"""
        return bytes(self._buffer)
    
    def clear(self):
        """Clear the buffer"""
        self._buffer.clear()
    
    def size(self) -> int:
        """Get current buffer size"""
        return len(self._buffer)


class LEB128Decoder:
    """Helper class for decoding multiple values"""
    
    def __init__(self, data: bytes):
        self._data = data
        self._offset = 0
    
    def read_unsigned(self) -> int:
        """Read unsigned LEB128 value"""
        value, consumed = decode_leb128(self._data, self._offset)
        self._offset += consumed
        return value
    
    def read_signed(self) -> int:
        """Read signed LEB128 value"""
        value, consumed = decode_signed_leb128(self._data, self._offset)
        self._offset += consumed
        return value
    
    def read_string(self) -> str:
        """Read length-prefixed string"""
        length = self.read_unsigned()
        if self._offset + length > len(self._data):
            raise ValueError("String length exceeds available data")
        value = self._data[self._offset:self._offset + length].decode('utf-8')
        self._offset += length
        return value
    
    def read_bytes(self) -> bytes:
        """Read length-prefixed bytes"""
        length = self.read_unsigned()
        if self._offset + length > len(self._data):
            raise ValueError("Bytes length exceeds available data")
        value = self._data[self._offset:self._offset + length]
        self._offset += length
        return value
    
    def read_float(self) -> float:
        """Read float (8 bytes double precision)"""
        import struct
        if self._offset + 8 > len(self._data):
            raise ValueError("Not enough data for float")
        value = struct.unpack('<d', self._data[self._offset:self._offset + 8])[0]
        self._offset += 8
        return value
    
    def read_bool(self) -> bool:
        """Read boolean (single byte)"""
        if self._offset >= len(self._data):
            raise ValueError("No data available for bool")
        value = self._data[self._offset] != 0
        self._offset += 1
        return value
    
    def remaining(self) -> int:
        """Get number of remaining bytes"""
        return len(self._data) - self._offset
    
    def is_empty(self) -> bool:
        """Check if all data has been read"""
        return self._offset >= len(self._data)
    
    def reset(self):
        """Reset offset to beginning"""
        self._offset = 0


def estimate_leb128_size(value: int, signed: bool = False) -> int:
    """
    Estimate the number of bytes needed to encode a value
    
    Args:
        value: Integer value
        signed: Whether to use signed encoding
        
    Returns:
        Number of bytes needed
    """
    if not signed and value < 0:
        raise ValueError("Negative value requires signed encoding")
    
    if signed:
        # For signed, we need to consider the sign extension
        if value >= 0:
            # Positive number - need room for sign bit
            bits_needed = value.bit_length() + 1
        else:
            # Negative number - count bits in two's complement
            bits_needed = (value + 1).bit_length() + 1
    else:
        # Unsigned - just count bits
        bits_needed = max(1, value.bit_length())
    
    # Each LEB128 byte stores 7 bits
    return (bits_needed + 6) // 7


def compare_encoding_sizes(values: List[int], signed: bool = False) -> dict:
    """
    Compare sizes of different encoding methods
    
    Args:
        values: List of integer values
        signed: Whether values are signed
        
    Returns:
        Dictionary with size comparisons
    """
    import struct
    
    leb128_size = 0
    fixed32_size = len(values) * 4
    fixed64_size = len(values) * 8
    
    for value in values:
        if signed:
            leb128_size += len(encode_signed_leb128(value))
        else:
            if value < 0:
                raise ValueError(f"Negative value {value} requires signed encoding")
            leb128_size += len(encode_leb128(value))
    
    # Calculate compression ratio
    compression_vs_32 = (1 - leb128_size / fixed32_size) * 100 if fixed32_size > 0 else 0
    compression_vs_64 = (1 - leb128_size / fixed64_size) * 100 if fixed64_size > 0 else 0
    
    return {
        'leb128_bytes': leb128_size,
        'fixed32_bytes': fixed32_size,
        'fixed64_bytes': fixed64_size,
        'compression_vs_32': f"{compression_vs_32:.1f}%",
        'compression_vs_64': f"{compression_vs_64:.1f}%",
        'avg_bytes_per_value': leb128_size / len(values) if values else 0
    }


# Optimized batch encoding/decoding
def encode_batch_leb128(values: List[int], signed: bool = False) -> bytes:
    """
    Encode a batch of integers efficiently
    
    Args:
        values: List of integers
        signed: Whether to use signed encoding
        
    Returns:
        Encoded bytes with count prefix
    """
    encoder = LEB128Encoder()
    
    # Write count first
    encoder.write_unsigned(len(values))
    
    # Write all values
    for value in values:
        if signed:
            encoder.write_signed(value)
        else:
            encoder.write_unsigned(value)
    
    return encoder.get_bytes()


def decode_batch_leb128(data: bytes, signed: bool = False) -> List[int]:
    """
    Decode a batch of integers
    
    Args:
        data: Encoded bytes
        signed: Whether values are signed
        
    Returns:
        List of decoded integers
    """
    decoder = LEB128Decoder(data)
    
    # Read count
    count = decoder.read_unsigned()
    
    # Read all values
    values = []
    for _ in range(count):
        if signed:
            values.append(decoder.read_signed())
        else:
            values.append(decoder.read_unsigned())
    
    return values
        
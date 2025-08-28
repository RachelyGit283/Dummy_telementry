# tests/test_leb128.py
"""
Tests for LEB128 encoding
"""

import pytest
from telemetry_generator.formats.leb128 import (
    encode_leb128,
    decode_leb128,
    encode_signed_leb128,
    decode_signed_leb128,
    LEB128Encoder,
    LEB128Decoder,
    estimate_leb128_size,
    compare_encoding_sizes
)


class TestLEB128Encoding:
    """Test LEB128 encoding/decoding"""
    
    @pytest.mark.parametrize("value", [0, 1, 127, 128, 255, 256, 1000, 65535, 1000000])
    def test_unsigned_encoding_roundtrip(self, value):
        """Test unsigned LEB128 encoding and decoding"""
        encoded = encode_leb128(value)
        decoded, consumed = decode_leb128(encoded)
        
        assert decoded == value
        assert consumed == len(encoded)
    
    @pytest.mark.parametrize("value", [-1000, -128, -1, 0, 1, 127, 128, 1000])
    def test_signed_encoding_roundtrip(self, value):
        """Test signed LEB128 encoding and decoding"""
        encoded = encode_signed_leb128(value)
        decoded, consumed = decode_signed_leb128(encoded)
        
        assert decoded == value
        assert consumed == len(encoded)
    
    def test_negative_unsigned_encoding(self):
        """Test that unsigned encoding rejects negative values"""
        with pytest.raises(ValueError, match="Cannot encode negative"):
            encode_leb128(-1)
    
    def test_size_estimation(self):
        """Test LEB128 size estimation"""
        # Small values should use 1 byte
        assert estimate_leb128_size(0) == 1
        assert estimate_leb128_size(127) == 1
        
        # Larger values need more bytes
        assert estimate_leb128_size(128) == 2
        assert estimate_leb128_size(16383) == 2
        assert estimate_leb128_size(16384) == 3
    
    def test_encoder_class(self):
        """Test LEB128Encoder helper class"""
        encoder = LEB128Encoder()
        
        encoder.write_unsigned(100)
        encoder.write_signed(-50)
        encoder.write_string("test")
        encoder.write_float(3.14)
        encoder.write_bool(True)
        
        data = encoder.get_bytes()
        assert len(data) > 0
        
        # Decode
        decoder = LEB128Decoder(data)
        
        assert decoder.read_unsigned() == 100
        assert decoder.read_signed() == -50
        assert decoder.read_string() == "test"
        assert abs(decoder.read_float() - 3.14) < 0.001
        assert decoder.read_bool() == True
    
    def test_compression_comparison(self):
        """Test compression comparison"""
        values = [1, 10, 100, 1000, 10000, 100000]
        
        stats = compare_encoding_sizes(values, signed=False)
        
        assert stats['leb128_bytes'] < stats['fixed32_bytes']
        assert stats['leb128_bytes'] < stats['fixed64_bytes']
        assert float(stats['compression_vs_32'][:-1]) > 0  # Should have compression



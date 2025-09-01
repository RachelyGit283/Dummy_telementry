import unittest
import struct
import zlib
from unittest.mock import Mock, MagicMock
from ..telemetry_generator.binary_packer import BinaryRecordPacker


class TestBinaryRecordPacker(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock processor
        self.mock_processor = Mock()
        self.mock_processor.endianness = 'little'
        self.mock_processor.total_bits = 320
        self.mock_processor.validation = {}
        
        # Sample field definitions
        self.sample_fields = [
            {
                "name": "field1",
                "type": "np.uint32",
                "start_bit": 0,
                "end_bit": 31,
                "bits": 32
            },
            {
                "name": "field2", 
                "type": "np.uint16",
                "start_bit": 32,
                "end_bit": 47,
                "bits": 16
            },
            {
                "name": "string_field",
                "type": "np.bytes_",
                "start_bit": 48,
                "end_bit": 111,
                "bits": 64
            },
            {
                "name": "enum_field",
                "type": "np.uint8",
                "start_bit": 112,
                "end_bit": 119,
                "bits": 8,
                "enum": {"0": "VALUE_A", "1": "VALUE_B", "2": "VALUE_C"}
            }
        ]
        
        self.mock_processor.fields = self.sample_fields
        self.mock_processor.fields_by_name = {
            field["name"]: field for field in self.sample_fields
        }
        
        self.packer = BinaryRecordPacker(self.mock_processor)
    
    def test_initialization_little_endian(self):
        """Test packer initialization with little endian"""
        self.assertEqual(self.packer.endian_prefix, '<')
    
    def test_initialization_big_endian(self):
        """Test packer initialization with big endian"""
        self.mock_processor.endianness = 'big'
        packer = BinaryRecordPacker(self.mock_processor)
        self.assertEqual(packer.endian_prefix, '>')
    
    def test_pack_record_basic(self):
        """Test basic record packing"""
        data = {
            "field1": 0x12345678,
            "field2": 0xABCD,
            "string_field": "hello",
            "enum_field": "VALUE_B"
        }
        
        result = self.packer.pack_record(data)
        
        # Check that result is bytes and has expected length
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 40)  # 320 bits = 40 bytes
        
        # Verify the packed data
        # Field1 (32-bit uint at start, little endian)
        field1_value = struct.unpack('<I', result[0:4])[0]
        self.assertEqual(field1_value, 0x12345678)
        
        # Field2 (16-bit uint at byte 4-5, little endian)
        field2_value = struct.unpack('<H', result[4:6])[0]
        self.assertEqual(field2_value, 0xABCD)
        
        # String field (8 bytes starting at byte 6)
        string_value = result[6:14].rstrip(b'\x00').decode('ascii')
        self.assertEqual(string_value, "hello")
    
    def test_pack_record_with_defaults(self):
        """Test packing with missing fields (should use defaults)"""
        data = {"field1": 100}  # Only provide one field
        
        result = self.packer.pack_record(data)
        
        # Should not raise exception and return proper length
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 40)
        
        # Field1 should have the provided value
        field1_value = struct.unpack('<I', result[0:4])[0]
        self.assertEqual(field1_value, 100)
        
        # Other fields should be zero/empty
        field2_value = struct.unpack('<H', result[4:6])[0]
        self.assertEqual(field2_value, 0)
    
    def test_pack_enum_field_by_string(self):
        """Test packing enum field using string value"""
        data = {"enum_field": "VALUE_C"}
        
        result = self.packer.pack_record(data)
        
        # Enum field is at bit 112 (byte 14)
        enum_value = result[14]
        self.assertEqual(enum_value, 2)  # "VALUE_C" maps to key "2"
    
    def test_pack_enum_field_invalid_string(self):
        """Test packing enum field with invalid string (should default to 0)"""
        data = {"enum_field": "INVALID_VALUE"}
        
        result = self.packer.pack_record(data)
        
        # Should default to 0
        enum_value = result[14]
        self.assertEqual(enum_value, 0)
    
    def test_pack_bytes_field_different_types(self):
        """Test packing bytes field with different input types"""
        test_cases = [
            ("hello", b"hello\x00\x00\x00"),  # string
            (b"world", b"world\x00\x00\x00"),  # bytes
            (bytearray(b"test"), b"test\x00\x00\x00\x00"),  # bytearray
            (12345, b"12345\x00\x00\x00"),  # number (converted to string)
        ]
        
        for input_value, expected_start in test_cases:
            with self.subTest(input_value=input_value):
                data = {"string_field": input_value}
                result = self.packer.pack_record(data)
                
                # String field starts at byte 6, 8 bytes long
                actual = result[6:14]
                self.assertEqual(actual, expected_start)
    
    def test_pack_float_fields(self):
        """Test packing float fields"""
        # Add float field to mock
        float_field = {
            "name": "float_field",
            "type": "np.float32", 
            "start_bit": 120,
            "end_bit": 151,
            "bits": 32
        }
        self.mock_processor.fields.append(float_field)
        self.mock_processor.fields_by_name["float_field"] = float_field
        
        data = {"float_field": 3.14159}
        result = self.packer.pack_record(data)
        
        # Float should be converted to its bit representation
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 40)
    
    def test_pack_negative_integers(self):
        """Test packing negative integers"""
        # Add signed int field
        signed_field = {
            "name": "signed_field",
            "type": "np.int16",
            "start_bit": 120,
            "end_bit": 135,
            "bits": 16
        }
        self.mock_processor.fields.append(signed_field)
        self.mock_processor.fields_by_name["signed_field"] = signed_field
        
        data = {"signed_field": -100}
        result = self.packer.pack_record(data)
        
        self.assertIsInstance(result, bytes)
        # Negative number should be handled with two's complement
    
    def test_pack_with_crc(self):
        """Test packing with CRC validation"""
        # Add CRC field and validation
        crc_field = {
            "name": "crc32",
            "type": "np.uint32",
            "start_bit": 280,
            "end_bit": 311,
            "bits": 32
        }
        self.mock_processor.fields.append(crc_field)
        self.mock_processor.fields_by_name["crc32"] = crc_field
        
        self.mock_processor.validation = {
            "crc32c": {
                "field": "crc32",
                "range_bits": "0-279"
            }
        }
        
        data = {"field1": 0x12345678, "field2": 0xABCD}
        result = self.packer.pack_record(data)
        
        self.assertIsInstance(result, bytes)
        # CRC should be calculated and inserted
        crc_bytes = result[35:39]  # CRC at bit 280 = byte 35
        crc_value = struct.unpack('<I', crc_bytes)[0]
        self.assertNotEqual(crc_value, 0)  # Should have calculated CRC
    
    def test_bit_aligned_fields(self):
        """Test packing of bit-aligned fields (not byte-aligned)"""
        # Add bit-aligned field
        bit_field = {
            "name": "bit_field",
            "type": "np.uint8",
            "start_bit": 5,  # Not byte-aligned
            "end_bit": 9,    # 5 bits
            "bits": 5
        }
        
        self.mock_processor.fields = [bit_field]
        self.mock_processor.fields_by_name = {"bit_field": bit_field}
        
        data = {"bit_field": 0x1F}  # 5 bits all set
        result = self.packer.pack_record(data)
        
        self.assertIsInstance(result, bytes)
        # Should handle bit-level packing correctly
    
    def test_big_endian_packing(self):
        """Test packing with big endian byte order"""
        self.mock_processor.endianness = 'big'
        packer = BinaryRecordPacker(self.mock_processor)
        
        data = {"field1": 0x12345678}
        result = packer.pack_record(data)
        
        # In big endian, bytes should be in reverse order
        field1_value = struct.unpack('>I', result[0:4])[0]
        self.assertEqual(field1_value, 0x12345678)
    
    def test_invalid_numeric_values(self):
        """Test handling of invalid numeric values"""
        data = {"field1": "not_a_number", "field2": None}
        
        # Should not raise exception, should use defaults
        result = self.packer.pack_record(data)
        self.assertIsInstance(result, bytes)
    
    def test_buffer_bounds_checking(self):
        """Test that buffer bounds are respected"""
        # Create a scenario where we might write beyond buffer
        oversized_data = {"string_field": "a" * 100}  # Much larger than 8 bytes
        
        result = self.packer.pack_record(oversized_data)
        
        # Should truncate to fit and not crash
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 40)
    
    def test_empty_data(self):
        """Test packing with empty data dictionary"""
        result = self.packer.pack_record({})
        
        # Should return zero-filled buffer
        self.assertIsInstance(result, bytes)
        self.assertEqual(len(result), 40)
        # Check that most of the buffer is zeros (allowing for some enum defaults)
        zero_count = result.count(b'\x00'[0])
        self.assertGreater(zero_count, 35)  # Most bytes should be zero
    
    def test_exception_handling_in_pack_record(self):
        """Test exception handling in main pack_record method"""
        # Force an exception by making fields invalid
        self.mock_processor.fields = [{"invalid": "field"}]  # Missing required keys
        
        with self.assertRaises(Exception):
            self.packer.pack_record({"field1": 100})
    
    def test_exception_handling_in_pack_field(self):
        """Test exception handling in _pack_field method"""
        # Create invalid field definition
        invalid_field = {
            "name": "invalid",
            "type": "invalid_type",
            "start_bit": "invalid",  # Should be int
            "end_bit": 31,
            "bits": 32
        }
        
        buffer = bytearray(40)
        
        with self.assertRaises(Exception):
            self.packer._pack_field(buffer, invalid_field, 100)
    
    def test_write_bytes_bounds(self):
        """Test _write_bytes with boundary conditions"""
        buffer = bytearray(10)
        
        # Normal case
        self.packer._write_bytes(buffer, 0, 4, 0x12345678)
        
        # Near boundary
        self.packer._write_bytes(buffer, 6, 4, 0xABCDEF00)
        
        # Should handle gracefully without crashing
        self.assertEqual(len(buffer), 10)
    
    def test_write_bits_bounds(self):
        """Test _write_bits with boundary conditions"""
        buffer = bytearray(10)
        
        # Normal case
        self.packer._write_bits(buffer, 0, 8, 0xFF)
        
        # Bit-aligned case
        self.packer._write_bits(buffer, 3, 5, 0x1F)
        
        # Should handle gracefully
        self.assertEqual(len(buffer), 10)


class TestBinaryRecordPackerIntegration(unittest.TestCase):
    """Integration tests with more realistic scenarios"""
    
    def setUp(self):
        """Set up realistic test scenario"""
        self.processor = Mock()
        self.processor.endianness = 'little'
        self.processor.total_bits = 256  # 32 bytes
        self.processor.validation = {}
        
        # Realistic protocol fields
        self.processor.fields = [
            {"name": "header", "type": "np.uint16", "start_bit": 0, "end_bit": 15, "bits": 16},
            {"name": "version", "type": "np.uint8", "start_bit": 16, "end_bit": 23, "bits": 8},
            {"name": "msg_type", "type": "np.uint8", "start_bit": 24, "end_bit": 31, "bits": 8,
             "enum": {"1": "DATA", "2": "ACK", "3": "NACK"}},
            {"name": "payload_len", "type": "np.uint16", "start_bit": 32, "end_bit": 47, "bits": 16},
            {"name": "timestamp", "type": "np.uint64", "start_bit": 48, "end_bit": 111, "bits": 64},
            {"name": "data", "type": "np.bytes_", "start_bit": 112, "end_bit": 239, "bits": 128},  # 16 bytes
            {"name": "checksum", "type": "np.uint16", "start_bit": 240, "end_bit": 255, "bits": 16}
        ]
        
        self.processor.fields_by_name = {f["name"]: f for f in self.processor.fields}
        
        self.packer = BinaryRecordPacker(self.processor)
    
    def test_realistic_protocol_packing(self):
        """Test packing a realistic protocol message"""
        data = {
            "header": 0xCAFE,
            "version": 1,
            "msg_type": "DATA",
            "payload_len": 1024,
            "timestamp": 1234567890123456789,
            "data": "Hello World!",
            "checksum": 0x1234
        }
        
        result = self.packer.pack_record(data)
        
        self.assertEqual(len(result), 32)  # 256 bits = 32 bytes
        
        # Verify header
        header = struct.unpack('<H', result[0:2])[0]
        self.assertEqual(header, 0xCAFE)
        
        # Verify version
        self.assertEqual(result[2], 1)
        
        # Verify msg_type (enum)
        self.assertEqual(result[3], 1)  # "DATA" -> "1"
        
        # Verify payload_len
        payload_len = struct.unpack('<H', result[4:6])[0]
        self.assertEqual(payload_len, 1024)
        
        # Verify timestamp
        timestamp = struct.unpack('<Q', result[6:14])[0]
        self.assertEqual(timestamp, 1234567890123456789)
        
        # Verify data field
        data_field = result[14:30].rstrip(b'\x00').decode('ascii')
        self.assertEqual(data_field, "Hello World!")
        
        # Verify checksum
        checksum = struct.unpack('<H', result[30:32])[0]
        self.assertEqual(checksum, 0x1234)


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)
    
    unittest.main()
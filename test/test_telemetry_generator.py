#!/usr/bin/env python3
"""
Comprehensive test suite for the telemetry generator system
Run with: python -m pytest test_telemetry_generator.py -v
"""

import pytest
import json
import os
import tempfile
import shutil
import struct
import time
import math
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# Import modules to test
import sys
sys.path.append('.')  # Add current directory to path

from telemetry_generator import EnhancedTelemetryGeneratorPro
from ..telemetry_generator.binary_schema import BinarySchemaProcessor
from ..telemetry_generator.binary_packer import BinaryRecordPacker
from telemetry_generator.types_and_enums import RecordType, OutputFormat, TelemetryRecord
from ..telemetry_generator.data_generators import FieldDataGenerator, RecordDataPopulator
from ..telemetry_generator.formatters import OutputFormatter
from ..telemetry_generator.utilities import TelemetryUtilities, BenchmarkRunner
from ..telemetry_generator.gpu_accelerator import GPUAcceleratedGenerator


class TestBinarySchemaProcessor:
    """Test binary schema processing functionality"""
    
    @pytest.fixture
    def sample_binary_schema(self):
        """Sample binary schema for testing"""
        return {
            "schema_name": "TestTelemetry",
            "endianness": "little",
            "total_bits": 338,
            "validation": {
                "crc32c": {
                    "field": "crc32c",
                    "range_bits": "0-287"
                }
            },
            "schema_version": {
                "type": "np.uint8",
                "bits": 8,
                "pos": "0-7",
                "desc": "Schema version"
            },
            "device_id_ascii": {
                "type": "np.bytes_",
                "bits": 64,
                "pos": "8-71",
                "desc": "Device ID (8 ASCII chars)"
            },
            "gpu_index": {
                "type": "np.uint8",
                "bits": 4,
                "pos": "72-75",
                "desc": "GPU index (0-15)"
            },
            "seq_no": {
                "type": "np.uint64",
                "bits": 32,
                "pos": "76-107",
                "desc": "Sequence number"
            },
            "timestamp_ns": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "108-171",
                "desc": "Timestamp in nanoseconds"
            },
            "scope": {
                "type": "np.uint8",
                "bits": 8,
                "pos": "172-179",
                "desc": "Measurement scope",
                "enum": {
                    "0": "global",
                    "1": "device",
                    "2": "kernel",
                    "3": "block"
                }
            },
            "block_id": {
                "type": "np.uint16",
                "bits": 16,
                "pos": "180-195",
                "desc": "Block ID (0xFFFF = N/A)"
            },
            "thread_id": {
                "type": "np.uint16",
                "bits": 16,
                "pos": "196-211",
                "desc": "Thread ID (0xFFFF = N/A)"
            },
            "metric_id": {
                "type": "np.uint16",
                "bits": 12,
                "pos": "212-223",
                "desc": "Metric identifier"
            },
            "value_type": {
                "type": "np.uint8",
                "bits": 2,
                "pos": "224-225",
                "desc": "Value data type",
                "enum": {
                    "0": "float32",
                    "1": "uint64",
                    "2": "int64",
                    "3": "bool"
                }
            },
            "value_bits": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "226-289",
                "desc": "Value data (interpreted by value_type)"
            },
            "unit_code": {
                "type": "np.uint8",
                "bits": 8,
                "pos": "290-297",
                "desc": "Unit code"
            },
            "scale_1eN": {
                "type": "np.int8",
                "bits": 8,
                "pos": "298-305",
                "desc": "Scale factor (power of 10)"
            },
            "crc32c": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "306-337",
                "desc": "CRC32C checksum"
            }
        }
    
    @pytest.fixture
    def invalid_schema(self):
        """Invalid schema for testing error handling"""
        return {
            "schema_name": "InvalidSchema",
            # Missing endianness
            "total_bits": 64,
            "field1": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "0-31"
            },
            "field2": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "30-61"  # Overlapping with field1
            }
        }
    
    def test_schema_processor_initialization(self, sample_binary_schema):
        """Test schema processor initialization"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        
        assert processor.schema_name == "TestTelemetry"
        assert processor.endianness == "little"
        assert processor.total_bits == 338
        assert len(processor.fields) > 0
        assert "crc32c" in processor.validation
    
    def test_schema_validation_overlap_detection(self, invalid_schema):
        """Test schema validation catches overlapping fields"""
        with pytest.raises(ValueError, match="Bit overlap"):
            BinarySchemaProcessor(invalid_schema)
    
    def test_schema_field_parsing(self, sample_binary_schema):
        """Test field parsing and bit position calculation"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        
        # Find device_id_ascii field
        device_field = next(f for f in processor.fields if f["name"] == "device_id_ascii")
        assert device_field["start_bit"] == 8
        assert device_field["end_bit"] == 71
        assert device_field["bits"] == 64
        assert device_field["type"] == "np.bytes_"
    
    def test_fields_sorted_by_position(self, sample_binary_schema):
        """Test that fields are sorted by bit position"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        
        for i in range(len(processor.fields) - 1):
            current = processor.fields[i]
            next_field = processor.fields[i + 1]
            assert current["start_bit"] <= next_field["start_bit"]


class TestBinaryRecordPacker:
    """Test binary record packing functionality"""
    
    @pytest.fixture
    def processor_and_packer(self, sample_binary_schema):
        """Create processor and packer for testing"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        packer = BinaryRecordPacker(processor)
        return processor, packer
    
    @pytest.fixture
    def sample_data(self):
        """Sample record data for testing"""
        return {
            "schema_version": 1,
            "device_id_ascii": "TESTDEV1",
            "gpu_index": 2,
            "seq_no": 12345,
            "timestamp_ns": 1234567890123456789,
            "scope": "device",
            "block_id": 42,
            "thread_id": 128,
            "metric_id": 999,
            "value_type": "float32",
            "value_bits": struct.unpack('I', struct.pack('f', 42.5))[0],
            "unit_code": 100,
            "scale_1eN": -3,
            "crc32c": 0  # Will be calculated
        }
    
    def test_record_packing_size(self, processor_and_packer, sample_data):
        """Test that packed records have correct size"""
        processor, packer = processor_and_packer
        packed = packer.pack_record(sample_data)
        
        expected_size = math.ceil(processor.total_bits / 8)
        assert len(packed) == expected_size
    
    def test_field_packing_integrity(self, processor_and_packer, sample_data):
        """Test individual field packing"""
        processor, packer = processor_and_packer
        packed = packer.pack_record(sample_data)
        
        # Test schema version (first byte)
        assert packed[0] == 1
        
        # Test device ID (bytes 1-8, ASCII)
        device_id_bytes = packed[1:9]
        assert device_id_bytes.decode('ascii').rstrip('\x00') == "TESTDEV1"
    
    def test_enum_handling(self, processor_and_packer):
        """Test enum value handling in packing"""
        processor, packer = processor_and_packer
        
        data = {
            "scope": "kernel",  # String enum value
            "value_type": "uint64"  # String enum value
        }
        
        # Should not raise exception
        packed = packer.pack_record(data)
        assert len(packed) == math.ceil(processor.total_bits / 8)
    
    def test_crc_calculation(self, processor_and_packer, sample_data):
        """Test CRC32C calculation"""
        processor, packer = processor_and_packer
        
        # Pack record twice - CRC should be consistent
        packed1 = packer.pack_record(sample_data)
        packed2 = packer.pack_record(sample_data)
        
        # Extract CRC field (last 4 bytes in little endian)
        crc1 = struct.unpack('<I', packed1[-4:])[0]
        crc2 = struct.unpack('<I', packed2[-4:])[0]
        
        assert crc1 == crc2
        assert crc1 != 0  # Should have calculated a CRC


class TestFieldDataGenerator:
    """Test field data generation"""
    
    def test_device_id_generation(self):
        """Test device ID generation"""
        device_id = FieldDataGenerator.generate_device_id()
        
        assert len(device_id) == 8
        assert device_id.isalnum()
        assert device_id.isupper() or device_id.isdigit()
    
    def test_enum_value_generation(self):
        """Test enum value generation"""
        field = {
            "enum": {
                "0": "global",
                "1": "device",
                "2": "kernel"
            }
        }
        
        value = FieldDataGenerator.generate_enum_value(field)
        assert value in ["global", "device", "kernel"]
    
    def test_generic_field_values(self):
        """Test generic field value generation"""
        # Test unsigned int
        uint_field = {"type": "np.uint32", "bits": 32}
        value = FieldDataGenerator.generate_generic_field_value(uint_field)
        assert 0 <= value <= (1 << 32) - 1
        
        # Test signed int
        int_field = {"type": "np.int16", "bits": 16}
        value = FieldDataGenerator.generate_generic_field_value(int_field)
        assert -(1 << 15) <= value <= (1 << 15) - 1
        
        # Test float
        float_field = {"type": "np.float32", "bits": 32}
        value = FieldDataGenerator.generate_generic_field_value(float_field)
        assert isinstance(value, (int, float))


class TestRecordDataPopulator:
    """Test record data population"""
    
    @pytest.fixture
    def processor_and_populator(self, sample_binary_schema):
        """Create processor and populator"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        populator = RecordDataPopulator(processor)
        return processor, populator
    
    def test_record_population(self, processor_and_populator):
        """Test complete record population"""
        processor, populator = processor_and_populator
        
        seq_id = 12345
        timestamp = 1234567890123456789
        
        data = populator.populate_record_data(seq_id, timestamp)
        
        assert data["seq_no"] == seq_id
        assert data["timestamp_ns"] == timestamp
        assert "device_id_ascii" in data
        assert len(data) == len(processor.fields)
    
    def test_populated_data_types(self, processor_and_populator):
        """Test that populated data has correct types"""
        processor, populator = processor_and_populator
        
        data = populator.populate_record_data(1, 123456789)
        
        # Check specific field types
        assert isinstance(data["schema_version"], int)
        assert isinstance(data["device_id_ascii"], str)
        assert isinstance(data["gpu_index"], int)
        assert data["gpu_index"] <= 15  # Should respect field constraints


class TestOutputFormatter:
    """Test output formatting"""
    
    @pytest.fixture
    def formatter_and_record(self):
        """Create formatter and sample record"""
        formatter = OutputFormatter("TestSchema")
        record = TelemetryRecord(
            record_type=RecordType.UPDATE,
            timestamp=1234567890,
            sequence_id=42,
            data={"field1": "value1", "field2": 123}
        )
        return formatter, record
    
    def test_json_formatting(self, formatter_and_record):
        """Test JSON formatting"""
        formatter, record = formatter_and_record
        
        json_str = formatter.format_json(record)
        parsed = json.loads(json_str)
        
        assert parsed["schema"] == "TestSchema"
        assert parsed["type"] == "update"
        assert parsed["timestamp"] == 1234567890
        assert parsed["sequence_id"] == 42
        assert parsed["data"]["field1"] == "value1"
    
    def test_ndjson_formatting(self, formatter_and_record):
        """Test NDJSON formatting"""
        formatter, record = formatter_and_record
        
        ndjson_str = formatter.format_ndjson(record)
        
        # Should end with newline
        assert ndjson_str.endswith('\n')
        
        # Should be valid JSON when newline is stripped
        parsed = json.loads(ndjson_str.strip())
        assert parsed["schema"] == "TestSchema"
    
    def test_influx_line_formatting(self, formatter_and_record):
        """Test InfluxDB line protocol formatting"""
        formatter, record = formatter_and_record
        
        influx_str = formatter.format_influx_line(record, "test_measurement")
        
        # Should contain measurement name
        assert influx_str.startswith("test_measurement")
        
        # Should end with newline
        assert influx_str.endswith('\n')
        
        # Should contain tags and fields
        assert "schema=TestSchema" in influx_str
        assert "field1=" in influx_str


class TestGPUAccelerator:
    """Test GPU acceleration (with mocking)"""
    
    def test_gpu_initialization_without_cupy(self):
        """Test GPU initialization when CuPy is not available"""
        with patch('telemetry_generator.gpu_accelerator.HAS_CUPY', False):
            gpu_gen = GPUAcceleratedGenerator(use_gpu=True)
            assert gpu_gen.use_gpu is False
            assert gpu_gen.xp is not None  # Should fall back to numpy or None
    
    def test_batch_int_generation_fallback(self):
        """Test integer batch generation fallback"""
        gpu_gen = GPUAcceleratedGenerator(use_gpu=False)
        
        batch = gpu_gen.generate_batch_int(10, 5, 16)
        
        assert len(batch) == 10  # batch_size
        assert all(len(row) == 5 for row in batch)  # num_fields
        assert all(0 <= val <= (1 << 16) - 1 for row in batch for val in row)
    
    def test_batch_float_generation_fallback(self):
        """Test float batch generation fallback"""
        gpu_gen = GPUAcceleratedGenerator(use_gpu=False)
        
        batch = gpu_gen.generate_batch_float(10, 3)
        
        assert len(batch) == 10
        assert all(len(row) == 3 for row in batch)
        assert all(0.0 <= val <= 1.0 for row in batch for val in row)


class TestTelemetryUtilities:
    """Test utility functions"""
    
    @pytest.fixture
    def utilities(self, sample_binary_schema):
        """Create utilities with sample schema"""
        processor = BinarySchemaProcessor(sample_binary_schema)
        return TelemetryUtilities(processor)
    
    def test_storage_estimation(self, utilities):
        """Test storage requirement estimation"""
        estimate = utilities.estimate_storage_requirements(
            num_records=1000,
            output_format=OutputFormat.BINARY,
            compression_ratio=0.5
        )
        
        assert estimate["records"] == 1000
        assert estimate["format"] == "binary"
        assert estimate["compressed_bytes"] < estimate["uncompressed_bytes"]
        assert estimate["compression_ratio"] == 0.5
    
    def test_schema_info(self, utilities):
        """Test schema information extraction"""
        info = utilities.get_schema_info()
        
        assert info["schema_name"] == "TestTelemetry"
        assert info["endianness"] == "little"
        assert info["total_bits"] == 338
        assert info["total_bytes"] == 43  # 320 bits = 40 bytes
        assert isinstance(info["fields"], list)
        assert len(info["fields"]) > 0


class TestEnhancedTelemetryGeneratorPro:
    """Test main generator class"""
    
    @pytest.fixture
    def temp_schema_file(self, sample_binary_schema):
        """Create temporary schema file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_binary_schema, f)
            return f.name
    
    @pytest.fixture
    def generator(self, temp_schema_file):
        """Create generator instance"""
        return EnhancedTelemetryGeneratorPro(schema_file=temp_schema_file)
    
    def test_generator_initialization(self, generator):
        """Test generator initialization"""
        assert generator.schema_processor is not None
        assert generator.record_packer is not None
        assert generator.data_populator is not None
        assert generator.formatter is not None
        assert generator._next_seq_id == 1
    
    def test_record_generation(self, generator):
        """Test record generation"""
        record = generator.generate_enhanced_record()
        # print(type(record))  # <--- הוסף שורה זו
        # print(record) 
        assert isinstance(record, TelemetryRecord)
        assert record.record_type == RecordType.UPDATE
        assert record.timestamp > 0
        assert record.sequence_id >= 1
        assert isinstance(record.data, dict)
        assert len(record.data) > 0
    
    def test_record_packing(self, generator):
        """Test record packing to binary"""
        record = generator.generate_enhanced_record()
        packed = generator.pack_record_enhanced(record)
        
        assert isinstance(packed, bytes)
        expected_size = math.ceil(generator.schema_processor.total_bits / 8)
        assert len(packed) == expected_size
    
    def test_sequential_ids(self, generator):
        """Test sequential ID generation"""
        record1 = generator.generate_enhanced_record()
        record2 = generator.generate_enhanced_record()
        
        assert record2.sequence_id == record1.sequence_id + 1
    
    def test_json_formatting(self, generator):
        """Test JSON format output"""
        record = generator.generate_enhanced_record()
        json_str = generator.format_json(record)
        
        parsed = json.loads(json_str)
        assert "schema" in parsed
        assert "data" in parsed
    
    def test_schema_info(self, generator):
        """Test schema information retrieval"""
        info = generator.get_enhanced_schema_info()
        
        assert "schema_name" in info
        assert "total_bits" in info
        assert "fields" in info
    
    def test_storage_estimation(self, generator):
        """Test storage estimation"""
        estimate = generator.estimate_storage_requirements(1000)
        
        assert estimate["records"] == 1000
        assert "uncompressed_mb" in estimate
        assert "compressed_mb" in estimate
    
    def teardown_method(self, method):
        """Clean up temporary files"""
        # Remove any temporary schema files created during tests
        for file in Path('.').glob('tmp*.json'):
            try:
                file.unlink()
            except:
                pass


class TestCLIIntegration:
    """Test CLI integration and file operations"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def schema_file(self, temp_dir, sample_binary_schema):
        """Create schema file in temp directory"""
        schema_path = os.path.join(temp_dir, "test_schema.json")
        with open(schema_path, 'w') as f:
            json.dump(sample_binary_schema, f)
        return schema_path
    
    @pytest.mark.integration
    def test_file_writing(self, schema_file, temp_dir):
        """Test writing records to file"""
        generator = EnhancedTelemetryGeneratorPro(schema_file=schema_file)
        
        output_path = os.path.join(temp_dir, "test_output.bin")
        
        # Generate a few records and write to file
        with open(output_path, 'wb') as f:
            for _ in range(10):
                record = generator.generate_enhanced_record()
                packed = generator.pack_record_enhanced(record)
                f.write(packed)
        
        # Verify file exists and has expected size
        assert os.path.exists(output_path)
        expected_size = 10 * math.ceil(generator.schema_processor.total_bits / 8)
        assert os.path.getsize(output_path) == expected_size
    
    @pytest.mark.integration
    def test_multiple_formats(self, schema_file, temp_dir):
        """Test generation in multiple formats"""
        generator = EnhancedTelemetryGeneratorPro(schema_file=schema_file)
        record = generator.generate_enhanced_record()
        
        # Test all format methods exist and work
        json_output = generator.format_json(record)
        ndjson_output = generator.format_ndjson(record)
        influx_output = generator.format_influx_line(record)
        binary_output = generator.pack_record_enhanced(record)
        
        assert isinstance(json_output, str)
        assert isinstance(ndjson_output, str)
        assert isinstance(influx_output, str)
        assert isinstance(binary_output, bytes)
        
        # JSON should be parseable
        json.loads(json_output)
        json.loads(ndjson_output.strip())


# Fixtures that need to be at module level
@pytest.fixture
def sample_binary_schema():
    """Sample binary schema for all tests"""
    return {
        "schema_name": "TestTelemetry",
        "endianness": "little", 
        "total_bits": 338,
        "validation": {
            "crc32c": {
                "field": "crc32c",
                "range_bits": "0-287"
            }
        },
        "schema_version": {
            "type": "np.uint8",
            "bits": 8,
            "pos": "0-7",
            "desc": "Schema version"
        },
        "device_id_ascii": {
            "type": "np.bytes_",
            "bits": 64,
            "pos": "8-71", 
            "desc": "Device ID (8 ASCII chars)"
        },
        "gpu_index": {
            "type": "np.uint8",
            "bits": 4,
            "pos": "72-75",
            "desc": "GPU index (0-15)"
        },
        "seq_no": {
            "type": "np.uint64",
            "bits": 32,
            "pos": "76-107",
            "desc": "Sequence number"
        },
        "timestamp_ns": {
            "type": "np.uint64",
            "bits": 64,
            "pos": "108-171",
            "desc": "Timestamp in nanoseconds"
        },
        "scope": {
            "type": "np.uint8",
            "bits": 8,
            "pos": "172-179",
            "desc": "Measurement scope",
            "enum": {
                "0": "global",
                "1": "device", 
                "2": "kernel",
                "3": "block"
            }
        },
        "block_id": {
            "type": "np.uint16",
            "bits": 16,
            "pos": "180-195",
            "desc": "Block ID (0xFFFF = N/A)"
        },
        "thread_id": {
            "type": "np.uint16", 
            "bits": 16,
            "pos": "196-211",
            "desc": "Thread ID (0xFFFF = N/A)"
        },
        "metric_id": {
            "type": "np.uint16",
            "bits": 12,
            "pos": "212-223",
            "desc": "Metric identifier"
        },
        "value_type": {
            "type": "np.uint8",
            "bits": 2,
            "pos": "224-225",
            "desc": "Value data type",
            "enum": {
                "0": "float32",
                "1": "uint64",
                "2": "int64", 
                "3": "bool"
            }
        },
        "value_bits": {
            "type": "np.uint64",
            "bits": 64,
            "pos": "226-289",
            "desc": "Value data (interpreted by value_type)"
        },
        "unit_code": {
            "type": "np.uint8",
            "bits": 8,
            "pos": "290-297",
            "desc": "Unit code"
        },
        "scale_1eN": {
            "type": "np.int8",
            "bits": 8,
            "pos": "298-305", 
            "desc": "Scale factor (power of 10)"
        },
        "crc32c": {
            "type": "np.uint32",
            "bits": 32,
            "pos": "306-337",
            "desc": "CRC32C checksum"
        }
    }


if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v", "--tb=short"])

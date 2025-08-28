# tests/test_generator.py
"""
Tests for the telemetry generator core functionality
"""

import pytest
import json
import tempfile
import os
from pathlib import Path

from telemetry_generator import (
    EnhancedTelemetryGeneratorPro,
    TelemetryRecord,
    RecordType,
    OutputFormat
)


@pytest.fixture
def sample_schema():
    """Sample schema for testing"""
    return {
        "device_id": {"type": "string", "length": 8},
        "temperature": {"type": "float", "min": -40.0, "max": 85.0},
        "status": {"type": "enum", "values": ["active", "idle", "error"]},
        "is_online": {"type": "bool"},
        "counter": {"type": "int", "bits": 16}
    }


@pytest.fixture
def schema_file(sample_schema, tmp_path):
    """Create temporary schema file"""
    schema_path = tmp_path / "test_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(sample_schema, f)
    return str(schema_path)


@pytest.fixture
def generator(schema_file):
    """Create generator instance"""
    return EnhancedTelemetryGeneratorPro(schema_file=schema_file)


class TestGeneratorInitialization:
    """Test generator initialization"""
    
    def test_init_with_file(self, schema_file):
        """Test initialization with schema file"""
        gen = EnhancedTelemetryGeneratorPro(schema_file=schema_file)
        assert gen is not None
        assert len(gen.schema) > 0
    
    def test_init_with_dict(self, sample_schema):
        """Test initialization with schema dictionary"""
        gen = EnhancedTelemetryGeneratorPro(schema_dict=sample_schema)
        assert gen is not None
        assert gen.schema == sample_schema
    
    def test_init_without_schema(self):
        """Test that initialization fails without schema"""
        with pytest.raises(ValueError, match="Either schema_file or schema_dict"):
            EnhancedTelemetryGeneratorPro()
    
    def test_init_with_invalid_file(self):
        """Test initialization with non-existent file"""
        with pytest.raises(FileNotFoundError):
            EnhancedTelemetryGeneratorPro(schema_file="nonexistent.json")
    
    def test_init_with_invalid_schema(self, tmp_path):
        """Test initialization with invalid JSON"""
        bad_schema = tmp_path / "bad.json"
        bad_schema.write_text("not json")
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            EnhancedTelemetryGeneratorPro(schema_file=str(bad_schema))


class TestRecordGeneration:
    """Test record generation"""
    
    def test_generate_single_record(self, generator):
        """Test generating a single record"""
        record = generator.generate_enhanced_record()
        
        assert isinstance(record, TelemetryRecord)
        assert record.record_type in [RecordType.UPDATE, RecordType.EVENT]
        assert record.timestamp > 0
        assert record.sequence_id >= 0
        assert isinstance(record.data, dict)
    
    def test_generate_multiple_records(self, generator):
        """Test generating multiple records"""
        records = [generator.generate_enhanced_record() for _ in range(100)]
        
        assert len(records) == 100
        
        # Check sequence IDs are incrementing
        seq_ids = [r.sequence_id for r in records]
        assert seq_ids == list(range(100))
    
    def test_field_types(self, generator):
        """Test that generated fields match schema types"""
        record = generator.generate_enhanced_record()
        data = record.data
        
        # String field
        assert isinstance(data.get('device_id'), str)
        assert len(data.get('device_id', '')) <= 8
        
        # Float field
        assert isinstance(data.get('temperature'), float)
        assert -40.0 <= data.get('temperature', 0) <= 85.0
        
        # Enum field
        assert data.get('status') in ["active", "idle", "error"]
        
        # Bool field
        assert isinstance(data.get('is_online'), bool)
        
        # Int field
        assert isinstance(data.get('counter'), int)
        assert 0 <= data.get('counter', 0) < (1 << 16)
    
    def test_reproducible_generation(self, schema_file):
        """Test reproducible generation with seed"""
        import random
        
        # Generate with seed
        random.seed(42)
        gen1 = EnhancedTelemetryGeneratorPro(schema_file=schema_file)
        records1 = [gen1.generate_enhanced_record() for _ in range(10)]
        
        # Generate again with same seed
        random.seed(42)
        gen2 = EnhancedTelemetryGeneratorPro(schema_file=schema_file)
        records2 = [gen2.generate_enhanced_record() for _ in range(10)]
        
        # Compare data (not timestamps or seq_ids)
        for r1, r2 in zip(records1, records2):
            assert r1.data == r2.data


class TestOutputFormats:
    """Test different output formats"""
    
    def test_binary_format(self, generator):
        """Test binary format serialization"""
        record = generator.generate_enhanced_record()
        binary = generator.pack_record_enhanced(record)
        
        assert isinstance(binary, bytes)
        assert len(binary) > 0
    
    def test_json_format(self, generator):
        """Test JSON format serialization"""
        record = generator.generate_enhanced_record()
        json_str = generator.format_json(record)
        
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed['type'] in ['update', 'event']
        assert 'timestamp' in parsed
        assert 'sequence_id' in parsed
        assert 'data' in parsed
    
    def test_ndjson_format(self, generator):
        """Test NDJSON format serialization"""
        record = generator.generate_enhanced_record()
        ndjson = generator.format_ndjson(record)
        
        assert isinstance(ndjson, str)
        assert ndjson.endswith('\n')
        
        # Should be valid JSON without the newline
        parsed = json.loads(ndjson.strip())
        assert 'type' in parsed
    
    def test_influx_format(self, generator):
        """Test InfluxDB Line Protocol format"""
        record = generator.generate_enhanced_record()
        influx = generator.format_influx_line(record)
        
        assert isinstance(influx, str)
        assert influx.endswith('\n')
        assert 'telemetry' in influx  # measurement name
        assert str(record.sequence_id) in influx


class TestFileWriting:
    """Test file writing functionality"""
    
    def test_write_records_to_file(self, generator, tmp_path):
        """Test writing records to file"""
        output_file = tmp_path / "test_output.bin"
        
        generator.write_records_enhanced(
            str(output_file),
            num_records=100,
            output_format=OutputFormat.BINARY
        )
        
        assert output_file.exists()
        assert output_file.stat().st_size > 0
    
    def test_write_json_records(self, generator, tmp_path):
        """Test writing JSON records"""
        output_file = tmp_path / "test_output.json"
        
        generator.write_records_enhanced(
            str(output_file),
            num_records=10,
            output_format=OutputFormat.JSON
        )
        
        assert output_file.exists()
        
        # Verify JSON is valid
        with open(output_file, 'r') as f:
            data = json.load(f)
            assert isinstance(data, dict) or isinstance(data, list)
    
    @pytest.mark.parametrize("num_records", [10, 100, 1000])
    def test_write_various_sizes(self, generator, tmp_path, num_records):
        """Test writing various numbers of records"""
        output_file = tmp_path / f"test_{num_records}.bin"
        
        generator.write_records_enhanced(
            str(output_file),
            num_records=num_records,
            output_format=OutputFormat.BINARY
        )
        
        assert output_file.exists()
        assert output_file.stat().st_size > 0


class TestSchemaCompilation:
    """Test schema compilation"""
    
    def test_schema_info(self, generator):
        """Test getting schema information"""
        info = generator.get_enhanced_schema_info()
        
        assert 'fields_count' in info
        assert 'total_bits_per_record' in info
        assert 'bytes_per_record' in info
        assert 'fields' in info
        assert isinstance(info['fields'], list)
    
    def test_storage_estimation(self, generator):
        """Test storage requirements estimation"""
        estimate = generator.estimate_storage_requirements(
            num_records=10000,
            output_format=OutputFormat.BINARY
        )
        
        assert 'records' in estimate
        assert estimate['records'] == 10000
        assert 'uncompressed_bytes' in estimate
        assert 'uncompressed_mb' in estimate
        assert estimate['uncompressed_bytes'] > 0



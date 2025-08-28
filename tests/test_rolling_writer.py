# tests/test_rolling_writer.py
"""
Tests for rolling file writer
"""

import pytest
import os
import json
from pathlib import Path

from telemetry_generator.rolling_writer import RollingFileWriter
from telemetry_generator import TelemetryRecord, RecordType


@pytest.fixture
def sample_record():
    """Create sample telemetry record"""
    return TelemetryRecord(
        record_type=RecordType.UPDATE,
        timestamp=1234567890,
        sequence_id=1,
        data={
            "temperature": 23.5,
            "status": "active",
            "count": 42
        }
    )


class TestRollingFileWriter:
    """Test rolling file writer"""
    
    def test_basic_writing(self, tmp_path, sample_record):
        """Test basic file writing"""
        writer = RollingFileWriter(
            base_path=str(tmp_path / "test"),
            max_size_bytes=1024 * 1024,  # 1MB
            format='ndjson'
        )
        
        writer.write_record(sample_record)
        writer.close()
        
        # Check file was created
        files = list(tmp_path.glob("*.ndjson"))
        assert len(files) == 1
        
        # Verify content
        with open(files[0], 'r') as f:
            line = f.readline()
            data = json.loads(line)
            assert data['seq_id'] == 1
            assert data['data']['temperature'] == 23.5
    
    def test_file_rotation(self, tmp_path, sample_record):
        """Test automatic file rotation"""
        writer = RollingFileWriter(
            base_path=str(tmp_path / "test"),
            max_size_bytes=100,  # Very small for testing
            format='ndjson'
        )
        
        # Write enough records to trigger rotation
        for i in range(10):
            record = TelemetryRecord(
                record_type=RecordType.UPDATE,
                timestamp=1234567890 + i,
                sequence_id=i,
                data={"value": i * 100}
            )
            writer.write_record(record)
        
        writer.close()
        
        # Should have multiple files
        files = list(tmp_path.glob("*.ndjson"))
        assert len(files) > 1
    
    def test_compression(self, tmp_path, sample_record):
        """Test gzip compression"""
        writer = RollingFileWriter(
            base_path=str(tmp_path / "test"),
            max_size_bytes=1024 * 1024,
            format='ndjson',
            compress=True
        )
        
        writer.write_record(sample_record)
        writer.close()
        
        # Check compressed file was created
        files = list(tmp_path.glob("*.ndjson.gz"))
        assert len(files) == 1
        
        # Verify compressed content
        import gzip
        with gzip.open(files[0], 'rt') as f:
            line = f.readline()
            data = json.loads(line)
            assert data['seq_id'] == 1
    
    @pytest.mark.parametrize("format", ['ndjson', 'json', 'binary', 'influx', 'leb128'])
    def test_different_formats(self, tmp_path, sample_record, format):
        """Test different output formats"""
        writer = RollingFileWriter(
            base_path=str(tmp_path / "test"),
            max_size_bytes=1024 * 1024,
            format=format
        )
        
        writer.write_record(sample_record)
        writer.close()
        
        # Check file was created with correct extension
        if format == 'ndjson':
            ext = '*.ndjson'
        elif format == 'json':
            ext = '*.json'
        elif format == 'binary':
            ext = '*.bin'
        elif format == 'influx':
            ext = '*.txt'
        elif format == 'leb128':
            ext = '*.leb128'
        
        files = list(tmp_path.glob(ext))
        assert len(files) == 1
        assert files[0].stat().st_size > 0
    
    def test_statistics(self, tmp_path, sample_record):
        """Test writer statistics"""
        writer = RollingFileWriter(
            base_path=str(tmp_path / "test"),
            max_size_bytes=1024 * 1024,
            format='ndjson'
        )
        
        for i in range(100):
            record = TelemetryRecord(
                record_type=RecordType.UPDATE,
                timestamp=1234567890 + i,
                sequence_id=i,
                data={"value": i}
            )
            writer.write_record(record)
        
        stats = writer.get_stats()
        
        assert stats['total_records'] == 100
        assert stats['files_created'] >= 1
        assert stats['total_bytes'] > 0
        assert stats['average_bytes_per_record'] > 0
        
        writer.close()



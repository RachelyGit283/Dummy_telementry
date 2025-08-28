# tests/test_integration.py
"""
Integration tests for the complete system
"""

import pytest
import json
import time
import gzip
from pathlib import Path
from click.testing import CliRunner
from telemetry_generator.cli import cli

from telemetry_generator import (
    EnhancedTelemetryGeneratorPro,
    RollingFileWriter,
    RateLimiter,
    ProfileManager
)


class TestIntegration:
    """Integration tests"""
    
    def test_end_to_end_generation(self, tmp_path):
        """Test complete end-to-end generation"""
        # Create schema
        schema = {
            "sensor_id": {"type": "string", "length": 6},
            "reading": {"type": "float", "min": 0.0, "max": 100.0},
            "active": {"type": "bool"}
        }
        
        schema_file = tmp_path / "schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema, f)
        
        # Initialize generator
        generator = EnhancedTelemetryGeneratorPro(
            schema_file=str(schema_file),
            output_dir=str(tmp_path)
        )
        
        # Create rolling writer
        writer = RollingFileWriter(
            base_path=str(tmp_path / "telemetry"),
            max_size_bytes=1024 * 1024,
            format='ndjson'
        )
        
        # Set up rate limiter
        limiter = RateLimiter(records_per_second=1000, batch_size=100)
        limiter.start()
        
        # Generate records
        for i in range(1000):
            record = generator.generate_enhanced_record()
            writer.write_record(record, generator)
            
            if i % 100 == 0:
                limiter.wait_if_needed(100)
        
        writer.close()
        
        # Verify output
        files = list(tmp_path.glob("telemetry_*.ndjson"))
        assert len(files) > 0
        
        # Count total records
        total_records = 0
        for file in files:
            with open(file, 'r') as f:
                total_records += sum(1 for _ in f)
        
        assert total_records == 1000
    
    def test_cli_integration(self, tmp_path):
        """Test CLI integration"""
        runner = CliRunner()
        
        # Create schema
        schema = {
            "value": {"type": "int", "bits": 16},
            "name": {"type": "string", "length": 10}
        }
        
        schema_file = tmp_path / "schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema, f)
        
        # Run generation via CLI
        result = runner.invoke(cli, [
            'generate',
            '--schema', str(schema_file),
            '--rate', '500',
            '--duration', '2',
            '--out-dir', str(tmp_path),
            '--rotate-size', '100KB',
            '--format', 'ndjson'
        ])
        
        # Check success
        assert result.exit_code == 0
        assert 'GENERATION COMPLETE' in result.output
        
        # Verify files
        files = list(tmp_path.glob("telemetry_*.ndjson"))
        assert len(files) > 0
    
    def test_with_compression(self, tmp_path):
        """Test generation with compression"""
        schema = {"value": {"type": "int", "bits": 32}}
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        
        writer = RollingFileWriter(
            base_path=str(tmp_path / "compressed"),
            max_size_bytes=50 * 1024,  # 50KB
            format='ndjson',
            compress=True
        )
        
        # Generate records
        for _ in range(1000):
            record = generator.generate_enhanced_record()
            writer.write_record(record, generator)
        
        writer.close()
        
        # Check compressed files exist
        files = list(tmp_path.glob("compressed_*.ndjson.gz"))
        assert len(files) > 0
        
        # Verify can decompress and read
        with gzip.open(files[0], 'rt') as f:
            first_line = f.readline()
            data = json.loads(first_line)
            assert 'seq_id' in data
    
    def test_load_profile_application(self, tmp_path):
        """Test applying load profiles"""
        schema = {"value": {"type": "int", "bits": 32}}
        
        # Get high load profile
        profile = ProfileManager.get_profile('high')
        
        generator = EnhancedTelemetryGeneratorPro(
            schema_dict=schema,
            enable_gpu=profile.use_gpu
        )
        
        writer = RollingFileWriter(
            base_path=str(tmp_path / "high_load"),
            max_size_bytes=100 * 1024 * 1024,  # 100MB
            format='binary'
        )
        
        limiter = RateLimiter(
            records_per_second=profile.rate,
            batch_size=profile.batch_size
        )
        
        # Generate with profile settings
        limiter.start()
        start_time = time.time()
        
        records_generated = 0
        while time.time() - start_time < 1.0:  # Run for 1 second
            batch_size = min(profile.batch_size, profile.rate - records_generated)
            
            for _ in range(batch_size):
                record = generator.generate_enhanced_record()
                writer.write_record(record, generator)
            
            records_generated += batch_size
            limiter.wait_if_needed(batch_size)
        
        writer.close()
        
        # Verify high throughput
        stats = writer.get_stats()
        assert stats['total_records'] >= profile.rate * 0.8  # Allow 20% tolerance
    
    def test_multiple_formats(self, tmp_path):
        """Test generation with multiple formats"""
        schema = {
            "id": {"type": "int", "bits": 32},
            "value": {"type": "float", "min": 0.0, "max": 1.0}
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        
        formats = ['ndjson', 'json', 'binary', 'influx', 'leb128']
        
        for fmt in formats:
            writer = RollingFileWriter(
                base_path=str(tmp_path / f"test_{fmt}"),
                max_size_bytes=10 * 1024,
                format=fmt
            )
            
            # Generate some records
            for _ in range(100):
                record = generator.generate_enhanced_record()
                writer.write_record(record, generator)
            
            writer.close()
            
            # Verify files exist
            pattern = f"test_{fmt}_*"
            files = list(tmp_path.glob(pattern + "*"))
            assert len(files) > 0, f"No files found for format {fmt}"



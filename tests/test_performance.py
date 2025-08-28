# tests/test_performance.py
"""
Performance tests for telemetry generator
"""

import pytest
import time
import psutil
import os
from telemetry_generator import EnhancedTelemetryGeneratorPro


class TestPerformance:
    """Performance benchmarks"""
    
    @pytest.mark.slow
    def test_generation_speed(self, tmp_path):
        """Test generation speed"""
        schema = {
            "value1": {"type": "int", "bits": 32},
            "value2": {"type": "float"},
            "value3": {"type": "bool"}
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        
        # Measure generation speed
        start = time.perf_counter()
        num_records = 10000
        
        for _ in range(num_records):
            generator.generate_enhanced_record()
        
        elapsed = time.perf_counter() - start
        records_per_second = num_records / elapsed
        
        print(f"\nGeneration speed: {records_per_second:.1f} records/sec")
        
        # Should achieve at least 1000 records/sec on modest hardware
        assert records_per_second >= 1000
    
    @pytest.mark.slow
    def test_memory_usage(self, tmp_path):
        """Test memory usage during generation"""
        schema = {
            "data": {"type": "string", "length": 100}
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        
        # Get initial memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Generate many records
        for _ in range(100000):
            generator.generate_enhanced_record()
        
        # Check memory after generation
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"\nMemory increase: {memory_increase:.1f} MB")
        
        # Should not leak excessive memory (less than 500MB for 100k records)
        assert memory_increase < 500
    
    @pytest.mark.slow
    def test_file_writing_speed(self, tmp_path):
        """Test file writing performance"""
        from telemetry_generator import RollingFileWriter
        
        schema = {"value": {"type": "int", "bits": 32}}
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        
        writer = RollingFileWriter(
            base_path=str(tmp_path / "perf_test"),
            max_size_bytes=100 * 1024 * 1024,  # 100MB
            format='binary'  # Most efficient format
        )
        
        start = time.perf_counter()
        num_records = 100000
        
        for _ in range(num_records):
            record = generator.generate_enhanced_record()
            writer.write_record(record, generator)
        
        writer.close()
        
        elapsed = time.perf_counter() - start
        records_per_second = num_records / elapsed
        mb_per_second = writer.total_bytes_written / elapsed / 1024 / 1024
        
        print(f"\nWrite speed: {records_per_second:.1f} records/sec")
        print(f"Throughput: {mb_per_second:.1f} MB/sec")
        
        # Should achieve reasonable throughput
        assert records_per_second >= 10000  # At least 10k records/sec for binary
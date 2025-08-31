# conftest.py - pytest configuration and shared fixtures
"""
Shared test configuration and fixtures for telemetry generator tests
"""

import pytest
import tempfile
import shutil
import json
import os
from pathlib import Path

@pytest.fixture(scope="session")
def test_data_dir():
    """Create a session-scoped temporary directory for test data"""
    temp_dir = tempfile.mkdtemp(prefix="telemetry_tests_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def sample_schema_dict():
    """Complete sample schema for testing"""
    return {
        "schema_name": "TestTelemetryV1",
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
def schema_file(test_data_dir, sample_schema_dict):
    """Create a temporary schema file"""
    schema_path = os.path.join(test_data_dir, "test_schema.json")
    with open(schema_path, 'w') as f:
        json.dump(sample_schema_dict, f, indent=2)
    return schema_path

@pytest.fixture
def invalid_schema_file(test_data_dir):
    """Create an invalid schema for error testing"""
    invalid_schema = {
        "schema_name": "InvalidSchema",
        "endianness": "little",
        "total_bits": 64,
        "field1": {
            "type": "np.uint32",
            "bits": 32,
            "pos": "0-31"
        },
        "field2": {
            "type": "np.uint32",
            "bits": 32,
            "pos": "30-61"  # Overlapping bits
        }
    }
    
    schema_path = os.path.join(test_data_dir, "invalid_schema.json")
    with open(schema_path, 'w') as f:
        json.dump(invalid_schema, f, indent=2)
    return schema_path

# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", 
        "integration: marks tests as integration tests (run with -m integration)"
    )
    config.addinivalue_line(
        "markers",
        "gpu: marks tests that require GPU functionality"
    )
    config.addinivalue_line(
        "markers", 
        "slow: marks tests as slow (run with -m slow)"
    )

def pytest_collection_modifyitems(config, items):
    """Automatically mark certain test types"""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        
        # Mark GPU tests
        if "gpu" in item.name.lower() or "GPU" in item.name:
            item.add_marker(pytest.mark.gpu)
        
        # Mark slow tests based on name patterns
        slow_patterns = ["benchmark", "performance", "multiple_files", "large_batch"]
        if any(pattern in item.name.lower() for pattern in slow_patterns):
            item.add_marker(pytest.mark.slow)
        
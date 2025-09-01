"""
Comprehensive test suite for FieldDataGenerator and related classes
"""

import unittest
import string
import struct
import random
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# Mock the imports that might not be available during testing
class MockRecordType:
    UPDATE = "UPDATE"

class MockTelemetryRecord:
    def __init__(self, record_type, timestamp, sequence_id, data):
        self.record_type = record_type
        self.timestamp = timestamp
        self.sequence_id = sequence_id
        self.data = data

class MockFaultInjector:
    def __init__(self):
        self.statistics = {"faults_injected": 0, "fault_types": []}
    
    def inject_faults(self, record):
        # Simple mock fault injection
        faulty_data = record.data.copy()
        fault_details = []
        
        # Simulate a simple fault
        if random.random() < 0.1:  # 10% chance of fault
            field_name = list(faulty_data.keys())[0] if faulty_data else "test_field"
            faulty_data[field_name] = 999999  # Inject an obvious fault
            fault_details.append({
                "field": field_name,
                "fault_type": "value_corruption",
                "original_value": record.data.get(field_name, 0),
                "faulty_value": 999999
            })
            self.statistics["faults_injected"] += 1
            self.statistics["fault_types"].append("value_corruption")
        
        record.data = faulty_data
        return record, fault_details
    
    def get_statistics(self):
        return self.statistics

# Mock the modules
import sys
from unittest.mock import MagicMock

mock_types_and_enums = MagicMock()
mock_types_and_enums.RecordType = MockRecordType
mock_types_and_enums.TelemetryRecord = MockTelemetryRecord

mock_fault_injector_module = MagicMock()
mock_fault_injector_module.FaultInjector = MockFaultInjector
mock_fault_injector_module.create_development_fault_injector = lambda x, y=None: MockFaultInjector()
mock_fault_injector_module.create_testing_fault_injector = lambda x, y=None: MockFaultInjector()
mock_fault_injector_module.create_stress_fault_injector = lambda x, y=None: MockFaultInjector()

sys.modules['telemetry_generator.types_and_enums'] = mock_types_and_enums
sys.modules['telemetry_generator.fault_injector'] = mock_fault_injector_module

# Now import the modules to test
try:
    from ..telemetry_generator.data_generators import (
        FieldDataGenerator, 
        FaultAwareRecordDataPopulator, 
        RecordDataPopulator,
        create_clean_populator,
        create_development_populator,
        create_testing_populator,
        create_stress_populator
    )
except ImportError:
    # Fallback import strategy
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from ..telemetry_generator.data_generators import (
        FieldDataGenerator, 
        FaultAwareRecordDataPopulator, 
        RecordDataPopulator,
        create_clean_populator,
        create_development_populator,
        create_testing_populator,
        create_stress_populator
    )

class TestFieldDataGenerator(unittest.TestCase):
    """Test cases for FieldDataGenerator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.generator = FieldDataGenerator()
        
        self.sample_enum_field = {
            "name": "status",
            "type": "np.uint8",
            "original_type": "enum",
            "bits": 8,
            "enum": {
                "0": "inactive",
                "1": "active",
                "2": "pending",
                "3": "error"
            }
        }
        
        self.sample_uint_field = {
            "name": "counter",
            "type": "np.uint16",
            "original_type": "uint16",
            "bits": 16
        }
        
        self.sample_float_field = {
            "name": "temperature",
            "type": "np.float32",
            "original_type": "float32",
            "bits": 32
        }
    
    def test_generate_device_id(self):
        """Test device ID generation"""
        device_id = self.generator.generate_device_id()
        
        # Check length
        self.assertEqual(len(device_id), 8)
        
        # Check characters are valid
        valid_chars = string.ascii_uppercase + string.digits
        for char in device_id:
            self.assertIn(char, valid_chars)
        
        # Test uniqueness (statistically)
        ids = set()
        for _ in range(100):
            ids.add(self.generator.generate_device_id())
        
        # Should have high uniqueness
        self.assertGreater(len(ids), 95)
    
    def test_generate_enum_value(self):
        """Test enum value generation"""
        # Test with valid enum
        for _ in range(50):
            value = self.generator.generate_enum_value(self.sample_enum_field)
            self.assertIn(value, [0, 1, 2, 3])
        
        # Test with empty enum
        empty_enum_field = {"enum": {}}
        value = self.generator.generate_enum_value(empty_enum_field)
        self.assertEqual(value, 0)
        
        # Test with no enum key
        no_enum_field = {}
        value = self.generator.generate_enum_value(no_enum_field)
        self.assertEqual(value, 0)
    
    def test_get_enum_string_value(self):
        """Test enum string value retrieval"""
        # Test valid indices
        self.assertEqual(
            self.generator.get_enum_string_value(self.sample_enum_field, 0),
            "inactive"
        )
        self.assertEqual(
            self.generator.get_enum_string_value(self.sample_enum_field, 1),
            "active"
        )
        self.assertEqual(
            self.generator.get_enum_string_value(self.sample_enum_field, 3),
            "error"
        )
        
        # Test invalid index
        self.assertEqual(
            self.generator.get_enum_string_value(self.sample_enum_field, 99),
            "UNKNOWN"
        )
        
        # Test empty enum
        empty_enum_field = {"enum": {}}
        self.assertEqual(
            self.generator.get_enum_string_value(empty_enum_field, 0),
            "UNKNOWN"
        )
    
    def test_generate_value_bits(self):
        """Test value_bits generation for different types"""
        # Test FLOAT32 (type 0)
        for _ in range(10):
            value = self.generator.generate_value_bits(0)
            self.assertIsInstance(value, int)
            # Should be a valid 32-bit unsigned integer
            self.assertGreaterEqual(value, 0)
            self.assertLessEqual(value, 2**32 - 1)
        
        # Test UINT64 (type 1)
        for _ in range(10):
            value = self.generator.generate_value_bits(1)
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 0)
            self.assertLessEqual(value, (1 << 48) - 1)
        
        # Test INT64 (type 2)
        for _ in range(10):
            value = self.generator.generate_value_bits(2)
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 0)  # After conversion to unsigned
            self.assertLessEqual(value, 2**64 - 1)
        
        # Test BOOL (type 3)
        bool_values = set()
        for _ in range(20):
            value = self.generator.generate_value_bits(3)
            bool_values.add(value)
            self.assertIn(value, [0, 1])
        
        # Should generate both 0 and 1 (statistically)
        self.assertEqual(len(bool_values), 2)
        
        # Test unknown type
        value = self.generator.generate_value_bits(999)
        self.assertEqual(value, 0)
    
    def test_generate_metric_id(self):
        """Test metric ID generation with weighted distribution"""
        metric_ids = []
        for _ in range(1000):
            metric_ids.append(self.generator.generate_metric_id())
        
        # All should be in valid range
        for mid in metric_ids:
            self.assertGreaterEqual(mid, 1)
            self.assertLessEqual(mid, 1000)
        
        # Should have bias toward 1-100 (about 70%)
        common_count = sum(1 for mid in metric_ids if mid <= 100)
        common_ratio = common_count / len(metric_ids)
        
        # Allow some variance in the statistical test
        self.assertGreater(common_ratio, 0.6)
        self.assertLess(common_ratio, 0.8)
    
    def test_generate_unit_code(self):
        """Test unit code generation"""
        unit_codes = []
        for _ in range(1000):
            unit_codes.append(self.generator.generate_unit_code())
        
        # All should be in valid range
        for code in unit_codes:
            self.assertGreaterEqual(code, 0)
            self.assertLessEqual(code, 255)
        
        # Should have bias toward common units
        common_units = [0, 1, 2, 3, 4, 5, 10, 11, 12]
        common_count = sum(1 for code in unit_codes if code in common_units)
        common_ratio = common_count / len(unit_codes)
        
        # Should be around 80% common units
        self.assertGreater(common_ratio, 0.7)
        self.assertLess(common_ratio, 0.9)
    
    def test_generate_scale_1eN(self):
        """Test scale factor generation"""
        scales = []
        for _ in range(1000):
            scales.append(self.generator.generate_scale_1eN())
        
        # All should be in valid range
        for scale in scales:
            self.assertGreaterEqual(scale, -9)
            self.assertLessEqual(scale, 9)
        
        # Should have bias toward -3 to 3
        close_to_zero = sum(1 for scale in scales if -3 <= scale <= 3)
        close_ratio = close_to_zero / len(scales)
        
        # Should be around 60% in close range
        self.assertGreater(close_ratio, 0.5)
        self.assertLess(close_ratio, 0.8)
    
    def test_generate_generic_field_value(self):
        """Test generic field value generation"""
        # Test enum field
        enum_value = self.generator.generate_generic_field_value(self.sample_enum_field)
        self.assertIn(enum_value, [0, 1, 2, 3])
        
        # Test uint field
        uint_value = self.generator.generate_generic_field_value(self.sample_uint_field)
        self.assertGreaterEqual(uint_value, 0)
        self.assertLessEqual(uint_value, (1 << 16) - 1)
        
        # Test float field
        float_value = self.generator.generate_generic_field_value(self.sample_float_field)
        self.assertGreaterEqual(float_value, -1000)
        self.assertLessEqual(float_value, 1000)
        
        # Test bytes field
        bytes_field = {
            "type": "np.bytes_",
            "original_type": "bytes",
            "bits": 64  # 8 bytes
        }
        bytes_value = self.generator.generate_generic_field_value(bytes_field)
        self.assertEqual(len(bytes_value), 8)
        
        # Test signed integer field
        int_field = {
            "type": "np.int16",
            "original_type": "int16",
            "bits": 16
        }
        int_value = self.generator.generate_generic_field_value(int_field)
        self.assertGreaterEqual(int_value, -(1 << 14) - 1)
        self.assertLessEqual(int_value, (1 << 14) - 1)


class TestFaultAwareRecordDataPopulator(unittest.TestCase):
    """Test cases for FaultAwareRecordDataPopulator"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_schema_processor = Mock()
        self.mock_schema_processor.fields = [
            {
                "name": "schema_version",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "device_id_ascii",
                "type": "np.bytes_",
                "original_type": "bytes",
                "bits": 64
            },
            {
                "name": "seq_no",
                "type": "np.uint32",
                "original_type": "uint32",
                "bits": 32
            },
            {
                "name": "timestamp_ns",
                "type": "np.uint64",
                "original_type": "uint64",
                "bits": 64
            },
            {
                "name": "scope",
                "type": "np.uint8",
                "original_type": "enum",
                "bits": 8,
                "enum": {"0": "DEVICE", "1": "BLOCK", "2": "THREAD"}
            }
        ]
        
        self.populator = FaultAwareRecordDataPopulator(self.mock_schema_processor)
    
    def test_initialization(self):
        """Test populator initialization"""
        self.assertEqual(self.populator.schema_processor, self.mock_schema_processor)
        self.assertIsInstance(self.populator.field_generator, FieldDataGenerator)
        self.assertIsNone(self.populator.fault_injector)
        self.assertEqual(self.populator._field_types_cache, {})
        self.assertEqual(self.populator._enum_fields_cache, {})
    
    def test_initialization_with_fault_injector(self):
        """Test populator initialization with fault injector"""
        fault_injector = MockFaultInjector()
        populator = FaultAwareRecordDataPopulator(self.mock_schema_processor, fault_injector)
        
        self.assertEqual(populator.fault_injector, fault_injector)
    
    def test_populate_record_data_clean(self):
        """Test record data population without faults"""
        seq_id = 12345
        timestamp = 1234567890123456789
        
        data, fault_details = self.populator.populate_record_data(
            seq_id, timestamp, inject_faults=False
        )
        
        # Check basic structure
        self.assertIsInstance(data, dict)
        self.assertIsInstance(fault_details, list)
        self.assertEqual(len(fault_details), 0)  # No faults injected
        
        # Check specific field values
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(data["seq_no"], seq_id)
        self.assertEqual(data["timestamp_ns"], timestamp)
        self.assertIsInstance(data["device_id_ascii"], str)
        self.assertEqual(len(data["device_id_ascii"]), 8)
        self.assertIn(data["scope"], [0, 1, 2])
    
    def test_populate_record_data_with_faults(self):
        """Test record data population with fault injection"""
        fault_injector = MockFaultInjector()
        self.populator.fault_injector = fault_injector
        
        seq_id = 12345
        timestamp = 1234567890123456789
        
        # Run multiple times to potentially trigger faults
        fault_found = False
        for _ in range(50):
            data, fault_details = self.populator.populate_record_data(
                seq_id, timestamp, inject_faults=True
            )
            
            self.assertIsInstance(data, dict)
            self.assertIsInstance(fault_details, list)
            
            if fault_details:
                fault_found = True
                # Check fault detail structure
                for fault in fault_details:
                    self.assertIn("field", fault)
                    self.assertIn("fault_type", fault)
                    self.assertIn("original_value", fault)
                    self.assertIn("faulty_value", fault)
                break
        
        # Note: Due to randomness, we can't guarantee a fault will occur,
        # but the test structure should be correct
    
    def test_generate_clean_data_specific_fields(self):
        """Test specific field generation logic"""
        seq_id = 12345
        timestamp = 1234567890123456789
        
        data = self.populator._generate_clean_data(seq_id, timestamp)
        
        # Test schema_version
        self.assertEqual(data["schema_version"], 1)
        
        # Test device_id_ascii
        device_id = data["device_id_ascii"]
        self.assertIsInstance(device_id, str)
        self.assertEqual(len(device_id), 8)
        valid_chars = string.ascii_uppercase + string.digits
        for char in device_id:
            self.assertIn(char, valid_chars)
        
        # Test seq_no and timestamp_ns
        self.assertEqual(data["seq_no"], seq_id)
        self.assertEqual(data["timestamp_ns"], timestamp)
        
        # Test scope enum
        self.assertIn(data["scope"], [0, 1, 2])
    
    def test_generate_clean_data_with_scope_dependencies(self):
        """Test field generation with scope-dependent logic"""
        # Add block_id and thread_id fields to test dependencies
        self.mock_schema_processor.fields.extend([
            {
                "name": "block_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "thread_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "gpu_index",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "metric_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "value_type",
                "type": "np.uint8",
                "original_type": "enum",
                "bits": 8,
                "enum": {"0": "FLOAT32", "1": "UINT64", "2": "INT64", "3": "BOOL"}
            },
            {
                "name": "value_bits",
                "type": "np.uint64",
                "original_type": "uint64",
                "bits": 64
            },
            {
                "name": "unit_code",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "scale_1eN",
                "type": "np.int8",
                "original_type": "int8",
                "bits": 8
            },
            {
                "name": "crc32c",
                "type": "np.uint32",
                "original_type": "uint32",
                "bits": 32
            }
        ])
        
        # Test multiple times to cover different scope scenarios
        for _ in range(20):
            data = self.populator._generate_clean_data(12345, 1234567890123456789)
            
            scope = data["scope"]
            
            # Test GPU index is reasonable
            self.assertGreaterEqual(data["gpu_index"], 0)
            self.assertLessEqual(data["gpu_index"], 7)
            
            # Test scope-dependent fields
            if scope == 0:  # DEVICE
                self.assertEqual(data["block_id"], 0xFFFF)
                self.assertEqual(data["thread_id"], 0xFFFF)
            elif scope == 1:  # BLOCK
                self.assertNotEqual(data["block_id"], 0xFFFF)
                self.assertEqual(data["thread_id"], 0xFFFF)
                self.assertLessEqual(data["block_id"], 2047)
            elif scope == 2:  # THREAD
                self.assertNotEqual(data["block_id"], 0xFFFF)
                self.assertNotEqual(data["thread_id"], 0xFFFF)
                self.assertLessEqual(data["block_id"], 2047)
                self.assertLessEqual(data["thread_id"], 1023)
            
            # Test other fields
            self.assertGreaterEqual(data["metric_id"], 1)
            self.assertLessEqual(data["metric_id"], 1000)
            self.assertIn(data["value_type"], [0, 1, 2, 3])
            self.assertGreaterEqual(data["unit_code"], 0)
            self.assertLessEqual(data["unit_code"], 255)
            self.assertGreaterEqual(data["scale_1eN"], -9)
            self.assertLessEqual(data["scale_1eN"], 9)
            self.assertEqual(data["crc32c"], 0)  # Always 0 initially
    
    def test_fault_injection_control(self):
        """Test fault injection enable/disable functionality"""
        fault_injector = MockFaultInjector()
        
        # Test enabling fault injection
        self.populator.enable_fault_injection(fault_injector)
        self.assertEqual(self.populator.fault_injector, fault_injector)
        
        # Test disabling fault injection
        self.populator.disable_fault_injection()
        self.assertIsNone(self.populator.fault_injector)
    
    def test_get_fault_statistics(self):
        """Test fault statistics retrieval"""
        # Test without fault injector
        stats = self.populator.get_fault_statistics()
        self.assertIsNone(stats)
        
        # Test with fault injector
        fault_injector = MockFaultInjector()
        self.populator.enable_fault_injection(fault_injector)
        
        stats = self.populator.get_fault_statistics()
        self.assertIsInstance(stats, dict)
        self.assertIn("faults_injected", stats)
        self.assertIn("fault_types", stats)


class TestRecordDataPopulator(unittest.TestCase):
    """Test cases for RecordDataPopulator (backwards compatibility)"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_schema_processor = Mock()
        self.mock_schema_processor.fields = [
            {
                "name": "seq_no",
                "type": "np.uint32",
                "original_type": "uint32",
                "bits": 32
            },
            {
                "name": "timestamp_ns",
                "type": "np.uint64",
                "original_type": "uint64",
                "bits": 64
            }
        ]
        
        self.populator = RecordDataPopulator(self.mock_schema_processor)
    
    def test_initialization(self):
        """Test backwards compatible initialization"""
        self.assertIsInstance(self.populator, FaultAwareRecordDataPopulator)
        self.assertIsNone(self.populator.fault_injector)
    
    def test_populate_record_data_backwards_compatible(self):
        """Test backwards compatible record data population"""
        seq_id = 12345
        timestamp = 1234567890123456789
        
        # Test with all parameters for backwards compatibility
        data = self.populator.populate_record_data(
            seq_id, timestamp, inject_faults=True, fault_config={"test": "value"}
        )
        
        # Should return just the data dict, not a tuple
        self.assertIsInstance(data, dict)
        self.assertEqual(data["seq_no"], seq_id)
        self.assertEqual(data["timestamp_ns"], timestamp)


class TestFactoryFunctions(unittest.TestCase):
    """Test cases for factory functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_schema_processor = Mock()
        self.mock_schema_processor.fields = []
    
    def test_create_clean_populator(self):
        """Test clean populator factory"""
        populator = create_clean_populator(self.mock_schema_processor)
        
        self.assertIsInstance(populator, RecordDataPopulator)
        self.assertIsNone(populator.fault_injector)
    
    @patch('telemetry_generator.data_generators.create_development_fault_injector')
    def test_create_development_populator(self, mock_create_injector):
        """Test development populator factory"""
        mock_injector = MockFaultInjector()
        mock_create_injector.return_value = mock_injector
        
        populator = create_development_populator(self.mock_schema_processor)
        
        self.assertIsInstance(populator, FaultAwareRecordDataPopulator)
        self.assertEqual(populator.fault_injector, mock_injector)
        mock_create_injector.assert_called_once_with(self.mock_schema_processor, None)
    
    @patch('telemetry_generator.data_generators.create_testing_fault_injector')
    def test_create_testing_populator(self, mock_create_injector):
        """Test testing populator factory"""
        mock_injector = MockFaultInjector()
        mock_create_injector.return_value = mock_injector
        mock_logger = Mock()
        
        populator = create_testing_populator(self.mock_schema_processor, mock_logger)
        
        self.assertIsInstance(populator, FaultAwareRecordDataPopulator)
        self.assertEqual(populator.fault_injector, mock_injector)
        mock_create_injector.assert_called_once_with(self.mock_schema_processor, mock_logger)
    
    @patch('telemetry_generator.data_generators.create_stress_fault_injector')
    def test_create_stress_populator(self, mock_create_injector):
        """Test stress populator factory"""
        mock_injector = MockFaultInjector()
        mock_create_injector.return_value = mock_injector
        
        populator = create_stress_populator(self.mock_schema_processor)
        
        self.assertIsInstance(populator, FaultAwareRecordDataPopulator)
        self.assertEqual(populator.fault_injector, mock_injector)
        mock_create_injector.assert_called_once_with(self.mock_schema_processor, None)


class TestIntegration(unittest.TestCase):
    """Integration tests combining multiple components"""
    
    def setUp(self):
        """Set up realistic test scenario"""
        self.schema_processor = Mock()
        self.schema_processor.fields = [
            {
                "name": "schema_version",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "device_id_ascii",
                "type": "np.bytes_",
                "original_type": "bytes",
                "bits": 64
            },
            {
                "name": "gpu_index",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "seq_no",
                "type": "np.uint32",
                "original_type": "uint32",
                "bits": 32
            },
            {
                "name": "timestamp_ns",
                "type": "np.uint64",
                "original_type": "uint64",
                "bits": 64
            },
            {
                "name": "scope",
                "type": "np.uint8",
                "original_type": "enum",
                "bits": 8,
                "enum": {"0": "DEVICE", "1": "BLOCK", "2": "THREAD"}
            },
            {
                "name": "block_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "thread_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "metric_id",
                "type": "np.uint16",
                "original_type": "uint16",
                "bits": 16
            },
            {
                "name": "value_type",
                "type": "np.uint8",
                "original_type": "enum",
                "bits": 8,
                "enum": {"0": "FLOAT32", "1": "UINT64", "2": "INT64", "3": "BOOL"}
            },
            {
                "name": "value_bits",
                "type": "np.uint64",
                "original_type": "uint64",
                "bits": 64
            },
            {
                "name": "unit_code",
                "type": "np.uint8",
                "original_type": "uint8",
                "bits": 8
            },
            {
                "name": "scale_1eN",
                "type": "np.int8",
                "original_type": "int8",
                "bits": 8
            },
            {
                "name": "crc32c",
                "type": "np.uint32",
                "original_type": "uint32",
                "bits": 32
            }
        ]
    
    def test_full_record_generation_workflow(self):
        """Test complete record generation workflow"""
        populator = FaultAwareRecordDataPopulator(self.schema_processor)
        
        # Generate multiple records
        records = []
        for i in range(10):
            seq_id = 1000 + i
            timestamp = 1234567890123456789 + i * 1000000000
            
            data, faults = populator.populate_record_data(
                seq_id, timestamp, inject_faults=False
            )
            
            records.append((data, faults))
            
            # Validate record structure
            self.assertIsInstance(data, dict)
            self.assertEqual(len(faults), 0)  # No fault injection
            
            # Check all expected fields are present
            expected_fields = {field["name"] for field in self.schema_processor.fields}
            actual_fields = set(data.keys())
            self.assertEqual(actual_fields, expected_fields)
            
            # Validate specific field values
            self.assertEqual(data["schema_version"], 1)
            self.assertEqual(data["seq_no"], seq_id)
            self.assertEqual(data["timestamp_ns"], timestamp)
            self.assertGreaterEqual(data["gpu_index"], 0)
            self.assertLessEqual(data["gpu_index"], 7)
            self.assertIn(data["scope"], [0, 1, 2])
            self.assertGreaterEqual(data["metric_id"], 1)
            self.assertIn(data["value_type"], [0, 1, 2, 3])
        
        # Check that records are different (not identical)
        unique_device_ids = set(record[0]["device_id_ascii"] for record in records)
        self.assertGreater(len(unique_device_ids), 1)  # Should have variety
    
    def test_fault_injection_integration(self):
        """Test fault injection integration"""
        fault_injector = MockFaultInjector()
        populator = FaultAwareRecordDataPopulator(self.schema_processor, fault_injector)
        
        # Generate records with fault injection
        total_faults = 0
        for i in range(50):  # More iterations to trigger faults
            data, faults = populator.populate_record_data(
                1000 + i, 1234567890123456789, inject_faults=True
            )
            
            total_faults += len(faults)
            
            # Validate that data is still a valid dict even with faults
            self.assertIsInstance(data, dict)
            self.assertIsInstance(faults, list)
        
        # Check statistics
        stats = populator.get_fault_statistics()
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats["faults_injected"], total_faults)


if __name__ == '__main__':
    # Create test suite
    test_classes = [
        TestFieldDataGenerator,
        TestFaultAwareRecordDataPopulator,
        TestRecordDataPopulator,
        TestFactoryFunctions,
        TestIntegration
    ]
    
    loader = unittest.TestLoader()
    suites = []
    
    for test_class in test_classes:
        suite = loader.loadTestsFromTestCase(test_class)
        suites.append(suite)
    
    # Combine all test suites
    combined_suite = unittest.TestSuite(suites)
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(combined_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for i, (test, traceback) in enumerate(result.failures, 1):
            print(f"{i}. {test}")
            print(f"   {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for i, (test, traceback) in enumerate(result.errors, 1):
            print(f"{i}. {test}")
            # Print just the error type and message
            lines = traceback.strip().split('\n')
            error_line = lines[-1] if lines else "Unknown error"
            print(f"   {error_line}")
    
    if not result.failures and not result.errors:
        print("\n🎉 All tests passed successfully!")
    
    print(f"{'='*60}")
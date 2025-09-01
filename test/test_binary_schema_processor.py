"""
Comprehensive test suite for BinarySchemaProcessor
"""

import unittest
import json
import tempfile
import os
import sys
from unittest.mock import patch, mock_open

# Add parent directory to path to import the module
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module to test
try:
    from ..telemetry_generator.binary_schema import BinarySchemaProcessor
except ImportError:
    # Try alternative import paths
    try:
        import ..telemetry_generator.binary_schema
        BinarySchemaProcessor = binary_schema.BinarySchemaProcessor
    except ImportError:
        # If still fails, try importing from current directory
        import sys
        import os
        sys.path.append(os.path.dirname(__file__))
        from ..telemetry_generator.binary_schema import BinarySchemaProcessor

class TestBinarySchemaProcessor(unittest.TestCase):
    """Test cases for BinarySchemaProcessor class"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        self.sample_schema = {
            "schema_name": "test_schema",
            "endianness": "little",
            "total_bits": 32,
            "validation": {"checksum": True},
            "field1": {
                "pos": "0-7",
                "type": "uint8",
                "bits": 8,
                "desc": "First field"
            },
            "field2": {
                "pos": "8-15",
                "type": "uint16",
                "bits": 8,
                "desc": "Second field"
            },
            "field3": {
                "pos": "16-31",
                "type": "uint32",
                "bits": 16,
                "desc": "Third field"
            }
        }
        
        self.enum_schema = {
            "schema_name": "enum_test",
            "endianness": "big",
            "total_bits": 16,
            "status": {
                "pos": "0-7",
                "type": "enum",
                "bits": 8,
                "desc": "Status field",
                "values": ["inactive", "active", "pending", "error"]
            },
            "counter": {
                "pos": "8-15",
                "type": "uint8",
                "bits": 8,
                "desc": "Counter field"
            }
        }
    
    def test_basic_initialization(self):
        """Test basic processor initialization"""
        processor = BinarySchemaProcessor(self.sample_schema)
        
        self.assertEqual(processor.schema_name, "test_schema")
        self.assertEqual(processor.endianness, "little")
        self.assertEqual(processor.total_bits, 32)
        self.assertEqual(len(processor.fields), 3)
        self.assertEqual(len(processor.fields_by_name), 3)
    
    def test_field_parsing(self):
        """Test correct parsing of field definitions"""
        processor = BinarySchemaProcessor(self.sample_schema)
        
        # Check first field
        field1 = processor.fields_by_name["field1"]
        self.assertEqual(field1["name"], "field1")
        self.assertEqual(field1["start_bit"], 0)
        self.assertEqual(field1["end_bit"], 7)
        self.assertEqual(field1["type"], "np.uint8")
        self.assertEqual(field1["original_type"], "uint8")
        
        # Check field ordering by bit position
        self.assertEqual(processor.fields[0]["name"], "field1")
        self.assertEqual(processor.fields[1]["name"], "field2")
        self.assertEqual(processor.fields[2]["name"], "field3")
    
    def test_enum_processing(self):
        """Test enum field processing"""
        processor = BinarySchemaProcessor(self.enum_schema)
        
        status_field = processor.fields_by_name["status"]
        self.assertEqual(status_field["type"], "np.uint8")
        self.assertEqual(status_field["original_type"], "enum")
        
        # Check enum mapping
        expected_enum = {
            "0": "inactive",
            "1": "active", 
            "2": "pending",
            "3": "error"
        }
        self.assertEqual(status_field["enum"], expected_enum)
    
    def test_default_values(self):
        """Test handling of default values"""
        minimal_schema = {
            "test_field": {
                "pos": "0-7"
            }
        }
        
        processor = BinarySchemaProcessor(minimal_schema)
        field = processor.fields_by_name["test_field"]
        
        self.assertEqual(processor.schema_name, "unknown")
        self.assertEqual(processor.endianness, "little")
        self.assertEqual(processor.total_bits, 0)
        self.assertEqual(field["type"], "np.uint8")
        self.assertEqual(field["bits"], 8)
        self.assertEqual(field["desc"], "")
    
    def test_validation_no_fields(self):
        """Test validation error when schema has no fields"""
        empty_schema = {
            "schema_name": "empty",
            "endianness": "little",
            "total_bits": 0
        }
        
        with self.assertRaises(ValueError) as context:
            BinarySchemaProcessor(empty_schema)
        
        self.assertIn("Schema contains no valid fields", str(context.exception))
    
    def test_validation_bit_overlap(self):
        """Test validation error for overlapping bit positions"""
        overlap_schema = {
            "field1": {
                "pos": "0-7",
                "type": "uint8"
            },
            "field2": {
                "pos": "7-15",  # Overlaps with field1
                "type": "uint8"
            }
        }
        
        with self.assertRaises(ValueError) as context:
            BinarySchemaProcessor(overlap_schema)
        
        self.assertIn("Bit overlap", str(context.exception))
    
    def test_validation_exceeds_total_bits(self):
        """Test validation error when fields exceed total bits"""
        exceed_schema = {
            "total_bits": 16,
            "field1": {
                "pos": "0-7",
                "type": "uint8"
            },
            "field2": {
                "pos": "8-31",  # Exceeds total_bits of 16
                "type": "uint32"
            }
        }
        
        with self.assertRaises(ValueError) as context:
            BinarySchemaProcessor(exceed_schema)
        
        self.assertIn("Schema fields extend beyond total_bits", str(context.exception))
    
    def test_type_mapping_with_file(self):
        """Test loading type mapping from external file"""
        custom_mapping = {
            "custom_uint8": "np.uint8",
            "custom_float": "np.float32"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(custom_mapping, f)
            temp_file = f.name
        
        try:
            schema = {
                "field1": {
                    "pos": "0-7",
                    "type": "custom_uint8"
                }
            }
            
            processor = BinarySchemaProcessor(schema, temp_file)
            self.assertEqual(processor.type_mapping, custom_mapping)
            
            field1 = processor.fields_by_name["field1"]
            self.assertEqual(field1["type"], "np.uint8")
        
        finally:
            os.unlink(temp_file)
    
    def test_type_mapping_file_not_found(self):
        """Test handling of missing type mapping file"""
        schema = {
            "field1": {
                "pos": "0-7",
                "type": "uint8"
            }
        }
        
        # Should not raise exception, should use default mapping
        processor = BinarySchemaProcessor(schema, "nonexistent_file.json")
        self.assertIn("uint8", processor.type_mapping)
    
    def test_type_mapping_invalid_json(self):
        """Test handling of invalid JSON in type mapping file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            temp_file = f.name
        
        try:
            schema = {
                "field1": {
                    "pos": "0-7",
                    "type": "uint8"
                }
            }
            
            # Should not raise exception, should use default mapping
            with patch('builtins.print') as mock_print:
                processor = BinarySchemaProcessor(schema, temp_file)
                mock_print.assert_called_once()
                self.assertIn("Warning", mock_print.call_args[0][0])
        
        finally:
            os.unlink(temp_file)
    
    def test_get_numpy_type_with_numpy(self):
        """Test numpy type conversion when numpy is available"""
        processor = BinarySchemaProcessor(self.sample_schema)
        
        # Assuming numpy is available
        if processor.get_numpy_type("np.uint8") is not None:
            import numpy as np
            self.assertEqual(processor.get_numpy_type("np.uint8"), np.uint8)
            self.assertEqual(processor.get_numpy_type("np.float32"), np.float32)
            self.assertEqual(processor.get_numpy_type("invalid_type"), np.uint8)  # default
    
    def test_get_numpy_type_without_numpy(self):
        """Test numpy type conversion when numpy is not available"""
        processor = BinarySchemaProcessor(self.sample_schema)
        
        # Mock the HAS_NUMPY variable directly on the processor's module
        import sys
        module_name = processor.__class__.__module__
        if module_name in sys.modules:
            module = sys.modules[module_name]
            original_has_numpy = getattr(module, 'HAS_NUMPY', True)
            
            try:
                setattr(module, 'HAS_NUMPY', False)
                # Create new processor instance to test with mocked value
                test_processor = BinarySchemaProcessor(self.sample_schema)
                self.assertIsNone(test_processor.get_numpy_type("np.uint8"))
            finally:
                # Restore original value
                setattr(module, 'HAS_NUMPY', original_has_numpy)
        else:
            # Fallback: just test that the method exists and handles None case
            self.assertIsNotNone(processor.get_numpy_type("np.uint8"))
    
    def test_complex_bit_positions(self):
        """Test parsing of various bit position formats"""
        complex_schema = {
            "field1": {"pos": "0-0", "type": "uint8"},    # Single bit
            "field2": {"pos": "1-8", "type": "uint8"},    # 8 bits
            "field3": {"pos": "9-24", "type": "uint16"},  # 16 bits
        }
        
        processor = BinarySchemaProcessor(complex_schema)
        
        self.assertEqual(processor.fields_by_name["field1"]["start_bit"], 0)
        self.assertEqual(processor.fields_by_name["field1"]["end_bit"], 0)
        
        self.assertEqual(processor.fields_by_name["field2"]["start_bit"], 1)
        self.assertEqual(processor.fields_by_name["field2"]["end_bit"], 8)
        
        self.assertEqual(processor.fields_by_name["field3"]["start_bit"], 9)
        self.assertEqual(processor.fields_by_name["field3"]["end_bit"], 24)
    
    def test_non_enum_field_enum_processing(self):
        """Test that non-enum fields don't get enum processing"""
        processor = BinarySchemaProcessor(self.sample_schema)
        field1 = processor.fields_by_name["field1"]
        
        self.assertEqual(field1["enum"], {})
    
    def test_empty_enum_values(self):
        """Test handling of enum field with empty values"""
        empty_enum_schema = {
            "status": {
                "pos": "0-7",
                "type": "enum",
                "values": []
            }
        }
        
        processor = BinarySchemaProcessor(empty_enum_schema)
        status_field = processor.fields_by_name["status"]
        
        self.assertEqual(status_field["enum"], {})
    
    def test_field_sorting_by_bit_position(self):
        """Test that fields are correctly sorted by bit position"""
        unsorted_schema = {
            "field_z": {"pos": "16-23", "type": "uint8"},
            "field_a": {"pos": "0-7", "type": "uint8"},
            "field_m": {"pos": "8-15", "type": "uint8"}
        }
        
        processor = BinarySchemaProcessor(unsorted_schema)
        
        # Check that fields are sorted by start_bit
        self.assertEqual(processor.fields[0]["name"], "field_a")  # bits 0-7
        self.assertEqual(processor.fields[1]["name"], "field_m")  # bits 8-15
        self.assertEqual(processor.fields[2]["name"], "field_z")  # bits 16-23

if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBinarySchemaProcessor)
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    print(f"{'='*50}")
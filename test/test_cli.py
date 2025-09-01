import unittest
import tempfile
import json
import os
import shutil
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from click.testing import CliRunner
import pytest
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# Import the CLI module
from telemetry_generator.cli import (
    cli, parse_size, validate_binary_schema, validate_schema_legacy,
    detect_schema_format, generate, validate, create_fault_config,
    list_fault_types, profiles, info
)
from telemetry_generator.fault_injector import FaultType
from telemetry_generator.load_profiles import LOAD_PROFILES


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions"""
    
    def test_parse_size_bytes(self):
        """Test parsing size strings"""
        test_cases = [
            ("100", 100),
            ("1024", 1024),
            ("1KB", 1024),
            ("1K", 1024),
            ("1MB", 1024 * 1024),
            ("1M", 1024 * 1024),
            ("1GB", 1024 * 1024 * 1024),
            ("1G", 1024 * 1024 * 1024),
            ("1TB", 1024 * 1024 * 1024 * 1024),
            ("1T", 1024 * 1024 * 1024 * 1024),
            ("512MB", 512 * 1024 * 1024),
            ("1.5GB", int(1.5 * 1024 * 1024 * 1024)),
            ("0.5MB", int(0.5 * 1024 * 1024))
        ]
        
        for size_str, expected in test_cases:
            with self.subTest(size_str=size_str):
                self.assertEqual(parse_size(size_str), expected)
    
    def test_parse_size_invalid(self):
        """Test parsing invalid size strings"""
        invalid_cases = ["invalid", "1XB", "abc123"]
        
        for invalid_size in invalid_cases:
            with self.subTest(invalid_size=invalid_size):
                with self.assertRaises(ValueError):
                    parse_size(invalid_size)
        
        # Test empty string separately - might raise different exceptions
        try:
            result = parse_size("")
            # If it doesn't raise an exception, it should return 0 or fail our test
            self.fail("Empty string should raise an exception")
        except (ValueError, AttributeError, TypeError):
            # Any of these exceptions is acceptable for empty string
            pass
    
    def test_parse_size_integer_input(self):
        """Test parsing integer input"""
        self.assertEqual(parse_size(1024), 1024)
    
    def test_detect_schema_format_binary(self):
        """Test detecting binary schema format"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            binary_schema = {
                "schema_name": "test_schema",
                "endianness": "little",
                "total_bits": 256,
                "fields": []
            }
            json.dump(binary_schema, f)
            temp_path = f.name
        
        try:
            format_type = detect_schema_format(temp_path)
            self.assertEqual(format_type, "binary")
        finally:
            os.unlink(temp_path)
    
    def test_detect_schema_format_legacy(self):
        """Test detecting legacy schema format"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            legacy_schema = {
                "field1": {"type": "int", "bits": 32},
                "field2": {"type": "string", "bits": 64}
            }
            json.dump(legacy_schema, f)
            temp_path = f.name
        
        try:
            format_type = detect_schema_format(temp_path)
            self.assertEqual(format_type, "legacy")
        finally:
            os.unlink(temp_path)
    
    def test_detect_schema_format_unknown(self):
        """Test detecting unknown schema format"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            unknown_schema = {"some": "random", "data": 123}
            json.dump(unknown_schema, f)
            temp_path = f.name
        
        try:
            format_type = detect_schema_format(temp_path)
            self.assertEqual(format_type, "unknown")
        finally:
            os.unlink(temp_path)


class TestSchemaValidation(unittest.TestCase):
    """Test schema validation functions"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def create_temp_schema(self, schema_data):
        """Helper to create temporary schema file"""
        schema_path = os.path.join(self.temp_dir, 'schema.json')
        with open(schema_path, 'w') as f:
            json.dump(schema_data, f)
        return schema_path
    
    @patch('telemetry_generator.cli.BinarySchemaProcessor')
    def test_validate_binary_schema_valid(self, mock_processor):
        """Test validating a valid binary schema"""
        valid_schema = {
            "schema_name": "test_telemetry",
            "endianness": "little",
            "total_bits": 256,
            "fields": [
                {
                    "name": "field1",
                    "type": "np.uint32",
                    "start_bit": 0,
                    "end_bit": 31,
                    "bits": 32
                }
            ]
        }
        
        schema_path = self.create_temp_schema(valid_schema)
        
        # Mock the processor constructor to not fail
        mock_proc_instance = Mock()
        mock_proc_instance.schema_name = "test_telemetry"
        mock_proc_instance.fields = valid_schema["fields"]
        mock_proc_instance.endianness = "little"
        mock_proc_instance.total_bits = 256
        mock_processor.return_value = mock_proc_instance
        
        result = validate_binary_schema(schema_path)
        self.assertEqual(result, valid_schema)
        mock_processor.assert_called_once()
    
    def test_validate_binary_schema_missing_file(self):
        """Test validating non-existent schema file"""
        with self.assertRaises(Exception):  # ClickException
            validate_binary_schema("nonexistent.json")
    
    def test_validate_binary_schema_invalid_json(self):
        """Test validating invalid JSON"""
        invalid_path = os.path.join(self.temp_dir, 'invalid.json')
        with open(invalid_path, 'w') as f:
            f.write("invalid json content")
        
        with self.assertRaises(Exception):
            validate_binary_schema(invalid_path)
    
    def test_validate_binary_schema_missing_fields(self):
        """Test validating schema with missing required fields"""
        incomplete_schema = {
            "schema_name": "test",
            # Missing endianness and total_bits
        }
        
        schema_path = self.create_temp_schema(incomplete_schema)
        
        with self.assertRaises(Exception):
            validate_binary_schema(schema_path)
    
    def test_validate_binary_schema_invalid_endianness(self):
        """Test validating schema with invalid endianness"""
        invalid_schema = {
            "schema_name": "test",
            "endianness": "invalid",
            "total_bits": 256
        }
        
        schema_path = self.create_temp_schema(invalid_schema)
        
        with self.assertRaises(Exception):
            validate_binary_schema(schema_path)
    
    def test_validate_schema_legacy_valid(self):
        """Test validating valid legacy schema"""
        legacy_schema = {
            "field1": {"type": "int", "bits": 32},
            "field2": {"type": "string", "bits": 64}
        }
        
        schema_path = self.create_temp_schema(legacy_schema)
        result = validate_schema_legacy(schema_path)
        self.assertEqual(result, legacy_schema)
    
    def test_validate_schema_legacy_empty(self):
        """Test validating empty legacy schema"""
        empty_schema = {}
        schema_path = self.create_temp_schema(empty_schema)
        
        with self.assertRaises(Exception):
            validate_schema_legacy(schema_path)


class TestCLICommands(unittest.TestCase):
    """Test CLI commands using Click's testing framework"""
    
    def setUp(self):
        self.runner = CliRunner()
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a sample binary schema
        self.binary_schema = {
            "schema_name": "test_telemetry",
            "endianness": "little",
            "total_bits": 256,
            "fields": [
                {
                    "name": "timestamp_ns",
                    "type": "np.uint64",
                    "start_bit": 0,
                    "end_bit": 63,
                    "bits": 64
                },
                {
                    "name": "device_id",
                    "type": "np.uint32", 
                    "start_bit": 64,
                    "end_bit": 95,
                    "bits": 32
                },
                {
                    "name": "value_type",
                    "type": "np.uint8",
                    "start_bit": 96,
                    "end_bit": 103,
                    "bits": 8,
                    "enum": {"0": "INTEGER", "1": "FLOAT", "2": "STRING"}
                }
            ]
        }
        
        self.schema_path = os.path.join(self.temp_dir, 'schema.json')
        with open(self.schema_path, 'w') as f:
            json.dump(self.binary_schema, f)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_cli_version(self):
        """Test CLI version option"""
        result = self.runner.invoke(cli, ['--version'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('1.0.0', result.output)
    
    def test_cli_help(self):
        """Test CLI help"""
        result = self.runner.invoke(cli, ['--help'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Telemetry Generator', result.output)
    
    @patch('cli.Math.ceil', return_value=32)  # Mock math.ceil if needed
    @patch('telemetry_generator.cli.click.echo')  # Mock click.echo to avoid output during tests
    @patch('telemetry_generator.cli.EnhancedTelemetryGeneratorPro')
    @patch('telemetry_generator.cli.RollingFileWriter')
    @patch('telemetry_generator.cli.RateLimiter')
    @patch('telemetry_generator.cli.BinarySchemaProcessor')
    @patch('telemetry_generator.cli.validate_binary_schema')
    @patch('telemetry_generator.cli.detect_schema_format', return_value='binary')
    def test_generate_command_basic(self, mock_detect, mock_validate_schema, mock_processor, 
                                   mock_rate_limiter, mock_writer, mock_generator, 
                                   mock_echo, mock_ceil):
        """Test basic generate command"""
        # Mock schema validation
        mock_validate_schema.return_value = self.binary_schema
        
        # Mock processor
        mock_proc_instance = Mock()
        mock_proc_instance.schema_name = "test_telemetry"
        mock_proc_instance.endianness = "little"
        mock_proc_instance.total_bits = 256
        mock_proc_instance.fields = self.binary_schema["fields"]
        mock_proc_instance.validation = {}
        mock_processor.return_value = mock_proc_instance
        
        # Setup generator mocks
        mock_gen_instance = Mock()
        mock_gen_instance.get_enhanced_schema_info.return_value = {
            'schema_name': 'test_telemetry',
            'fields_count': 3,
            'total_bits': 256,
            'total_bytes': 32,
            'fields': []
        }
        mock_gen_instance.estimate_storage_requirements.return_value = {
            'compressed_mb': 10.0
        }
        mock_gen_instance.generate_enhanced_record.return_value = (Mock(), [])
        mock_gen_instance.pack_record_enhanced.return_value = b'test_binary_data'
        mock_gen_instance.get_fault_statistics.return_value = {
            'faulty_records': 0,
            'fault_rate_percent': 0.0,
            'fault_types': {},
            'affected_fields': {}
        }
        mock_generator.return_value = mock_gen_instance
        
        # Mock writer
        mock_writer_instance = Mock()
        mock_writer_instance.file_count = 1
        mock_writer_instance.total_bytes_written = 1000
        mock_writer_instance.close = Mock()
        mock_writer_instance.flush = Mock()
        mock_writer.return_value = mock_writer_instance
        
        # Mock rate limiter
        mock_limiter_instance = Mock()
        mock_limiter_instance.start = Mock()
        mock_limiter_instance.wait_if_needed = Mock()
        mock_limiter_instance.stop = Mock()
        mock_limiter_instance.get_stats.return_value = Mock(actual_rate=100, target_rate=100)
        mock_rate_limiter.return_value = mock_limiter_instance
        
        # Test command
        result = self.runner.invoke(generate, [
            '--schema', self.schema_path,
            '--rate', '100',
            '--duration', '1',
            '--out-dir', self.temp_dir,
            '--quiet'  # Add quiet flag to reduce output complexity
        ])
        
        if result.exit_code != 0:
            print(f"Command failed with: {result.output}")
            if result.exception:
                print(f"Exception: {result.exception}")
        
        self.assertEqual(result.exit_code, 0)
    
    @patch('telemetry_generator.cli.click.echo')  # Mock click.echo to avoid output during tests
    @patch('telemetry_generator.cli.EnhancedTelemetryGeneratorPro')
    @patch('telemetry_generator.cli.BinarySchemaProcessor')
    @patch('telemetry_generator.cli.validate_binary_schema')
    @patch('telemetry_generator.cli.detect_schema_format', return_value='binary')
    def test_validate_command_binary_schema(self, mock_detect, mock_validate_schema, 
                                          mock_processor, mock_generator, mock_echo):
        """Test validate command with binary schema"""
        # Mock schema validation
        mock_validate_schema.return_value = self.binary_schema
        
        # Mock processor
        mock_proc_instance = Mock()
        mock_proc_instance.schema_name = "test_telemetry"
        mock_proc_instance.endianness = "little"
        mock_proc_instance.total_bits = 256
        mock_proc_instance.fields = self.binary_schema["fields"]
        mock_proc_instance.fields_by_name = {
            'timestamp_ns': {'name': 'timestamp_ns', 'enum': None}
        }
        mock_proc_instance.validation = {}
        mock_processor.return_value = mock_proc_instance
        
        # Setup generator mock
        mock_gen_instance = Mock()
        mock_gen_instance.get_enhanced_schema_info.return_value = {
            'schema_name': 'test_telemetry',
            'fields_count': 3,
            'total_bits': 256,
            'total_bytes': 32,
            'fields': [
                {
                    'name': 'timestamp_ns',
                    'type': 'np.uint64',
                    'bits': 64,
                    'position': '0-63'
                }
            ]
        }
        mock_gen_instance.generate_clean_record.return_value = Mock()
        mock_gen_instance.pack_record_enhanced.return_value = b'x' * 32  # 32 bytes
        mock_gen_instance.format_json.return_value = '{"test": "data"}'
        mock_gen_instance.format_ndjson.return_value = '{"test": "data"}\n'
        mock_generator.return_value = mock_gen_instance
        
        result = self.runner.invoke(validate, [
            '--schema', self.schema_path,
            '--records', '10'
        ])
        
        if result.exit_code != 0:
            print(f"Command failed with: {result.output}")
            if result.exception:
                print(f"Exception: {result.exception}")
                import traceback
                traceback.print_exc()
        
        self.assertEqual(result.exit_code, 0)
    
    def test_create_fault_config_basic(self):
        """Test creating basic fault configuration"""
        output_file = os.path.join(self.temp_dir, 'fault_config.json')
        
        result = self.runner.invoke(create_fault_config, [
            output_file,
            '--fault-rate', '0.1',
            '--profile', 'testing'
        ])
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Fault configuration created', result.output)
        
        # Verify file was created and has valid content
        self.assertTrue(os.path.exists(output_file))
        with open(output_file, 'r') as f:
            config = json.load(f)
        
        self.assertEqual(config['global_fault_rate'], 0.1)
        self.assertIn('fault_configs', config)
        self.assertIsInstance(config['fault_configs'], list)
    
    def test_create_fault_config_with_include_exclude(self):
        """Test creating fault config with include/exclude types"""
        output_file = os.path.join(self.temp_dir, 'fault_config.json')
        
        result = self.runner.invoke(create_fault_config, [
            output_file,
            '--include-types', 'out_of_range,wrong_type',
            '--exclude-types', 'wrong_type',
            '--profile', 'custom'
        ])
        
        self.assertEqual(result.exit_code, 0)
        
        with open(output_file, 'r') as f:
            config = json.load(f)
        
        # Should only have out_of_range (wrong_type excluded)
        fault_types = [fc['fault_type'] for fc in config['fault_configs']]
        self.assertIn('out_of_range', fault_types)
        self.assertNotIn('wrong_type', fault_types)
    
    def test_create_fault_config_invalid_types(self):
        """Test creating fault config with invalid types"""
        output_file = os.path.join(self.temp_dir, 'fault_config.json')
        
        result = self.runner.invoke(create_fault_config, [
            output_file,
            '--include-types', 'invalid_fault_type'
        ])
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Invalid fault types', result.output)
    
    def test_list_fault_types_command(self):
        """Test list fault types command"""
        result = self.runner.invoke(list_fault_types)
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Available Fault Injection Types', result.output)
        
        # Check that all fault types are listed
        for fault_type in FaultType:
            self.assertIn(fault_type.value, result.output)
    
    def test_profiles_command(self):
        """Test profiles command"""
        result = self.runner.invoke(profiles)
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Available Load Profiles', result.output)
        
        # Check that some known profiles are listed
        for profile_name in ['low', 'medium', 'high']:
            self.assertIn(profile_name.upper(), result.output)
    
    @patch('telemetry_generator.cli.BinarySchemaProcessor')
    @patch('telemetry_generator.cli.validate_binary_schema')
    @patch('telemetry_generator.cli.detect_schema_format', return_value='binary')
    def test_info_command_binary_schema(self, mock_detect, mock_validate, mock_processor):
        """Test info command with binary schema"""
        # Mock schema validation
        mock_validate.return_value = self.binary_schema
        
        # Setup mock processor
        mock_proc_instance = Mock()
        mock_proc_instance.schema_name = "test_telemetry"
        mock_proc_instance.endianness = "little"
        mock_proc_instance.total_bits = 256
        mock_proc_instance.fields = [
            {
                'name': 'test_field',
                'type': 'np.uint32',
                'bits': 32,
                'start_bit': 0,
                'end_bit': 31
            }
        ]
        mock_proc_instance.validation = {}
        mock_processor.return_value = mock_proc_instance
        
        result = self.runner.invoke(info, [self.schema_path])
        
        if result.exit_code != 0:
            print(f"Info command failed: {result.output}")
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('test_telemetry', result.output)
        self.assertIn('little', result.output)
        self.assertIn('256', result.output)


class TestCLIIntegration(unittest.TestCase):
    """Integration tests for CLI with real file operations"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.runner = CliRunner()
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def create_complete_binary_schema(self):
        """Create a complete binary schema file for testing"""
        schema = {
            "schema_name": "integration_test",
            "endianness": "little",
            "total_bits": 128,
            "fields": [
                {
                    "name": "timestamp_ns",
                    "type": "np.uint64",
                    "start_bit": 0,
                    "end_bit": 63,
                    "bits": 64
                },
                {
                    "name": "device_id",
                    "type": "np.uint32",
                    "start_bit": 64,
                    "end_bit": 95,
                    "bits": 32
                },
                {
                    "name": "status",
                    "type": "np.uint8",
                    "start_bit": 96,
                    "end_bit": 103,
                    "bits": 8,
                    "enum": {"0": "OK", "1": "ERROR", "2": "WARNING"}
                },
                {
                    "name": "reserved",
                    "type": "np.uint24",
                    "start_bit": 104,
                    "end_bit": 127,
                    "bits": 24
                }
            ],
            "validation": {
                "crc32c": {
                    "field": "reserved",
                    "range_bits": "0-103"
                }
            }
        }
        
        schema_path = os.path.join(self.test_dir, 'integration_schema.json')
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        return schema_path
    
    def test_cli_workflow_complete(self):
        """Test complete CLI workflow: validate -> create fault config -> generate"""
        schema_path = self.create_complete_binary_schema()
        
        # Step 1: Validate schema
        with patch('telemetry_generator.cli.EnhancedTelemetryGeneratorPro') as mock_gen, \
             patch('telemetry_generator.cli.BinarySchemaProcessor') as mock_proc, \
             patch('telemetry_generator.cli.validate_binary_schema') as mock_validate, \
             patch('telemetry_generator.cli.detect_schema_format', return_value='binary') as mock_detect, \
             patch('telemetry_generator.cli.click.echo') as mock_echo:
            
            # Mock schema validation
            mock_validate.return_value = {
                "schema_name": "integration_test",
                "endianness": "little", 
                "total_bits": 128,
                "fields": []
            }
            
            # Mock processor
            mock_proc_instance = Mock()
            mock_proc_instance.schema_name = "integration_test"
            mock_proc_instance.endianness = "little"
            mock_proc_instance.total_bits = 128
            mock_proc_instance.fields = []
            mock_proc_instance.fields_by_name = {}
            mock_proc_instance.validation = {}
            mock_proc.return_value = mock_proc_instance
            
            # Mock generator
            mock_instance = Mock()
            mock_instance.get_enhanced_schema_info.return_value = {
                'schema_name': 'integration_test',
                'fields_count': 4,
                'total_bits': 128,
                'total_bytes': 16,
                'fields': []
            }
            mock_instance.generate_clean_record.return_value = Mock()
            mock_instance.pack_record_enhanced.return_value = b'x' * 16
            mock_instance.format_json.return_value = '{"test": "data"}'
            mock_instance.format_ndjson.return_value = '{"test": "data"}\n'
            mock_gen.return_value = mock_instance
            
            result = self.runner.invoke(validate, [
                '--schema', schema_path,
                '--records', '5'
            ])
            
            if result.exit_code != 0:
                print(f"Validate command failed: {result.output}")
            
            self.assertEqual(result.exit_code, 0)
        
        # Step 2: Create fault configuration
        fault_config_path = os.path.join(self.test_dir, 'faults.json')
        result = self.runner.invoke(create_fault_config, [
            fault_config_path,
            '--fault-rate', '0.05',
            '--profile', 'development'
        ])
        
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(os.path.exists(fault_config_path))
        
        # Step 3: Generate data (mocked to avoid long execution)
        with patch('telemetry_generator.cli.EnhancedTelemetryGeneratorPro') as mock_gen, \
             patch('telemetry_generator.cli.RollingFileWriter') as mock_writer, \
             patch('telemetry_generator.cli.RateLimiter') as mock_limiter:
            
            # Setup generator mock
            mock_gen_instance = Mock()
            mock_gen_instance.get_enhanced_schema_info.return_value = {
                'schema_name': 'integration_test',
                'fields_count': 4,
                'total_bits': 128,
                'total_bytes': 16,
                'fields': []
            }
            mock_gen_instance.estimate_storage_requirements.return_value = {
                'compressed_mb': 1.0
            }
            mock_gen_instance.generate_enhanced_record.return_value = (Mock(), [])
            mock_gen_instance.get_fault_statistics.return_value = {
                'faulty_records': 0,
                'fault_rate_percent': 0.0,
                'fault_types': {},
                'affected_fields': {}
            }
            mock_gen.return_value = mock_gen_instance
            
            # Setup writer mock
            mock_writer_instance = Mock()
            mock_writer_instance.file_count = 1
            mock_writer_instance.total_bytes_written = 100
            mock_writer.return_value = mock_writer_instance
            
            # Setup rate limiter mock
            mock_limiter_instance = Mock()
            mock_limiter_instance.get_stats.return_value = Mock(
                actual_rate=10, target_rate=10
            )
            mock_limiter.return_value = mock_limiter_instance
            
            result = self.runner.invoke(generate, [
                '--schema', schema_path,
                '--fault-config', fault_config_path,
                '--enable-faults',
                '--rate', '10',
                '--duration', '1',
                '--out-dir', self.test_dir,
                '--quiet'
            ])
            
            self.assertEqual(result.exit_code, 0)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in CLI commands"""
    
    def setUp(self):
        self.runner = CliRunner()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_generate_missing_schema(self):
        """Test generate command with missing schema file"""
        result = self.runner.invoke(generate, [
            '--schema', 'nonexistent.json'
        ])
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('does not exist', result.output)
    
    def test_validate_invalid_schema(self):
        """Test validate command with invalid schema"""
        invalid_schema_path = os.path.join(self.temp_dir, 'invalid.json')
        with open(invalid_schema_path, 'w') as f:
            f.write("invalid json")
        
        result = self.runner.invoke(validate, [
            '--schema', invalid_schema_path
        ])
        
        self.assertNotEqual(result.exit_code, 0)
        # Check for error indicators in output
        error_indicators = ['Error', 'error', 'Invalid', 'invalid', 'Failed', 'failed']
        has_error_indicator = any(indicator in result.output for indicator in error_indicators)
        self.assertTrue(has_error_indicator, f"Expected error indicator in output: {result.output}")
    
    def test_create_fault_config_invalid_directory(self):
        """Test creating fault config in invalid directory"""
        invalid_path = '/invalid/directory/config.json'
        
        result = self.runner.invoke(create_fault_config, [invalid_path])
        
        self.assertNotEqual(result.exit_code, 0)


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)
    
    # Run tests
    unittest.main(verbosity=2)
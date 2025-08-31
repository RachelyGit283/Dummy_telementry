# #!/usr/bin/env python3
# """
# quick_test.py - Simple test script that works with your directory structure
# Run from: C:\Users\סולי\Music\DD_data_generator
# """

# import sys
# import os
# import json
# import tempfile
# from pathlib import Path

# # Add telemetry_generator to Python path
# current_dir = Path.cwd()
# telemetry_dir = current_dir / "telemetry_generator"

# if telemetry_dir.exists():
#     sys.path.insert(0, str(telemetry_dir))
#     print(f"Added {telemetry_dir} to Python path")
# else:
#     print(f"Warning: telemetry_generator directory not found at {telemetry_dir}")

# def test_basic_imports():
#     """Test basic imports"""
#     print("\n" + "="*50)
#     print("TESTING BASIC IMPORTS")
#     print("="*50)
    
#     try:
#         from .telemetry_generator.types_and_enums import RecordType, OutputFormat, TelemetryRecord
#         print("✅ types_and_enums imported successfully")
        
#         from .telemetry_generator.binary_schema import BinarySchemaProcessor
#         print("✅ binary_schema imported successfully")
        
#         from telemetry_generator import EnhancedTelemetryGeneratorPro
#         print("✅ telemetry_generator imported successfully")
        
#         from .telemetry_generator.data_generators import FieldDataGenerator, RecordDataPopulator
#         print("✅ data_generators imported successfully")
        
#         from .telemetry_generator.formatters import OutputFormatter
#         print("✅ formatters imported successfully")
        
#         return True
        
#     except ImportError as e:
#         print(f"❌ Import failed: {e}")
#         return False

# def test_schema_processing():
#     """Test schema processing with a simple schema"""
#     print("\n" + "="*50)
#     print("TESTING SCHEMA PROCESSING")
#     print("="*50)
    
#     try:
#         from .telemetry_generator.binary_schema import BinarySchemaProcessor
        
#         # Create simple test schema
#         schema = {
#             "schema_name": "QuickTest",
#             "endianness": "little",
#             "total_bits": 96,
#             "seq_no": {
#                 "type": "np.uint32",
#                 "bits": 32,
#                 "pos": "0-31",
#                 "desc": "Sequence number"
#             },
#             "timestamp_ns": {
#                 "type": "np.uint64",
#                 "bits": 64,
#                 "pos": "32-95",
#                 "desc": "Timestamp"
#             }
#         }
        
#         processor = BinarySchemaProcessor(schema)
#         print(f"✅ Schema '{processor.schema_name}' processed successfully")
#         print(f"   Fields: {len(processor.fields)}")
#         print(f"   Total bits: {processor.total_bits}")
#         print(f"   Endianness: {processor.endianness}")
        
#         return True
        
#     except Exception as e:
#         print(f"❌ Schema processing failed: {e}")
#         return False

# def test_record_generation():
#     """Test record generation"""
#     print("\n" + "="*50)
#     print("TESTING RECORD GENERATION")
#     print("="*50)
    
#     try:
#         from telemetry_generator import EnhancedTelemetryGeneratorPro
#         from .telemetry_generator.types_and_enums import RecordType
        
#         # Simple schema for testing
#         schema = {
#             "schema_name": "RecordTest",
#             "endianness": "little",
#             "total_bits": 96,
#             "seq_no": {
#                 "type": "np.uint32",
#                 "bits": 32,
#                 "pos": "0-31",
#                 "desc": "Sequence number"
#             },
#             "timestamp_ns": {
#                 "type": "np.uint64",
#                 "bits": 64,
#                 "pos": "32-95",
#                 "desc": "Timestamp"
#             }
#         }
        
#         generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
#         print("✅ Generator created successfully")
        
#         # Generate a few records
#         for i in range(3):
#             record = generator.generate_enhanced_record()
#             print(f"   Record {i+1}: seq_id={record.sequence_id}, type={record.record_type.value}")
        
#         print("✅ Record generation working")
#         return True
        
#     except Exception as e:
#         print(f"❌ Record generation failed: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# def test_binary_packing():
#     """Test binary packing"""
#     print("\n" + "="*50)
#     print("TESTING BINARY PACKING")
#     print("="*50)
    
#     try:
#         from telemetry_generator import EnhancedTelemetryGeneratorPro
#         import math
        
#         schema = {
#             "schema_name": "PackingTest",
#             "endianness": "little",
#             "total_bits": 64,
#             "field1": {
#                 "type": "np.uint32",
#                 "bits": 32,
#                 "pos": "0-31",
#                 "desc": "Test field 1"
#             },
#             "field2": {
#                 "type": "np.uint32",
#                 "bits": 32,
#                 "pos": "32-63",
#                 "desc": "Test field 2"
#             }
#         }
        
#         generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
#         record = generator.generate_enhanced_record()
        
#         # Pack to binary
#         packed = generator.pack_record_enhanced(record)
#         expected_size = math.ceil(64 / 8)  # 64 bits = 8 bytes
        
#         print(f"✅ Binary packing successful")
#         print(f"   Expected size: {expected_size} bytes")
#         print(f"   Actual size: {len(packed)} bytes")
#         print(f"   Binary data: {packed.hex()}")
        
#         assert len(packed) == expected_size
#         return True
        
#     except Exception as e:
#         print(f"❌ Binary packing failed: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# def test_output_formats():
#     """Test different output formats"""
#     print("\n" + "="*50)
#     print("TESTING OUTPUT FORMATS")
#     print("="*50)
    
#     try:
#         from telemetry_generator import EnhancedTelemetryGeneratorPro
#         import json
        
#         schema = {
#             "schema_name": "FormatTest",
#             "endianness": "little",
#             "total_bits": 64,
#             "test_field": {
#                 "type": "np.uint64",
#                 "bits": 64,
#                 "pos": "0-63",
#                 "desc": "Test field"
#             }
#         }
        
#         generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
#         record = generator.generate_enhanced_record()
        
#         # Test JSON format
#         json_output = generator.format_json(record)
#         json.loads(json_output)  # Validate JSON
#         print("✅ JSON format working")
        
#         # Test NDJSON format
#         ndjson_output = generator.format_ndjson(record)
#         json.loads(ndjson_output.strip())  # Validate NDJSON
#         print("✅ NDJSON format working")
        
#         # Test InfluxDB format
#         influx_output = generator.format_influx_line(record)
#         assert influx_output.endswith('\n')
#         print("✅ InfluxDB line protocol working")
        
#         return True
        
#     except Exception as e:
#         print(f"❌ Output format testing failed: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# def run_pytest_if_available():
#     """Try to run pytest on any test files found"""
#     print("\n" + "="*50)
#     print("RUNNING PYTEST (if available)")
#     print("="*50)
    
#     test_files = list(Path(".").glob("*test*.py"))
    
#     if not test_files:
#         print("No test files found")
#         return True
    
#     try:
#         import pytest
        
#         for test_file in test_files:
#             print(f"\nRunning pytest on {test_file}")
#             try:
#                 # Run pytest programmatically
#                 exit_code = pytest.main([str(test_file), "-v", "--tb=short"])
#                 if exit_code == 0:
#                     print(f"✅ {test_file} - PASSED")
#                 else:
#                     print(f"❌ {test_file} - FAILED")
#             except Exception as e:
#                 print(f"❌ Error running pytest on {test_file}: {e}")
        
#         return True
        
#     except ImportError:
#         print("pytest not available, skipping")
#         return True

# def main():
#     """Main function"""
#     print("Quick Test Script for Telemetry Generator")
#     print("Current directory:", Path.cwd())
    
#     tests = [
#         ("Basic Imports", test_basic_imports),
#         ("Schema Processing", test_schema_processing), 
#         ("Record Generation", test_record_generation),
#         ("Binary Packing", test_binary_packing),
#         ("Output Formats", test_output_formats),
#         ("Pytest Runner", run_pytest_if_available)
#     ]
    
#     results = []
    
#     for test_name, test_func in tests:
#         try:
#             result = test_func()
#             results.append((test_name, result))
#         except Exception as e:
#             print(f"❌ {test_name} crashed: {e}")
#             results.append((test_name, False))
    
#     # Final summary
#     print("\n" + "="*60)
#     print("FINAL TEST SUMMARY")
#     print("="*60)
    
#     passed = 0
#     for test_name, result in results:
#         status = "✅ PASSED" if result else "❌ FAILED"
#         print(f"{test_name:20} {status}")
#         if result:
#             passed += 1
    
#     print(f"\nOverall: {passed}/{len(results)} tests passed")
    
#     if passed == len(results):
#         print("\n🎉 All tests passed! Your telemetry generator is working correctly.")
#     else:
#         print(f"\n⚠️  {len(results) - passed} tests failed. Check the output above for details.")

# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
"""
quick_test.py - Simple test script that works with your directory structure
Run from your DD_data_generator directory
"""

import sys
import os
import json
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# Add telemetry_generator to Python path
current_dir = Path.cwd()
telemetry_dir = current_dir / "telemetry_generator"

if telemetry_dir.exists():
    sys.path.insert(0, str(telemetry_dir))
    print(f"Added {telemetry_dir} to Python path")
else:
    print(f"Warning: telemetry_generator directory not found at {telemetry_dir}")

def test_basic_imports():
    """Test basic imports"""
    print("\n" + "="*50)
    print("TESTING BASIC IMPORTS")
    print("="*50)
    
    try:

        from .telemetry_generator.types_and_enums import RecordType, OutputFormat, TelemetryRecord
        print("✅ types_and_enums imported successfully")
        
        from .telemetry_generator.binary_schema import BinarySchemaProcessor
        print("✅ binary_schema imported successfully")
        
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        print("✅ telemetry_generator imported successfully")
        
        from .telemetry_generator.data_generators import FieldDataGenerator, RecordDataPopulator
        print("✅ data_generators imported successfully")
        
        from .telemetry_generator.formatters import OutputFormatter
        print("✅ formatters imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_schema_processing():
    """Test schema processing with a simple schema"""
    print("\n" + "="*50)
    print("TESTING SCHEMA PROCESSING")
    print("="*50)
    
    try:
        from .telemetry_generator.binary_schema import BinarySchemaProcessor
        
        # Create simple test schema
        schema = {
            "schema_name": "QuickTest",
            "endianness": "little",
            "total_bits": 96,
            "seq_no": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "0-31",
                "desc": "Sequence number"
            },
            "timestamp_ns": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "32-95",
                "desc": "Timestamp"
            }
        }
        
        processor = BinarySchemaProcessor(schema)
        print(f"✅ Schema '{processor.schema_name}' processed successfully")
        print(f"   Fields: {len(processor.fields)}")
        print(f"   Total bits: {processor.total_bits}")
        print(f"   Endianness: {processor.endianness}")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema processing failed: {e}")
        return False

def test_record_generation():
    """Test record generation"""
    print("\n" + "="*50)
    print("TESTING RECORD GENERATION")
    print("="*50)
    
    try:
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        from .telemetry_generator.types_and_enums import RecordType
        
        # Simple schema for testing
        schema = {
            "schema_name": "RecordTest",
            "endianness": "little",
            "total_bits": 96,
            "seq_no": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "0-31",
                "desc": "Sequence number"
            },
            "timestamp_ns": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "32-95",
                "desc": "Timestamp"
            }
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        print("✅ Generator created successfully")
        
        # Generate a few records
        for i in range(3):
            record = generator.generate_enhanced_record()
            print(f"   Record {i+1}: seq_id={record.sequence_id}, type={record.record_type.value}")
        
        print("✅ Record generation working")
        return True
        
    except Exception as e:
        print(f"❌ Record generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_binary_packing():
    """Test binary packing"""
    print("\n" + "="*50)
    print("TESTING BINARY PACKING")
    print("="*50)
    
    try:
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        import math
        
        schema = {
            "schema_name": "PackingTest",
            "endianness": "little",
            "total_bits": 64,
            "field1": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "0-31",
                "desc": "Test field 1"
            },
            "field2": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "32-63",
                "desc": "Test field 2"
            }
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        record = generator.generate_enhanced_record()
        
        # Pack to binary
        packed = generator.pack_record_enhanced(record)
        expected_size = math.ceil(64 / 8)  # 64 bits = 8 bytes
        
        print(f"✅ Binary packing successful")
        print(f"   Expected size: {expected_size} bytes")
        print(f"   Actual size: {len(packed)} bytes")
        print(f"   Binary data: {packed.hex()}")
        
        assert len(packed) == expected_size
        return True
        
    except Exception as e:
        print(f"❌ Binary packing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_output_formats():
    """Test different output formats"""
    print("\n" + "="*50)
    print("TESTING OUTPUT FORMATS")
    print("="*50)
    
    try:
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        import json
        
        schema = {
            "schema_name": "FormatTest",
            "endianness": "little",
            "total_bits": 64,
            "test_field": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "0-63",
                "desc": "Test field"
            }
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        record = generator.generate_enhanced_record()
        
        # Test JSON format
        json_output = generator.format_json(record)
        json.loads(json_output)  # Validate JSON
        print("✅ JSON format working")
        
        # Test NDJSON format
        ndjson_output = generator.format_ndjson(record)
        json.loads(ndjson_output.strip())  # Validate NDJSON
        print("✅ NDJSON format working")
        
        # Test InfluxDB format
        influx_output = generator.format_influx_line(record)
        assert influx_output.endswith('\n')
        print("✅ InfluxDB line protocol working")
        
        return True
        
    except Exception as e:
        print(f"❌ Output format testing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_pytest_if_available():
    """Try to run pytest on any test files found"""
    print("\n" + "="*50)
    print("RUNNING PYTEST (if available)")
    print("="*50)
    
    test_files = list(Path(".").glob("*test*.py"))
    
    if not test_files:
        print("No test files found")
        return True
    
    try:
        import pytest
        
        for test_file in test_files:
            print(f"\nRunning pytest on {test_file}")
            try:
                # Run pytest programmatically
                exit_code = pytest.main([str(test_file), "-v", "--tb=short"])
                if exit_code == 0:
                    print(f"✅ {test_file} - PASSED")
                else:
                    print(f"❌ {test_file} - FAILED")
            except Exception as e:
                print(f"❌ Error running pytest on {test_file}: {e}")
        
        return True
        
    except ImportError:
        print("pytest not available, skipping")
        return True

def main():
    """Main function"""
    print("Quick Test Script for Telemetry Generator")
    print("Current directory:", Path.cwd())
    
    tests = [
        ("Basic Imports", test_basic_imports),
        ("Schema Processing", test_schema_processing), 
        ("Record Generation", test_record_generation),
        ("Binary Packing", test_binary_packing),
        ("Output Formats", test_output_formats),
        ("Pytest Runner", run_pytest_if_available)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Final summary
    print("\n" + "="*60)
    print("FINAL TEST SUMMARY")
    print("="*60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:20} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! Your telemetry generator is working correctly.")
    else:
        print(f"\n⚠️  {len(results) - passed} tests failed. Check the output above for details.")

if __name__ == "__main__":
    main()
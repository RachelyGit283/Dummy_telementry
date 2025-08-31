#!/usr/bin/env python3
"""
run_tests.py - Fixed test runner for telemetry generator
Works with your actual project structure
"""

import sys
import subprocess
import os
from pathlib import Path

def run_command(cmd, description=""):
    """Run a command and return success status"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"✅ {description} - PASSED")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - FAILED (exit code: {e.returncode})")
        return False
    except FileNotFoundError:
        print(f"❌ {description} - FAILED (command not found)")
        return False

def check_project_structure():
    """Check if we're in the right directory and find key files"""
    current_dir = Path.cwd()
    print(f"Current directory: {current_dir}")
    
    # Look for telemetry_generator directory or key files
    possible_locations = [
        current_dir / "telemetry_generator",
        current_dir / "cli.py",
        current_dir / "telemetry_generator.py"
    ]
    
    for location in possible_locations:
        if location.exists():
            print(f"✅ Found: {location}")
            return True
    
    print("❌ Could not find telemetry generator files")
    print("Looking for:")
    for loc in possible_locations:
        print(f"  - {loc}")
    
    return False

def check_dependencies():
    """Check if required dependencies are installed"""
    print("Checking dependencies...")
    
    required_packages = ['pytest', 'pytest-cov']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package} - installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} - missing")
    
    if missing_packages:
        print(f"\nInstall missing packages with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def create_simple_test():
    """Create a simple test file if the main one doesn't exist"""
    test_file = "simple_test.py"
    
    if Path(test_file).exists():
        return test_file
    
    simple_test_content = '''#!/usr/bin/env python3
"""
Simple test to verify basic functionality
"""

import pytest
import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.insert(0, '.')
sys.path.insert(0, './telemetry_generator')

def test_basic_imports():
    """Test that we can import basic modules"""
    try:
        # Try different import paths based on your structure
        if Path("telemetry_generator").exists():
            sys.path.insert(0, "./telemetry_generator")
        
        from types_and_enums import RecordType, OutputFormat, TelemetryRecord
        print("✅ Successfully imported types_and_enums")
        
        from binary_schema import BinarySchemaProcessor
        print("✅ Successfully imported binary_schema")
        
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        print("✅ Successfully imported telemetry_generator")
        
        assert True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        pytest.fail(f"Could not import required modules: {e}")

def test_basic_schema_processing():
    """Test basic schema processing"""
    try:
        from binary_schema import BinarySchemaProcessor
        
        # Simple test schema
        schema = {
            "schema_name": "SimpleTest",
            "endianness": "little",
            "total_bits": 64,
            "test_field": {
                "type": "np.uint64",
                "bits": 64,
                "pos": "0-63",
                "desc": "Test field"
            }
        }
        
        processor = BinarySchemaProcessor(schema)
        assert processor.schema_name == "SimpleTest"
        assert processor.total_bits == 64
        assert len(processor.fields) == 1
        
        print("✅ Basic schema processing works")
        
    except Exception as e:
        pytest.fail(f"Schema processing failed: {e}")

def test_basic_record_generation():
    """Test basic record generation"""
    try:
        from telemetry_generator import EnhancedTelemetryGeneratorPro
        from types_and_enums import RecordType
        
        schema = {
            "schema_name": "BasicTest",
            "endianness": "little", 
            "total_bits": 64,
            "seq_no": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "0-31",
                "desc": "Sequence number"
            },
            "timestamp_ns": {
                "type": "np.uint32",
                "bits": 32,
                "pos": "32-63",
                "desc": "Timestamp"
            }
        }
        
        generator = EnhancedTelemetryGeneratorPro(schema_dict=schema)
        record = generator.generate_enhanced_record()
        
        assert record.record_type == RecordType.UPDATE
        assert record.sequence_id > 0
        assert "seq_no" in record.data
        assert "timestamp_ns" in record.data
        
        print("✅ Basic record generation works")
        
    except Exception as e:
        pytest.fail(f"Record generation failed: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''
    
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(simple_test_content)
    
    print(f"Created simple test file: {test_file}")
    return test_file

def main():
    """Main test runner"""
    print("Telemetry Generator Test Suite")
    print("="*60)
    
    # Check project structure
    if not check_project_structure():
        print("\n💡 Tip: Make sure you're in the directory that contains your telemetry generator files")
        
        # Try to help find the right directory
        current = Path.cwd()
        for subdir in current.rglob("telemetry_generator.py"):
            print(f"Found telemetry_generator.py in: {subdir.parent}")
        
        # Create a simple test anyway
        print("\nCreating a simple test to verify basic functionality...")
        test_file = create_simple_test()
    else:
        test_file = "test_telemetry_generator.py"
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Missing dependencies. Please install them first:")
        print("pip install pytest pytest-cov")
        return False
    
    # Parse command line arguments
    test_types = sys.argv[1:] if len(sys.argv) > 1 else ['basic']
    
    results = []
    
    # Always try basic test first
    if 'basic' in test_types or 'all' in test_types:
        cmd = [
            'python', '-m', 'pytest', 
            test_file,
            '-v',
            '--tb=short'
        ]
        results.append(run_command(cmd, "Basic Functionality Tests"))
    
    if 'unit' in test_types or 'all' in test_types:
        # Only run if main test file exists
        if Path("test_telemetry_generator.py").exists():
            cmd = [
                'python', '-m', 'pytest',
                'test_telemetry_generator.py',
                '-v',
                '--tb=short',
                '-m', 'not integration and not slow'
            ]
            results.append(run_command(cmd, "Unit Tests"))
        else:
            print("⚠️  Main test file not found, skipping unit tests")
    
    if 'validate' in test_types:
        # Run validation test
        print("\n" + "="*60)
        print("Running manual validation...")
        print("="*60)
        
        try:
            # Test basic imports
            print("Testing imports...")
            
            # Add telemetry_generator to path if it exists
            if Path("telemetry_generator").exists():
                sys.path.insert(0, "./telemetry_generator")
            
            from ..telemetry_generator.types_and_enums import RecordType
            print("✅ types_and_enums imported successfully")
            
            from ..telemetry_generator.binary_schema import BinarySchemaProcessor
            print("✅ binary_schema imported successfully")
            
            # Test basic schema
            schema = {
                "schema_name": "ValidationTest",
                "endianness": "little",
                "total_bits": 32,
                "test_field": {
                    "type": "np.uint32",
                    "bits": 32,
                    "pos": "0-31",
                    "desc": "Test field"
                }
            }
            
            processor = BinarySchemaProcessor(schema)
            print(f"✅ Schema processor created: {processor.schema_name}")
            
            results.append(True)
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total and total > 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed or no tests ran")
        return False

if __name__ == "__main__":
    # Display help if requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("""
Telemetry Generator Test Runner

Usage: python run_tests.py [test_type]

Test Types:
    basic       - Run basic functionality tests (default)
    validate    - Run manual validation
    unit        - Run unit tests (if available)
    all         - Run all available tests

Examples:
    python run_tests.py                # Basic tests
    python run_tests.py validate       # Validation only
    python run_tests.py all           # Everything
        """)
        sys.exit(0)
    
    success = main()
    sys.exit(0 if success else 1)
#!/usr/bin/env python3
"""
Test script for the new binary schema format
Run this to verify everything works correctly
"""

import json
import os
import sys
from pathlib import Path

# Add the current directory to Python path for import
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    # Import from local telemetry_generator module
    from generator import EnhancedTelemetryGeneratorPro, BinarySchemaProcessor
    print("✓ Successfully imported from local generator module")
except ImportError as e:
    print(f"✗ Import error: {e}")
    print("Make sure you're running this from the telemetry_generator directory")
    sys.exit(1)

def create_test_schema():
    """Create a minimal test schema for validation"""
    schema = {
        "schema_name": "test_minimal_v1",
        "endianness": "little", 
        "total_bits": 152,  # Fixed: 8+32+64+8+8+32 = 152 bits
        "validation": {
            "crc32c": {
                "algorithm": "crc32c",
                "range_bits": "0-119",  # Fixed: CRC covers bits 0-119 (before CRC field)
                "field": "crc32c"
            }
        },
        "schema_version": {
            "type": "np.uint8",
            "bits": 8,
            "pos": "0-7",
            "desc": "Schema version"
        },
        "device_id": {
            "type": "np.uint32",
            "bits": 32,
            "pos": "8-39", 
            "desc": "Device identifier"
        },
        "timestamp_ns": {
            "type": "np.uint64",
            "bits": 64,
            "pos": "40-103",
            "desc": "Timestamp nanoseconds"
        },
        "status": {
            "type": "np.uint8", 
            "bits": 8,
            "pos": "104-111",
            "enum": {
                "0": "active",
                "1": "idle", 
                "2": "error"
            },
            "desc": "Device status"
        },
        "reserved": {
            "type": "np.uint8",
            "bits": 8,
            "pos": "112-119",
            "desc": "Reserved field"
        },
        "crc32c": {
            "type": "np.uint32",
            "bits": 32,
            "pos": "120-151", 
            "desc": "CRC32C checksum"
        }
    }
    return schema

def test_schema_processing():
    """Test schema processing"""
    print("=== Testing Schema Processing ===")
    
    schema = create_test_schema()
    
    try:
        processor = BinarySchemaProcessor(schema)
        print(f"✓ Schema processed: {processor.schema_name}")
        print(f"  Total bits: {processor.total_bits}")
        print(f"  Fields: {len(processor.fields)}")
        print(f"  Endianness: {processor.endianness}")
        
        for field in processor.fields:
            print(f"    {field['name']}: {field['start_bit']}-{field['end_bit']} "
                  f"({field['bits']} bits, {field['type']})")
        
        return processor
    except Exception as e:
        print(f"✗ Schema processing failed: {e}")
        return None

def test_record_generation(processor):
    """Test record generation"""
    print("\n=== Testing Record Generation ===")
    
    try:
        # Create generator with dict schema
        generator = EnhancedTelemetryGeneratorPro(
            schema_dict=processor.schema
        )
        
        print("✓ Generator initialized")
        
        # Generate a test record
        record = generator.generate_enhanced_record()
        print(f"✓ Record generated: seq_id={record.sequence_id}")
        print(f"  Data fields: {list(record.data.keys())}")
        
        # Pack to binary
        binary_data = generator.pack_record_enhanced(record)
        expected_size = processor.total_bits // 8
        
        print(f"✓ Binary packing: {len(binary_data)} bytes (expected: {expected_size})")
        print(f"  Hex: {binary_data.hex()}")
        
        if len(binary_data) == expected_size:
            print("✓ Size validation passed")
        else:
            print(f"✗ Size mismatch: got {len(binary_data)}, expected {expected_size}")
        
        return generator, record, binary_data
        
    except Exception as e:
        print(f"✗ Record generation failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

def test_multiple_records(generator, count=10):
    """Test generating multiple records"""
    print(f"\n=== Testing Multiple Records ({count}) ===")
    
    try:
        records = []
        for i in range(count):
            record = generator.generate_enhanced_record()
            binary_data = generator.pack_record_enhanced(record)
            records.append((record, binary_data))
        
        print(f"✓ Generated {len(records)} records")
        
        # Validate all have same size
        sizes = [len(data[1]) for data in records]
        if len(set(sizes)) == 1:
            print(f"✓ All records same size: {sizes[0]} bytes")
        else:
            print(f"✗ Size variation: {set(sizes)}")
        
        # Check sequence numbers
        seq_numbers = [data[0].sequence_id for data in records]
        if seq_numbers == list(range(seq_numbers[0], seq_numbers[0] + count)):
            print("✓ Sequence numbers are monotonic")
        else:
            print(f"✗ Sequence numbers not monotonic: {seq_numbers}")
        
        return records
        
    except Exception as e:
        print(f"✗ Multiple records test failed: {e}")
        return []

def test_json_formats(generator):
    """Test JSON format outputs"""
    print("\n=== Testing JSON Formats ===")
    
    try:
        record = generator.generate_enhanced_record()
        
        # Test JSON
        json_str = generator.format_json(record)
        json_data = json.loads(json_str)
        print("✓ JSON format works")
        print(f"  Schema: {json_data.get('schema')}")
        
        # Test NDJSON  
        ndjson_str = generator.format_ndjson(record)
        ndjson_data = json.loads(ndjson_str.strip())
        print("✓ NDJSON format works")
        
        return True
        
    except Exception as e:
        print(f"✗ JSON format test failed: {e}")
        return False

def test_schema_info(generator):
    """Test schema info extraction"""
    print("\n=== Testing Schema Info ===")
    
    try:
        info = generator.get_enhanced_schema_info()
        
        print("✓ Schema info extracted:")
        print(f"  Schema: {info.get('schema_name')}")
        print(f"  Fields: {info.get('fields_count')}")
        print(f"  Total bits: {info.get('total_bits')}")
        print(f"  Total bytes: {info.get('total_bytes')}")
        print(f"  Validation: {info.get('validation')}")
        
        return info
        
    except Exception as e:
        print(f"✗ Schema info test failed: {e}")
        return None

def save_test_schema_file():
    """Save test schema to file for CLI testing"""
    schema = create_test_schema()
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    schema_path = 'data/test_schema.json'
    with open(schema_path, 'w') as f:
        json.dump(schema, f, indent=2)
    
    print(f"\n=== Test Schema Saved ===")
    print(f"Schema saved to: {schema_path}")
    print("You can now test with CLI:")
    print(f"  telegen validate --schema {schema_path}")
    print(f"  telegen generate --schema {schema_path} --rate 100 --duration 10")
    
    return schema_path

def main():
    """Run all tests"""
    print("Testing New Binary Schema Format")
    print("=" * 50)
    
    # Test schema processing
    processor = test_schema_processing()
    if not processor:
        return False
    
    # Test record generation
    generator, record, binary_data = test_record_generation(processor)
    if not generator:
        return False
    
    # Test multiple records
    records = test_multiple_records(generator, count=5)
    if not records:
        return False
    
    # Test JSON formats
    json_success = test_json_formats(generator)
    if not json_success:
        return False
    
    # Test schema info
    info = test_schema_info(generator)
    if not info:
        return False
    
    # Save test schema file
    schema_path = save_test_schema_file()
    
    print("\n" + "=" * 50)
    print("✓ ALL TESTS PASSED")
    print("The new binary schema format is working correctly!")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
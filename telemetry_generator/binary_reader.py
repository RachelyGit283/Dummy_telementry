#!/usr/bin/env python3
"""
Binary Telemetry Reader
Reads binary telemetry files generated with the new schema format
"""

import json
import struct
import math
import sys
import os
from typing import Dict, Any, List, Generator, Union
from pathlib import Path

class BinaryRecordReader:
    """קורא רשומות בינאריות לפי סכמה"""
    
    def __init__(self, schema_file: str):
        """Initialize reader with schema file"""
        with open(schema_file, 'r') as f:
            self.schema = json.load(f)
        
        self.schema_name = self.schema.get("schema_name", "unknown")
        self.endianness = self.schema.get("endianness", "little")
        self.total_bits = self.schema.get("total_bits", 0)
        self.record_size = math.ceil(self.total_bits / 8)
        
        # Parse fields
        self.fields = []
        for field_name, field_info in self.schema.items():
            if field_name in ["schema_name", "endianness", "total_bits", "validation"]:
                continue
            
            pos_str = field_info.get("pos", "0-7")
            start_bit, end_bit = map(int, pos_str.split("-"))
            
            field_data = {
                "name": field_name,
                "type": field_info.get("type", "np.uint8"),
                "bits": field_info.get("bits", 8),
                "start_bit": start_bit,
                "end_bit": end_bit,
                "enum": field_info.get("enum", {}),
                "desc": field_info.get("desc", "")
            }
            self.fields.append(field_data)
        
        # Sort by bit position
        self.fields.sort(key=lambda x: x["start_bit"])
        
        print(f"Initialized reader for schema '{self.schema_name}'")
        print(f"Record size: {self.record_size} bytes")
        print(f"Fields: {len(self.fields)}")

    def read_file(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Read binary file with newline separators and yield records one by one"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_size = os.path.getsize(file_path)
        record_size_with_separator = self.record_size + 1  # +1 for newline
        expected_records = file_size // record_size_with_separator
        
        print(f"Reading {file_path}")
        print(f"File size: {file_size:,} bytes")
        print(f"Record size: {self.record_size} bytes + 1 separator = {record_size_with_separator} bytes")
        print(f"Expected records: {expected_records:,}")
        
        # Check if file size matches expected format
        if file_size % record_size_with_separator != 0:
            print(f"Warning: File size not exact multiple of record+separator size")
            print(f"Remainder: {file_size % record_size_with_separator} bytes")
            print("Attempting line-by-line reading for error recovery...")
            
            # Fallback to line-by-line reading
            yield from self._read_file_by_lines(file_path)
            return
        
        # Standard reading with fixed record size
        with open(file_path, 'rb') as f:
            record_num = 0
            while True:
                # Read record + separator
                record_with_sep = f.read(record_size_with_separator)
                
                if not record_with_sep:
                    break  # End of file
                
                if len(record_with_sep) != record_size_with_separator:
                    print(f"Warning: Incomplete record {record_num} - got {len(record_with_sep)} bytes")
                    # Try to salvage partial data
                    if len(record_with_sep) >= self.record_size:
                        record_bytes = record_with_sep[:self.record_size]
                    else:
                        break
                else:
                    # Check for newline separator
                    if record_with_sep[-1:] == b'\n':
                        record_bytes = record_with_sep[:-1]  # Remove newline
                    else:
                        print(f"Warning: Record {record_num} missing newline separator")
                        record_bytes = record_with_sep[:self.record_size]
                
                # Parse the record
                try:
                    record_data = self._parse_record(record_bytes)
                    record_data['_record_number'] = record_num
                    yield record_data
                    record_num += 1
                except Exception as e:
                    print(f"Error parsing record {record_num}: {e}")
                    print(f"Record hex: {record_bytes.hex()}")
                    # Continue to next record instead of breaking

    def _read_file_by_lines(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Fallback method: read file line by line for error recovery"""
        record_num = 0
        
        with open(file_path, 'rb') as f:
            while True:
                # Read until newline
                line = f.readline()
                
                if not line:
                    break  # End of file
                
                # Remove newline
                if line.endswith(b'\n'):
                    record_bytes = line[:-1]
                else:
                    record_bytes = line
                
                # Check record size
                if len(record_bytes) != self.record_size:
                    print(f"Warning: Record {record_num} has wrong size: {len(record_bytes)} bytes (expected {self.record_size})")
                    if len(record_bytes) == 0:
                        continue  # Skip empty lines
                    # Try to parse anyway if close to expected size
                    if len(record_bytes) < self.record_size // 2:
                        print(f"Skipping record {record_num} - too small")
                        continue
                
                # Parse the record
                try:
                    record_data = self._parse_record(record_bytes)
                    record_data['_record_number'] = record_num
                    record_data['_size_bytes'] = len(record_bytes)
                    yield record_data
                    record_num += 1
                except Exception as e:
                    print(f"Error parsing record {record_num}: {e}")
                    print(f"Record size: {len(record_bytes)} bytes")
                    print(f"Record hex: {record_bytes[:32].hex()}...")
                    # Continue to next record for error recovery

    def _parse_record(self, record_bytes: bytes) -> Dict[str, Any]:
        """Parse a single binary record"""
        data = {}
        
        for field in self.fields:
            value = self._extract_field(record_bytes, field)
            data[field["name"]] = value
        
        return data

    # def _extract_field(self, record_bytes: bytes, field: Dict[str, Any]) -> Any:
    #     """Extract a field value from binary data"""
    #     start_bit = field["start_bit"]
    #     end_bit = field["end_bit"]
    #     bits = field["bits"]
    #     field_type = field["type"]
        
    #     if field_type == "np.bytes_":
    #         # Extract bytes/string
    #         start_byte = start_bit // 8
    #         num_bytes = bits // 8
            
    #         field_bytes = record_bytes[start_byte:start_byte + num_bytes]
    #         # Remove null padding and decode
    #         try:
    #             return field_bytes.rstrip(b'\x00').decode('ascii')
    #         except UnicodeDecodeError:
    #             return field_bytes.hex()  # Return as hex if not ASCII
        
    #     else:
    #         # Extract numeric value
    #         if start_bit % 8 == 0 and bits % 8 == 0:
    #             # Byte-aligned - optimized
    #             value = self._extract_bytes(record_bytes, start_bit // 8, bits // 8)
    #         else:
    #             # Bit-aligned - general
    #             value = self._extract_bits(record_bytes, start_bit, bits)
            
    #         # Handle signed integers
    #         if "int8" in field_type and value >= (1 << (bits - 1)):
    #             value = value - (1 << bits)  # Two's complement
            
    #         # Handle enums
    #         if field.get("enum"):
    #             enum_map = field["enum"]
    #             return enum_map.get(str(value), f"unknown_{value}")
            
    #         # Handle special interpretations
    #         if field["name"] == "value_bits":
    #             # Don't interpret value_bits directly
    #             return value
            
    #         return value
    def _extract_field(self, record_bytes: bytes, field: Dict[str, Any]) -> Any:
        """Extract a field value from binary data - מעודכן לפורמט חדש"""
        start_bit = field["start_bit"]
        end_bit = field["end_bit"]
        bits = field["bits"]
        field_type = field["type"]
        
        if field_type == "bytes":  # שונה מ-"np.bytes_"
            # Extract bytes/string
            start_byte = start_bit // 8
            num_bytes = bits // 8
            
            field_bytes = record_bytes[start_byte:start_byte + num_bytes]
            try:
                return field_bytes.rstrip(b'\x00').decode('ascii')
            except UnicodeDecodeError:
                return field_bytes.hex()
        
        else:
            # Extract numeric value
            if start_bit % 8 == 0 and bits % 8 == 0:
                value = self._extract_bytes(record_bytes, start_bit // 8, bits // 8)
            else:
                value = self._extract_bits(record_bytes, start_bit, bits)
            
            # Handle signed integers - מעודכן
            if field_type == "int8" and value >= (1 << (bits - 1)):
                value = value - (1 << bits)  # Two's complement
            
            # Handle enums - מעודכן לפורמט חדש
            if field.get("type") == "enum":
                values = field.get("values", [])
                if values and isinstance(value, int) and 0 <= value < len(values):
                    return values[value]  # החזר את הערך במיקום המתאים
                else:
                    return f"unknown_{value}"
            
            return value
    def _extract_bytes(self, data: bytes, start_byte: int, num_bytes: int) -> int:
        """Extract byte-aligned integer"""
        field_bytes = data[start_byte:start_byte + num_bytes]
        
        value = 0
        if self.endianness == 'little':
            for i, byte in enumerate(field_bytes):
                value |= byte << (i * 8)
        else:  # big endian
            for i, byte in enumerate(field_bytes):
                value |= byte << ((num_bytes - 1 - i) * 8)
        
        return value

    def _extract_bits(self, data: bytes, start_bit: int, num_bits: int) -> int:
        """Extract bit-aligned integer"""
        value = 0
        
        for bit_offset in range(num_bits):
            bit_pos = start_bit + bit_offset
            byte_pos = bit_pos // 8
            
            if byte_pos >= len(data):
                break
            
            if self.endianness == 'little':
                bit_in_byte = bit_pos % 8
                bit_value = (data[byte_pos] >> bit_in_byte) & 1
                value |= bit_value << bit_offset
            else:  # big endian
                bit_in_byte = 7 - (bit_pos % 8)
                bit_value = (data[byte_pos] >> bit_in_byte) & 1
                value |= bit_value << (num_bits - 1 - bit_offset)
        
        return value

    def read_all_records(self, file_path: str) -> List[Dict[str, Any]]:
        """Read all records from file into memory"""
        return list(self.read_file(file_path))

    def convert_to_json(self, binary_file: str, json_file: str, format_type: str = "ndjson"):
        """Convert binary file to JSON format"""
        records_converted = 0
        
        # with open(json_file, 'w') as f:
        #     if format_type == "json":
        #         f.write("[\n")
            
        #     for i, record in enumerate(self.read_file(binary_file)):
        #         if format_type == "ndjson":
        #             json.dump(record, f, separators=(',', ':'))
        #             f.write('\n')
        #         elif format_type == "json":
        #             if i > 0:
        #                 f.write(',\n')
        #             json.dump(record, f, indent=2)
                
        #         records_converted += 1
            
        #     if format_type == "json":
        #         f.write("\n]")
        
        # print(f"Converted {records_converted:,} records from {binary_file} to {json_file}")
        with open(json_file, 'w') as f:
            if format_type == "json":
                f.write("[\n")
            
            for i, record in enumerate(self.read_file(binary_file)):
                # המר את כל המפתחות למחרוזות
                record_str_keys = {str(k): v for k, v in record.items()}

                if format_type == "ndjson":
                    json.dump(record_str_keys, f, indent=2)
                    f.write('\n')
                elif format_type == "json":
                    if i > 0:
                        f.write(',\n')
                    json.dump(record_str_keys, f, indent=2)
                
                records_converted += 1
            
            if format_type == "json":
                f.write("\n]")

        print(f"Converted {records_converted:,} records from {binary_file} to {json_file}")

def main():
    """Example usage"""
    if len(sys.argv) < 3:
        print("Usage: python binary_reader.py <schema.json> <binary_file.bin> [output.json]")
        print("Example: python binary_reader.py gpu_telemetry_schema.json data/telemetry_0001.bin output.json")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    binary_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        # Create reader
        reader = BinaryRecordReader(schema_file)
        
        if output_file:
            # Convert to JSON
            reader.convert_to_json(binary_file, output_file, "ndjson")
        else:
            # Just display first few records
            print(f"\nFirst 5 records from {binary_file}:")
            print("="*60)
            
            for i, record in enumerate(reader.read_file(binary_file)):
                if i >= 5:
                    break
                
                print(f"\nRecord {i+1}:")
                for key, value in record.items():
                    if key != '_record_number':
                        print(f"  {key:20} {value}")
            
            # Show total count
            total_records = len(reader.read_all_records(binary_file))
            print(f"\nTotal records in file: {total_records:,}")
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
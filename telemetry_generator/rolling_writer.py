"""
Rolling File Writer
Handles automatic file rotation based on size limits
"""

import os
import json
import gzip
import time
import struct
import logging
from pathlib import Path
from typing import Optional, Any, BinaryIO, TextIO, Union
from datetime import datetime

from .formats.leb128 import encode_leb128, encode_signed_leb128

class RollingFileWriter:
    """
    Manages rolling file writes with automatic rotation based on size
    """
    
    def __init__(
        self,
        base_path: str,
        max_size_bytes: int,
        format: str = 'ndjson',
        compress: bool = False,
        timestamp_format: str = '%Y%m%d_%H%M%S',
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize RollingFileWriter
        
        Args:
            base_path: Base path for output files (without extension)
            max_size_bytes: Maximum size in bytes before rotating
            format: Output format ('ndjson', 'json', 'binary', 'influx', 'leb128')
            compress: Whether to compress files with gzip
            timestamp_format: Format for timestamps in filenames
            logger: Optional logger instance
        """
        self.base_path = base_path
        self.max_size_bytes = max_size_bytes
        self.format = format.lower()
        self.compress = compress
        self.timestamp_format = timestamp_format
        self.logger = logger or logging.getLogger(__name__)
        
        # State
        self.current_file: Optional[Union[BinaryIO, TextIO]] = None
        self.current_file_path: Optional[str] = None
        self.current_size: int = 0
        self.file_count: int = 0
        self.total_bytes_written: int = 0
        self.records_in_current_file: int = 0
        self.total_records_written: int = 0
        
        # Format-specific settings
        self.is_binary = format in ['binary', 'leb128']
        self.file_extension = self._get_file_extension()
        
        # Create output directory if it doesn't exist
        self.output_dir = os.path.dirname(base_path) or '.'
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # JSON array handling (for regular JSON format)
        self.json_first_record = True
        
        self.logger.info(
            f"Initialized RollingFileWriter: format={format}, "
            f"max_size={max_size_bytes:,} bytes, compress={compress}"
        )

    def _get_file_extension(self) -> str:
        """Get appropriate file extension based on format and compression"""
        extensions = {
            'ndjson': '.ndjson',
            'json': '.json',
            'binary': '.bin',
            'influx': '.txt',
            'leb128': '.leb128'
        }
        
        ext = extensions.get(self.format, '.dat')
        
        if self.compress:
            ext += '.gz'
        
        return ext

    def _generate_filename(self) -> str:
        """Generate a unique filename with timestamp and sequence number"""
        timestamp = datetime.now().strftime(self.timestamp_format)
        sequence = f"{self.file_count:04d}"
        
        filename = f"{os.path.basename(self.base_path)}_{timestamp}_{sequence}{self.file_extension}"
        
        return os.path.join(self.output_dir, filename)

    def _open_new_file(self):
        """Open a new file for writing"""
        # Close current file if open
        if self.current_file:
            self._close_current_file()
        
        # Generate new filename
        self.current_file_path = self._generate_filename()
        self.file_count += 1
        self.current_size = 0
        self.records_in_current_file = 0
        self.json_first_record = True
        
        self.logger.info(f"Opening new file: {self.current_file_path}")
        
        # Open file with appropriate mode and compression
        if self.compress:
            if self.is_binary:
                self.current_file = gzip.open(self.current_file_path, 'wb')
            else:
                self.current_file = gzip.open(self.current_file_path, 'wt', encoding='utf-8')
        else:
            if self.is_binary:
                self.current_file = open(self.current_file_path, 'wb')
            else:
                self.current_file = open(self.current_file_path, 'w', encoding='utf-8')
        
        # Write header for JSON array format
        if self.format == 'json':
            self._write_raw('[\n')

    def _close_current_file(self):
        """Close the current file"""
        if not self.current_file:
            return
        
        # Close JSON array if needed
        if self.format == 'json' and self.records_in_current_file > 0:
            self._write_raw('\n]')
        
        # Ensure all data is flushed before closing
        self.current_file.flush()
        self.current_file.close()
        self.current_file = None
        
        self.logger.info(
            f"Closed file: {self.current_file_path} "
            f"({self.records_in_current_file:,} records, {self.current_size:,} bytes)"
        )

    def _should_rotate(self, additional_bytes: int = 0) -> bool:
        """Check if file should be rotated"""
        if not self.current_file:
            return True
        
        # Check size limit
        if self.current_size + additional_bytes >= self.max_size_bytes:
            return True
        
        return False

    def _write_raw(self, data: Union[str, bytes]):
        """Write raw data to file and update counters"""
        if isinstance(data, str):
            if self.is_binary:
                data = data.encode('utf-8')
            self.current_file.write(data)
            byte_count = len(data.encode('utf-8'))
        else:
            self.current_file.write(data)
            byte_count = len(data)
        
        self.current_size += byte_count
        self.total_bytes_written += byte_count
        
        return byte_count

    def write_record(self, record: Any, generator: Any = None):
        """
        Write a telemetry record to file
        
        Args:
            record: TelemetryRecord object to write
            generator: Optional generator instance for format-specific serialization
        """
        # Serialize record based on format
        serialized = self._serialize_record(record, generator)
        
        # Check if we need to rotate BEFORE writing
        estimated_size = len(serialized) if isinstance(serialized, bytes) else len(serialized.encode('utf-8'))
        
        if self._should_rotate(estimated_size):
            self._open_new_file()
        
        # Write the record
        if self.format == 'json':
            # Handle JSON array format
            if not self.json_first_record:
                self._write_raw(',\n  ')
            else:
                self._write_raw('  ')
                self.json_first_record = False
            self._write_raw(serialized)
        else:
            self._write_raw(serialized)
        
        # Update counters
        self.records_in_current_file += 1
        self.total_records_written += 1

    
    def _serialize_record(self, record: Any, generator: Any = None) -> Union[str, bytes]:
        """Serialize record based on format"""
        
        if self.format == 'ndjson':
            # Newline-delimited JSON
            data = {
                'type': record.record_type.value,
                'timestamp': record.timestamp,
                'seq_id': record.sequence_id,
                'data': record.data
            }
            return json.dumps(data, separators=(',', ':')) + '\n'
        
        elif self.format == 'json':
            # Regular JSON (for array)
            data = {
                'type': record.record_type.value,
                'timestamp': record.timestamp,
                'seq_id': record.sequence_id,
                'data': record.data
            }
            return json.dumps(data, separators=(',', ':'), indent=2)
        
        elif self.format == 'binary':
            # Binary format with record separator for error recovery
            if generator and hasattr(generator, 'pack_record_enhanced'):
                binary_data = generator.pack_record_enhanced(record)
                return binary_data + b'\n'  # הוסף newline אחרי כל רשומה
            else:
                # Fallback binary serialization
                fallback_data = self._simple_binary_serialize(record)
                return fallback_data + b'\n'  # הוסף newline גם לfallback
        
        elif self.format == 'influx':
            # InfluxDB Line Protocol
            if generator and hasattr(generator, 'format_influx_line'):
                return generator.format_influx_line(record)
            else:
                # Fallback InfluxDB format
                return self._simple_influx_format(record)
        
        elif self.format == 'leb128':
            # LEB128 variable-length encoding
            return self._serialize_leb128(record)
        
        else:
            # Default to NDJSON
            data = {
                'type': record.record_type.value,
                'timestamp': record.timestamp,
                'seq_id': record.sequence_id,
                'data': record.data
            }
            return json.dumps(data) + '\n'
    def _simple_binary_serialize(self, record: Any) -> bytes:
        """Simple binary serialization fallback"""
        # Header: record_type (1 byte) + timestamp (8 bytes) + seq_id (8 bytes)
        header = struct.pack(
            '>BQQ',
            0 if record.record_type.value == 'update' else 1,
            record.timestamp & ((1 << 64) - 1),
            record.sequence_id
        )
        
        # Serialize data as JSON and convert to bytes
        data_json = json.dumps(record.data, separators=(',', ':'))
        data_bytes = data_json.encode('utf-8')
        
        # Length prefix for data (4 bytes)
        length = struct.pack('>I', len(data_bytes))
        
        return header + length + data_bytes

    def _simple_influx_format(self, record: Any) -> str:
        """Simple InfluxDB Line Protocol format"""
        measurement = "telemetry"
        
        # Tags
        tags = [
            f"type={record.record_type.value}",
            f"seq_id={record.sequence_id}"
        ]
        tags_str = ',' + ','.join(tags) if tags else ''
        
        # Fields
        fields = []
        for key, value in record.data.items():
            if isinstance(value, str):
                fields.append(f'{key}="{value}"')
            elif isinstance(value, bool):
                fields.append(f'{key}={str(value).lower()}')
            elif isinstance(value, float):
                fields.append(f'{key}={value}')
            else:  # int
                fields.append(f'{key}={value}i')
        
        fields_str = ','.join(fields)
        
        # Line format: measurement,tags fields timestamp
        return f"{measurement}{tags_str} {fields_str} {record.timestamp}\n"

    def _serialize_leb128(self, record: Any) -> bytes:
        """Serialize record using LEB128 encoding"""
        output = bytearray()
        
        # Record type (1 byte)
        output.append(0 if record.record_type.value == 'update' else 1)
        
        # Timestamp (LEB128 encoded)
        output.extend(encode_leb128(record.timestamp))
        
        # Sequence ID (LEB128 encoded)
        output.extend(encode_leb128(record.sequence_id))
        
        # Number of fields
        output.extend(encode_leb128(len(record.data)))
        
        # Each field
        for key, value in record.data.items():
            # Key (length-prefixed string)
            key_bytes = key.encode('utf-8')
            output.extend(encode_leb128(len(key_bytes)))
            output.extend(key_bytes)
            
            # Value type and value
            if isinstance(value, bool):
                output.append(0)  # Type: bool
                output.append(1 if value else 0)
            elif isinstance(value, int):
                if value < 0:
                    output.append(1)  # Type: signed int
                    output.extend(encode_signed_leb128(value))
                else:
                    output.append(2)  # Type: unsigned int
                    output.extend(encode_leb128(value))
            elif isinstance(value, float):
                output.append(3)  # Type: float
                output.extend(struct.pack('<d', value))  # Double precision
            elif isinstance(value, str):
                output.append(4)  # Type: string
                str_bytes = value.encode('utf-8')
                output.extend(encode_leb128(len(str_bytes)))
                output.extend(str_bytes)
            else:
                # Fallback: serialize as JSON string
                output.append(4)  # Type: string
                json_bytes = json.dumps(value).encode('utf-8')
                output.extend(encode_leb128(len(json_bytes)))
                output.extend(json_bytes)
        
        # Record separator
        output.append(0xFF)
        
        return bytes(output)

    def flush(self):
        """Flush current file buffer"""
        if self.current_file:
            self.current_file.flush()

    def close(self):
        """Close the writer and any open files"""
        if self.current_file:
            # Make sure to flush before closing
            self.flush()
            self._close_current_file()
        
        self.logger.info(
            f"Writer closed: {self.file_count} files, "
            f"{self.total_records_written:,} records, "
            f"{self.total_bytes_written:,} bytes total"
        )

    def get_stats(self) -> dict:
        """Get current writer statistics"""
        return {
            'files_created': self.file_count,
            'current_file': self.current_file_path,
            'current_file_size': self.current_size,
            'current_file_records': self.records_in_current_file,
            'total_records': self.total_records_written,
            'total_bytes': self.total_bytes_written,
            'average_bytes_per_record': (
                self.total_bytes_written / self.total_records_written
                if self.total_records_written > 0 else 0
            )
        }

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Convenience class for specific formats
class NDJSONWriter(RollingFileWriter):
    """Specialized writer for NDJSON format"""
    def __init__(self, base_path: str, max_size_bytes: int, **kwargs):
        super().__init__(base_path, max_size_bytes, format='ndjson', **kwargs)

class BinaryWriter(RollingFileWriter):
    """Specialized writer for binary format"""
    def __init__(self, base_path: str, max_size_bytes: int, **kwargs):
        super().__init__(base_path, max_size_bytes, format='binary', **kwargs)

class LEB128Writer(RollingFileWriter):
    """Specialized writer for LEB128 format"""
    def __init__(self, base_path: str, max_size_bytes: int, **kwargs):
        super().__init__(base_path, max_size_bytes, format='leb128', **kwargs)
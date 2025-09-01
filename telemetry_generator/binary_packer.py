# """
# אורז רשומות בפורמט בינארי עם תמיכה ב-CRC וביטים קבועים
# """
# import logging
# import math
# import struct
# import zlib
# from typing import Dict, Any

# from .binary_schema import BinarySchemaProcessor

# class BinaryRecordPacker:
#     """אורז רשומות בפורמט בינארי חדש"""
    
#     def __init__(self, processor: BinarySchemaProcessor):
#         self.processor = processor
#         self.endian_prefix = '<' if processor.endianness == 'little' else '>'
        
#     def pack_record(self, data: Dict[str, Any]) -> bytes:
#         """ארוז רשומה לפורמט בינארי קבוע"""
#         # יצירת buffer בגודל המדויק
#         total_bytes = math.ceil(self.processor.total_bits / 8)
#         buffer = bytearray(total_bytes)
        
#         # מילוי השדות
#         for field in self.processor.fields:
#             value = data.get(field["name"], 0)
#             self._pack_field(buffer, field, value)
        
#         # חישוב CRC32C אם נדרש
#         if "crc32c" in self.processor.validation:
#             crc_config = self.processor.validation["crc32c"]
#             crc_field_name = crc_config.get("field")
            
#             if crc_field_name and crc_field_name in self.processor.fields_by_name:
#                 crc_field = self.processor.fields_by_name[crc_field_name]
                
#                 # חישוב CRC על הטווח המוגדר
#                 range_bits = crc_config.get("range_bits", "0-319")
#                 start_bit, end_bit = map(int, range_bits.split("-"))
                
#                 start_byte = start_bit // 8
#                 end_byte = (end_bit + 7) // 8
                
#                 # הכן נתונים לCRC (אפס את ה-CRC field תחילה)
#                 temp_buffer = bytearray(buffer)
#                 crc_start_byte = crc_field["start_bit"] // 8
#                 crc_end_byte = (crc_field["end_bit"] + 7) // 8
                
#                 # אפס את שדה ה-CRC
#                 for i in range(crc_start_byte, min(crc_end_byte, len(temp_buffer))):
#                     temp_buffer[i] = 0
                
#                 # חשב CRC על הטווח המוגדר
#                 crc_data = temp_buffer[start_byte:min(end_byte, len(temp_buffer))]
#                 crc_value = zlib.crc32(crc_data) & 0xffffffff
                
#                 # כתוב את ה-CRC לbuffer המקורי
#                 self._pack_field(buffer, crc_field, crc_value)
        
#         return bytes(buffer)
    
#     def _pack_field(self, buffer: bytearray, field: Dict[str, Any], value: Any):
#         """ארוז שדה יחיד לbuffer"""
#         start_bit = field["start_bit"]
#         end_bit = field["end_bit"]
#         bits = field["bits"]
#         field_type = field["type"]
        
#         # טיפול בenums
#         if field.get("enum"):
#             if isinstance(value, str):
#                 # חיפוש המפתח לפי הערך
#                 for key, val in field["enum"].items():
#                     if val == value:
#                         value = int(key)
#                         break
#                 else:
#                     value = 0  # default
        
#         # טיפול בtypes שונים
#         if field_type == "np.bytes_":
#             # טיפול בbytes/string
#             if isinstance(value, str):
#                 value = value.encode('ascii')[:bits//8]
#             elif isinstance(value, (bytes, bytearray)):
#                 value = bytes(value)[:bits//8]
#             else:
#                 value = str(value).encode('ascii')[:bits//8]
            
#             # pad to exact length
#             value = value.ljust(bits//8, b'\x00')
            
#             # כתיבה לbuffer (byte-aligned)
#             start_byte = start_bit // 8
#             for i, byte_val in enumerate(value):
#                 if start_byte + i < len(buffer):
#                     buffer[start_byte + i] = byte_val
        
#         else:
#             # טיפול בערכים נומריים
#             if isinstance(value, float):
#                 if field_type == "np.float32":
#                     value = struct.unpack('I', struct.pack('f', value))[0]
#                 elif field_type == "np.float64":
#                     value = struct.unpack('Q', struct.pack('d', value))[0]
            
            
#             if "int" in field_type.lower():
#                 if "uint" in field_type.lower():
#                     # Unsigned integer
#                     try:
#                         int_value = int(abs(value)) & ((1 << bits) - 1)
#                     except (ValueError, TypeError):
#                         logging.warning(f"Skipping unsigned int conversion for non-numeric value: {value}")
#                         int_value = 0
#                 else:
#                     # Signed integer
#                     try:
#                         int_value = int(value)
#                         # Handle two's complement for negative numbers
#                         if int_value < 0:
#                             int_value = int_value & ((1 << bits) - 1)
#                         else:
#                             int_value = int_value & ((1 << bits) - 1)
#                     except (ValueError, TypeError):
#                         logging.warning(f"Skipping signed int conversion for non-numeric value: {value}")
#                         int_value = 0
#             else:
#                 try:
#                     int_value = int(value) & ((1 << bits) - 1)
#                 except (ValueError, TypeError):
#                     logging.warning(f"Skipping numeric conversion for non-numeric value: {value}")
#                     int_value = 0
#             # כתיבה bit by bit או byte by byte
#             if start_bit % 8 == 0 and bits % 8 == 0:
#                 # Byte-aligned - optimized path
#                 self._write_bytes(buffer, start_bit // 8, bits // 8, int_value)
#             else:
#                 # Bit-aligned - general path
#                 self._write_bits(buffer, start_bit, bits, int_value)
    
#     def _write_bytes(self, buffer: bytearray, start_byte: int, num_bytes: int, value: int):
#         """כתיבת bytes מלאים (אופטימיזציה)"""
#         if self.processor.endianness == 'little':
#             for i in range(num_bytes):
#                 if start_byte + i < len(buffer):
#                     buffer[start_byte + i] = (value >> (i * 8)) & 0xFF
#         else:  # big endian
#             for i in range(num_bytes):
#                 if start_byte + i < len(buffer):
#                     buffer[start_byte + i] = (value >> ((num_bytes - 1 - i) * 8)) & 0xFF
    
#     def _write_bits(self, buffer: bytearray, start_bit: int, num_bits: int, value: int):
#         """כתיבת ביטים לbuffer"""
#         if self.processor.endianness == 'little':
#             # Little-endian bit order
#             for bit_offset in range(num_bits):
#                 bit_pos = start_bit + bit_offset
#                 byte_pos = bit_pos // 8
#                 bit_in_byte = bit_pos % 8
                
#                 if byte_pos < len(buffer):
#                     bit_value = (value >> bit_offset) & 1
#                     if bit_value:
#                         buffer[byte_pos] |= (1 << bit_in_byte)
#                     else:
#                         buffer[byte_pos] &= ~(1 << bit_in_byte)
#         else:
#             # Big-endian bit order
#             for bit_offset in range(num_bits):
#                 bit_pos = start_bit + bit_offset
#                 byte_pos = bit_pos // 8
#                 bit_in_byte = 7 - (bit_pos % 8)  # MSB first
                
#                 if byte_pos < len(buffer):
#                     bit_value = (value >> (num_bits - 1 - bit_offset)) & 1
#                     if bit_value:
#                         buffer[byte_pos] |= (1 << bit_in_byte)
#                     else:
#                         buffer[byte_pos] &= ~(1 << bit_in_byte)
"""
Binary record packer with CRC and fixed bit support
"""
import logging
import math
import struct
import zlib
from typing import Dict, Any

from .binary_schema import BinarySchemaProcessor

class BinaryRecordPacker:
    """Packs records into binary format"""
    
    def __init__(self, processor: BinarySchemaProcessor):
        self.processor = processor
        self.endian_prefix = '<' if processor.endianness == 'little' else '>'
        
    def pack_record(self, data: Dict[str, Any]) -> bytes:
        """Pack a record into fixed binary format"""
        try:
            # Create buffer with exact size
            total_bytes = math.ceil(self.processor.total_bits / 8)
            buffer = bytearray(total_bytes)
            
            # Fill the fields
            for field in self.processor.fields:
                value = data.get(field["name"], 0)
                self._pack_field(buffer, field, value)
            
            # Calculate CRC32C if required
            if "crc32c" in self.processor.validation:
                crc_config = self.processor.validation["crc32c"]
                crc_field_name = crc_config.get("field")
                
                if crc_field_name and crc_field_name in self.processor.fields_by_name:
                    crc_field = self.processor.fields_by_name[crc_field_name]
                    
                    # Calculate CRC on the defined range
                    range_bits = crc_config.get("range_bits", "0-319")
                    start_bit, end_bit = map(int, range_bits.split("-"))
                    
                    start_byte = start_bit // 8
                    end_byte = (end_bit + 7) // 8
                    
                    # Prepare data for CRC (zero the CRC field first)
                    temp_buffer = bytearray(buffer)
                    crc_start_byte = crc_field["start_bit"] // 8
                    crc_end_byte = (crc_field["end_bit"] + 7) // 8
                    
                    # Zero the CRC field
                    for i in range(crc_start_byte, min(crc_end_byte, len(temp_buffer))):
                        temp_buffer[i] = 0
                    
                    # Calculate CRC on the defined range
                    crc_data = temp_buffer[start_byte:min(end_byte, len(temp_buffer))]
                    crc_value = zlib.crc32(crc_data) & 0xffffffff
                    
                    # Write the CRC to the original buffer
                    self._pack_field(buffer, crc_field, crc_value)
            
            return bytes(buffer)
            
        except Exception as e:
            logging.error(f"Error packing record: {e}")
            raise
    
    def _pack_field(self, buffer: bytearray, field: Dict[str, Any], value: Any):
        """Pack a single field into the buffer"""
        try:
            start_bit = field["start_bit"]
            end_bit = field["end_bit"]
            bits = field["bits"]
            field_type = field["type"]
            
            # Handle enums
            if field.get("enum"):
                if isinstance(value, str):
                    # Search for the key by value
                    for key, val in field["enum"].items():
                        if val == value:
                            value = int(key)
                            break
                    else:
                        value = 0  # default
            
            # Handle different types
            if field_type == "np.bytes_":
                # Handle bytes/string
                if isinstance(value, str):
                    value = value.encode('ascii')[:bits//8]
                elif isinstance(value, (bytes, bytearray)):
                    value = bytes(value)[:bits//8]
                else:
                    value = str(value).encode('ascii')[:bits//8]
                
                # Pad to exact length
                value = value.ljust(bits//8, b'\x00')
                
                # Write to buffer (byte-aligned)
                start_byte = start_bit // 8
                for i, byte_val in enumerate(value):
                    if start_byte + i < len(buffer):
                        buffer[start_byte + i] = byte_val
            
            else:
                # Handle numeric values
                if isinstance(value, float):
                    if field_type == "np.float32":
                        value = struct.unpack('I', struct.pack('f', value))[0]
                    elif field_type == "np.float64":
                        value = struct.unpack('Q', struct.pack('d', value))[0]
                
                if "int" in field_type.lower():
                    if "uint" in field_type.lower():
                        # Unsigned integer
                        try:
                            int_value = int(abs(value)) & ((1 << bits) - 1)
                        except (ValueError, TypeError):
                            logging.warning(f"Skipping unsigned int conversion for non-numeric value: {value}")
                            int_value = 0
                    else:
                        # Signed integer
                        try:
                            int_value = int(value)
                            # Handle two's complement for negative numbers
                            if int_value < 0:
                                int_value = int_value & ((1 << bits) - 1)
                            else:
                                int_value = int_value & ((1 << bits) - 1)
                        except (ValueError, TypeError):
                            logging.warning(f"Skipping signed int conversion for non-numeric value: {value}")
                            int_value = 0
                else:
                    try:
                        int_value = int(value) & ((1 << bits) - 1)
                    except (ValueError, TypeError):
                        logging.warning(f"Skipping numeric conversion for non-numeric value: {value}")
                        int_value = 0
                        
                # Write bit by bit or byte by byte
                if start_bit % 8 == 0 and bits % 8 == 0:
                    # Byte-aligned - optimized path
                    self._write_bytes(buffer, start_bit // 8, bits // 8, int_value)
                else:
                    # Bit-aligned - general path
                    self._write_bits(buffer, start_bit, bits, int_value)
                    
        except Exception as e:
            logging.error(f"Error packing field {field.get('name', 'unknown')}: {e}")
            raise
    
    def _write_bytes(self, buffer: bytearray, start_byte: int, num_bytes: int, value: int):
        """Write full bytes (optimization)"""
        try:
            if self.processor.endianness == 'little':
                for i in range(num_bytes):
                    if start_byte + i < len(buffer):
                        buffer[start_byte + i] = (value >> (i * 8)) & 0xFF
            else:  # big endian
                for i in range(num_bytes):
                    if start_byte + i < len(buffer):
                        buffer[start_byte + i] = (value >> ((num_bytes - 1 - i) * 8)) & 0xFF
        except Exception as e:
            logging.error(f"Error writing bytes at position {start_byte}: {e}")
            raise
    
    def _write_bits(self, buffer: bytearray, start_bit: int, num_bits: int, value: int):
        """Write bits to buffer"""
        try:
            if self.processor.endianness == 'little':
                # Little-endian bit order
                for bit_offset in range(num_bits):
                    bit_pos = start_bit + bit_offset
                    byte_pos = bit_pos // 8
                    bit_in_byte = bit_pos % 8
                    
                    if byte_pos < len(buffer):
                        bit_value = (value >> bit_offset) & 1
                        if bit_value:
                            buffer[byte_pos] |= (1 << bit_in_byte)
                        else:
                            buffer[byte_pos] &= ~(1 << bit_in_byte)
            else:
                # Big-endian bit order
                for bit_offset in range(num_bits):
                    bit_pos = start_bit + bit_offset
                    byte_pos = bit_pos // 8
                    bit_in_byte = 7 - (bit_pos % 8)  # MSB first
                    
                    if byte_pos < len(buffer):
                        bit_value = (value >> (num_bits - 1 - bit_offset)) & 1
                        if bit_value:
                            buffer[byte_pos] |= (1 << bit_in_byte)
                        else:
                            buffer[byte_pos] &= ~(1 << bit_in_byte)
        except Exception as e:
            logging.error(f"Error writing bits at position {start_bit}: {e}")
            raise
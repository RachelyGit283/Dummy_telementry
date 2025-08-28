# import json
# import math
# import os
# import random
# import string
# import struct
# import time
# import threading
# from typing import Dict, Any, List, Union, Optional, Tuple
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import asyncio
# import logging
# from enum import Enum
# from dataclasses import dataclass
# from pathlib import Path

# try:
#     import numpy as np
#     HAS_NUMPY = True
# except ImportError:
#     HAS_NUMPY = False

# try:
#     import cupy as cp
#     HAS_CUPY = True
# except ImportError:
#     HAS_CUPY = False

# Number = Union[int, float]

# class RecordType(Enum):
#     UPDATE = "update"
#     EVENT = "event"

# class OutputFormat(Enum):
#     BINARY = "binary"
#     INFLUX_LINE = "influx_line"
#     JSON = "json"

# @dataclass
# class TelemetryRecord:
#     """מבנה נתונים לרשומת טלמטריה"""
#     record_type: RecordType
#     timestamp: int
#     sequence_id: int
#     data: Dict[str, Any]

# class EnhancedBitPacker:
#     """BitPacker מתקדם עם תמיכה ב-Float ו-String"""
    
#     def __init__(self):
#         self._out: List[int] = []
#         self._bitbuf: int = 0
#         self._bitcount: int = 0

#     def write(self, value: int, nbits: int):
#         if nbits <= 0:
#             return
#         if value < 0 or value >= (1 << nbits):
#             raise ValueError(f"value {value} doesn't fit in {nbits} bits")
        
#         self._bitbuf = (self._bitbuf << nbits) | value
#         self._bitcount += nbits
        
#         while self._bitcount >= 8:
#             self._bitcount -= 8
#             byte = (self._bitbuf >> self._bitcount) & 0xFF
#             self._out.append(byte)
#             self._bitbuf &= (1 << self._bitcount) - 1

#     def write_float(self, value: float, precision_bits: int = 16):
#         """
#         כותב float עם קוונטיזציה
#         precision_bits: מספר ביטים לחלק השבר
#         """
#         # המרה לקוונטיזציה
#         scale = 1 << precision_bits
#         quantized = int(value * scale) & ((1 << 32) - 1)
#         self.write(quantized, 32)

#     def write_ieee754_float(self, value: float):
#         """כותב float במפורמט IEEE 754 (32 bit)"""
#         packed = struct.pack('>f', value)  # Big-endian float
#         for byte in packed:
#             self.write(byte, 8)

#     def write_string(self, value: str, max_length: int = 255):
#         """
#         כותב string עם length prefix
#         max_length: אורך מקסימלי (עד 255)
#         """
#         # חיתוך אם צריך
#         truncated = value[:max_length]
#         length = len(truncated)
        
#         # כתיבת length (8 bit = עד 255 תווים)
#         self.write(length, 8)
        
#         # כתיבת התווים
#         for char in truncated:
#             self.write(ord(char), 8)

#     def write_compressed_string(self, value: str, char_bits: int = 6):
#         """
#         כותב string דחוס (רק אותיות וספרות)
#         char_bits: מספר ביטים לתו (6 = 64 תווים שונים)
#         """
#         # מיפוי תווים לערכים
#         charset = string.ascii_letters + string.digits + " ._"
#         char_to_val = {char: i for i, char in enumerate(charset)}
        
#         # אורך
#         length = min(len(value), (1 << 8) - 1)
#         self.write(length, 8)
        
#         # תווים
#         for char in value[:length]:
#             if char in char_to_val:
#                 self.write(char_to_val[char], char_bits)
#             else:
#                 self.write(0, char_bits)  # תו ברירת מחדל

#     def flush_to_byte_boundary(self, pad_with_zero: bool = True):
#         if self._bitcount > 0:
#             if pad_with_zero:
#                 byte = (self._bitbuf << (8 - self._bitcount)) & 0xFF
#                 self._out.append(byte)
#             self._bitbuf = 0
#             self._bitcount = 0

#     def bytes(self) -> bytes:
#         return bytes(self._out)

# class InfluxDBLineProtocolWriter:
#     """כותב בפורמט InfluxDB Line Protocol"""
    
#     @staticmethod
#     def format_record(measurement: str, tags: Dict[str, str], 
#                      fields: Dict[str, Union[int, float, str]], 
#                      timestamp: int) -> str:
#         """
#         יוצר שורת Line Protocol
#         Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
#         """
#         # Tags
#         tag_str = ""
#         if tags:
#             tag_pairs = [f"{k}={v}" for k, v in tags.items()]
#             tag_str = "," + ",".join(tag_pairs)
        
#         # Fields
#         field_pairs = []
#         for k, v in fields.items():
#             if isinstance(v, str):
#                 field_pairs.append(f'{k}="{v}"')
#             elif isinstance(v, float):
#                 field_pairs.append(f'{k}={v}')
#             elif isinstance(v, bool):
#                 field_pairs.append(f'{k}={str(v).lower()}')
#             else:  # int
#                 field_pairs.append(f'{k}={v}i')
        
#         field_str = ",".join(field_pairs)
        
#         return f"{measurement}{tag_str} {field_str} {timestamp}\n"

# class GPUAcceleratedGenerator:
#     """מחלקה לייצור נתונים מואץ GPU"""
    
#     def __init__(self, use_gpu: bool = True):
#         self.use_gpu = False  # מתחיל כ-False
#         self.xp = None
        
#         if use_gpu:
#             try:
#                 import cupy as cp
#                 # בדיקה אם CUDA זמין ועובד
#                 cp.cuda.Device(0).use()  # ננסה להשתמש במכשיר 0
#                 test_array = cp.array([1, 2, 3])  # בדיקה פשוטה
#                 test_result = cp.sum(test_array).get()  # בדיקה שהמרה מGPU עובדת
                
#                 self.use_gpu = True
#                 self.xp = cp
                
#             except (ImportError, Exception) as e:
#                 # כשל בטעינת CUDA או בשימוש בו - נחזור לNumPy או Python רגיל
#                 self.use_gpu = False
#                 try:
#                     import numpy as np
#                     self.xp = np
#                 except ImportError:
#                     self.xp = None
    
#     def generate_batch_int(self, batch_size: int, num_fields: int, 
#                           max_bits: int = 32) -> 'np.ndarray':
#         """יוצר batch של ערכי int"""
#         if not self.xp:
#             # Fallback ל-Python רגיל
#             return [[random.randint(0, (1 << max_bits) - 1) 
#                     for _ in range(num_fields)] 
#                    for _ in range(batch_size)]
        
#         max_val = (1 << max_bits) - 1
        
#         try:
#             batch = self.xp.random.randint(0, max_val, (batch_size, num_fields))
            
#             if self.use_gpu:
#                 return batch.get()  # העברה מ-GPU ל-CPU
#             return batch
            
#         except Exception as e:
#             # אם GPU נכשל באמצע - חזור לPython רגיל
#             self.use_gpu = False
#             self.xp = None
#             return [[random.randint(0, max_val) 
#                     for _ in range(num_fields)] 
#                    for _ in range(batch_size)]

#     def generate_batch_float(self, batch_size: int, num_fields: int) -> 'np.ndarray':
#         """יוצר batch של ערכי float"""
#         if not self.xp:
#             return [[random.random() for _ in range(num_fields)] 
#                    for _ in range(batch_size)]
        
#         try:
#             batch = self.xp.random.random((batch_size, num_fields))
            
#             if self.use_gpu:
#                 return batch.get()
#             return batch
            
#         except Exception as e:
#             # fallback
#             self.use_gpu = False
#             self.xp = None
#             return [[random.random() for _ in range(num_fields)] 
#                    for _ in range(batch_size)]

# class EnhancedTelemetryGeneratorPro:
#     """
#     גרסה מתקדמת של TelemetryGeneratorPro עם תמיכה מלאה ב:
#     - Float/String
#     - סוגי רשומות (Update/Event)
#     - InfluxDB Line Protocol
#     - GPU Acceleration
#     - פורמטי פלט מרובים
#     """

#     def __init__(
#         self,
#         schema_file: str,
#         *,
#         add_sequential_id: bool = True,
#         id_field_name: str = "_seq_id",
#         id_bits: int = 64,
#         output_dir: str = ".",
#         record_padding_to_byte: bool = True,
#         default_record_type: RecordType = RecordType.UPDATE,
#         enable_gpu: bool = False,
#         logger: Optional[logging.Logger] = None
#     ):
#         self.logger = logger or logging.getLogger(__name__)
        
#         # GPU support
#         self.gpu_generator = GPUAcceleratedGenerator(enable_gpu) if enable_gpu else None
        
#         # טעינת סכמה
#         self._load_and_validate_schema(schema_file)
        
#         self.add_sequential_id = add_sequential_id
#         self.id_field_name = id_field_name
#         self.id_bits = id_bits
#         self.output_dir = output_dir
#         self.record_padding_to_byte = record_padding_to_byte
#         self.default_record_type = default_record_type
#         self._next_seq_id = 0
        
#         # קומפילצית סכמה
#         self._compiled_fields: List[Tuple[str, Dict[str, Any], int]] = []
#         self._compile_enhanced_schema()
        
#         self.logger.info(f"Initialized Enhanced TelemetryGeneratorPro with {len(self._compiled_fields)} fields")

#     def _load_and_validate_schema(self, schema_file: str):
#         """טעינה ווולידציה של סכמה מתקדמת"""
#         if not os.path.exists(schema_file):
#             raise FileNotFoundError(f"Schema file {schema_file} not found")
        
#         try:
#             with open(schema_file, "r", encoding='utf-8') as f:
#                 self.schema: Dict[str, Dict[str, Any]] = json.load(f)
#         except json.JSONDecodeError as e:
#             raise ValueError(f"Invalid JSON schema: {e}")
        
#         # וולידציה מתקדמת
#         supported_types = ["int", "bool", "enum", "time", "float", "string"]
#         for field_name, field_info in self.schema.items():
#             field_type = field_info.get("type", "int")
#             if field_type not in supported_types:
#                 raise ValueError(f"Unsupported type '{field_type}' for field {field_name}")

#     def _compile_enhanced_schema(self):
#         """קומפילציה מתקדמת של סכמה"""
#         for key, info in self.schema.items():
#             ftype = info.get("type", "int")
            
#             if ftype == "bool":
#                 nbits = info.get("bits", 1)
#             elif ftype == "enum":
#                 values = info["values"]
#                 # טיפול בstring enums
#                 if isinstance(values[0], str):
#                     # String enum - נשמור את השמות ונעבוד עם אינדקסים
#                     nbits = max(1, math.ceil(math.log2(len(values))))
#                     info["_string_enum_map"] = {i: val for i, val in enumerate(values)}
#                     info["_reverse_enum_map"] = {val: i for i, val in enumerate(values)}
#                 else:
#                     # Int enum - כמו קודם
#                     max_val = max(values)
#                     nbits = max(1, math.ceil(math.log2(max_val + 1)))
#             elif ftype == "float":
#                 nbits = info.get("bits", 32)  # IEEE 754 או קוונטיזציה
#             elif ftype == "string":
#                 max_length = info.get("max_length", 16)
#                 nbits = 8 + (max_length * 8)  # length byte + characters
#             else:  # int, time
#                 nbits = info.get("bits", 32)
            
#             self._compiled_fields.append((key, info, nbits))

#         # הוספת Sequential ID
#         if self.add_sequential_id:
#             self._compiled_fields = (
#                 [(self.id_field_name, {"type": "int", "bits": self.id_bits}, self.id_bits)]
#                 + self._compiled_fields
#             )

#     # ---------- מחוללי ערכים מתקדמים ----------
#     def _gen_float(self, min_val: float = 0.0, max_val: float = 1.0) -> float:
#         """מחולל float עם טווח"""
#         return random.uniform(min_val, max_val)

#     def _gen_string(self, length: int = 8, charset: str = None) -> str:
#         """מחולל string"""
#         if charset is None:
#             charset = string.ascii_letters + string.digits
#         return ''.join(random.choices(charset, k=length))

#     def _gen_enum(self, info: Dict[str, Any]) -> Union[int, str]:
#         """מחולל enum - תומך בint וstring"""
#         values = info["values"]
        
#         if isinstance(values[0], str):
#             # String enum - בוחר ישירות מהרשימה
#             return random.choice(values)
#         else:
#             # Int enum - כמו קודם
#             return random.choice(values)

#     def _gen_time_advanced(self, bits: int, base_time: int = None, 
#                           time_range: int = 3600) -> int:
#         """מחולל זמן מתקדם עם base time"""
#         if base_time is None:
#             base_time = int(time.time())
        
#         offset = random.randint(0, time_range)
#         return base_time + offset

#     def generate_enhanced_record(self, seq_id: Optional[int] = None, 
#                                record_type: RecordType = None) -> TelemetryRecord:
#         """יוצר רשומת טלמטריה מתקדמת"""
#         if record_type is None:
#             record_type = self.default_record_type
        
#         timestamp = time.time_ns()  # נאנו-שניות לדיוק גבוה
        
#         if seq_id is None:
#             seq_id = self._next_seq_id
#             self._next_seq_id += 1

#         # יצירת נתונים
#         data = {}
#         for key, info, nbits in self._compiled_fields:
#             if self.add_sequential_id and key == self.id_field_name:
#                 continue  # נטפל בזה בנפרד
            
#             ftype = info.get("type", "int")
            
#             if ftype == "bool":
#                 data[key] = bool(random.randint(0, 1))
#             elif ftype == "enum":
#                 data[key] = self._gen_enum(info)
#             elif ftype == "float":
#                 min_val = info.get("min", 0.0)
#                 max_val = info.get("max", 1.0)
#                 data[key] = self._gen_float(min_val, max_val)
#             elif ftype == "string":
#                 length = info.get("length", 8)
#                 data[key] = self._gen_string(length)
#             elif ftype == "time":
#                 base_time = info.get("base_time")
#                 time_range = info.get("range", 3600)
#                 data[key] = self._gen_time_advanced(nbits, base_time, time_range)
#             else:  # int
#                 max_val = (1 << nbits) - 1
#                 data[key] = random.randint(0, max_val)

#         return TelemetryRecord(
#             record_type=record_type,
#             timestamp=timestamp,
#             sequence_id=seq_id,
#             data=data
#         )

#     # ---------- פורמטי פלט מרובים ----------
#     def pack_record_enhanced(self, record: TelemetryRecord) -> bytes:
#         """אריזת רשומה מתקדמת לבינארי"""
#         packer = EnhancedBitPacker()
        
#         # Record type (1 bit: 0=UPDATE, 1=EVENT)
#         packer.write(1 if record.record_type == RecordType.EVENT else 0, 1)
        
#         # Timestamp (64 bit)
#         packer.write(record.timestamp & ((1 << 64) - 1), 64)
        
#         # Sequential ID
#         if self.add_sequential_id:
#             packer.write(record.sequence_id, self.id_bits)
        
#         # Data fields
#         for key, info, nbits in self._compiled_fields:
#             if self.add_sequential_id and key == self.id_field_name:
#                 continue
            
#             ftype = info.get("type", "int")
#             value = record.data[key]
            
#             if ftype == "bool":
#                 packer.write(1 if value else 0, 1)
#             elif ftype == "float":
#                 if info.get("encoding") == "ieee754":
#                     packer.write_ieee754_float(value)
#                 else:
#                     precision = info.get("precision_bits", 16)
#                     packer.write_float(value, precision)
#             elif ftype == "string":
#                 if info.get("compressed", False):
#                     char_bits = info.get("char_bits", 6)
#                     packer.write_compressed_string(value, char_bits)
#                 else:
#                     max_length = info.get("max_length", 255)
#                     packer.write_string(value, max_length)
#             elif ftype == "enum":
#                 value = record.data[key]
#                 # טיפול בstring enum לפני אריזה
#                 if isinstance(value, str) and "_reverse_enum_map" in info:
#                     # המרת string לindex
#                     enum_index = info["_reverse_enum_map"].get(value, 0)
#                     packer.write(enum_index, nbits)
#                 elif isinstance(value, int):
#                     # Int enum רגיל
#                     clamped_value = value & ((1 << nbits) - 1)
#                     packer.write(clamped_value, nbits)
#                 else:
#                     # Fallback - נסה להמיר לint
#                     int_value = int(value) & ((1 << nbits) - 1)
#                     packer.write(int_value, nbits)
        
#         if self.record_padding_to_byte:
#             packer.flush_to_byte_boundary()
        
#         return packer.bytes()

#     def format_influx_line(self, record: TelemetryRecord, measurement: str = "telemetry") -> str:
#         """המרת רשומה לפורמט InfluxDB Line Protocol"""
#         # Tags - מידע מטא
#         tags = {
#             "type": record.record_type.value,
#             "seq_id": str(record.sequence_id)
#         }
        
#         # Fields - הנתונים בפועל
#         fields = record.data.copy()
        
#         # המרת timestamp לנאנושניות
#         timestamp = record.timestamp
        
#         return InfluxDBLineProtocolWriter.format_record(measurement, tags, fields, timestamp)

#     def format_json(self, record: TelemetryRecord) -> str:
#         """המרת רשומה לפורמט JSON"""
#         return json.dumps({
#             "type": record.record_type.value,
#             "timestamp": record.timestamp,
#             "sequence_id": record.sequence_id,
#             "data": record.data
#         })

#     # ---------- כתיבה לקובץ במרובים פורמטים ----------
#     def write_records_enhanced(
#         self,
#         path: str,
#         num_records: int,
#         *,
#         output_format: OutputFormat = OutputFormat.BINARY,
#         start_seq_id: Optional[int] = None,
#         record_type_ratio: Dict[RecordType, float] = None,
#         measurement_name: str = "telemetry",
#         progress_callback: Optional[callable] = None
#     ):
#         """כתיבת רשומות בפורמטים שונים"""
#         if start_seq_id is not None:
#             self._next_seq_id = start_seq_id
        
#         # ברירת מחדל ליחס סוגי רשומות
#         if record_type_ratio is None:
#             record_type_ratio = {RecordType.UPDATE: 0.7, RecordType.EVENT: 0.3}
        
#         os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
#         mode = "wb" if output_format == OutputFormat.BINARY else "w"
#         encoding = None if output_format == OutputFormat.BINARY else "utf-8"
        
#         try:
#             with open(path, mode, encoding=encoding) as f:
#                 for i in range(num_records):
#                     # בחירת סוג רשומה לפי היחס
#                     rand = random.random()
#                     cumulative = 0
#                     selected_type = RecordType.UPDATE
                    
#                     for rtype, ratio in record_type_ratio.items():
#                         cumulative += ratio
#                         if rand <= cumulative:
#                             selected_type = rtype
#                             break
                    
#                     # יצירת רשומה
#                     record = self.generate_enhanced_record(record_type=selected_type)
                    
#                     # כתיבה לפי הפורמט
#                     if output_format == OutputFormat.BINARY:
#                         f.write(self.pack_record_enhanced(record))
#                     elif output_format == OutputFormat.INFLUX_LINE:
#                         f.write(self.format_influx_line(record, measurement_name))
#                     elif output_format == OutputFormat.JSON:
#                         f.write(self.format_json(record) + "\n")
                    
#                     # עדכון התקדמות
#                     if progress_callback and i % 1000 == 0:
#                         progress_callback(i, num_records)
            
#             self.logger.info(f"Successfully wrote {num_records} records to {path} in {output_format.value} format")
            
#         except Exception as e:
#             self.logger.error(f"Error writing enhanced records to {path}: {e}")
#             raise

#     # ---------- יצירה עם GPU acceleration ----------
#     def generate_batch_gpu_accelerated(
#         self, 
#         batch_size: int,
#         record_type: RecordType = RecordType.UPDATE
#     ) -> List[TelemetryRecord]:
#         """יצירת batch של רשומות עם האצת GPU"""
#         if not self.gpu_generator:
#             # Fallback ליצירה רגילה
#             return [self.generate_enhanced_record(record_type=record_type) 
#                    for _ in range(batch_size)]
        
#         records = []
#         timestamp_base = time.time_ns()
        
#         # חישוב מספר שדות מכל סוג
#         int_fields = []
#         float_fields = []
        
#         for key, info, nbits in self._compiled_fields:
#             if self.add_sequential_id and key == self.id_field_name:
#                 continue
            
#             ftype = info.get("type", "int")
#             if ftype in ["int", "enum", "bool", "time"]:
#                 int_fields.append((key, info, nbits))
#             elif ftype == "float":
#                 float_fields.append((key, info))
        
#         # יצירת batches עם GPU
#         if int_fields:
#             int_batch = self.gpu_generator.generate_batch_int(
#                 batch_size, len(int_fields), 
#                 max_bits=max(nbits for _, _, nbits in int_fields)
#             )
        
#         if float_fields:
#             float_batch = self.gpu_generator.generate_batch_float(
#                 batch_size, len(float_fields)
#             )
        
#         # הרכבת רשומות
#         for i in range(batch_size):
#             data = {}
            
#             # שדות int
#             for j, (key, info, nbits) in enumerate(int_fields):
#                 ftype = info.get("type", "int")
#                 raw_value = int_batch[i][j] if int_fields else 0
                
#                 if ftype == "bool":
#                     data[key] = bool(raw_value % 2)
#                 elif ftype == "enum":
#                     values = info["values"]
#                     if isinstance(values[0], str):
#                         # String enum
#                         data[key] = values[raw_value % len(values)]
#                     else:
#                         # Int enum
#                         data[key] = values[raw_value % len(values)]
#                 else:
#                     # חיתוך לטווח הנכון
#                     max_val = (1 << nbits) - 1
#                     data[key] = raw_value & max_val
            
#             # שדות float
#             for j, (key, info) in enumerate(float_fields):
#                 raw_value = float_batch[i][j] if float_fields else 0.0
#                 min_val = info.get("min", 0.0)
#                 max_val = info.get("max", 1.0)
#                 data[key] = min_val + raw_value * (max_val - min_val)
            
#             # שדות string (לא מואצים בGPU)
#             for key, info, _ in self._compiled_fields:
#                 if info.get("type") == "string":
#                     length = info.get("length", 8)
#                     data[key] = self._gen_string(length)
            
#             record = TelemetryRecord(
#                 record_type=record_type,
#                 timestamp=timestamp_base + i,  # timestamps עוקבים
#                 sequence_id=self._next_seq_id + i,
#                 data=data
#             )
#             records.append(record)
        
#         self._next_seq_id += batch_size
#         return records

#     # ---------- יצירה מקבילית מתקדמת ----------
#     def generate_multiple_files_enhanced(
#         self,
#         *,
#         num_files: int,
#         records_per_file: int,
#         file_prefix: str = "telemetry",
#         output_format: OutputFormat = OutputFormat.BINARY,
#         max_workers: int = 4,
#         start_seq_id: int = 0,
#         use_gpu_batches: bool = False,
#         batch_size: int = 1000,
#         record_type_ratio: Dict[RecordType, float] = None
#     ) -> List[str]:
#         """יצירת קבצים מרובים עם תכונות מתקדמות"""
#         paths: List[str] = []
#         futures = []
        
#         # הגדרת סיומת לפי פורמט
#         extensions = {
#             OutputFormat.BINARY: ".bin",
#             OutputFormat.INFLUX_LINE: ".txt",
#             OutputFormat.JSON: ".json"
#         }
#         ext = extensions[output_format]
        
#         total_records = num_files * records_per_file
#         self.logger.info(f"Starting enhanced generation: {num_files} files × {records_per_file} records = {total_records} total")
        
#         with ThreadPoolExecutor(max_workers=max_workers) as ex:
#             for i in range(num_files):
#                 path = os.path.join(self.output_dir, f"{file_prefix}_{i:04d}{ext}")
#                 paths.append(path)
                
#                 file_start_seq = start_seq_id + i * records_per_file
#                 futures.append(
#                     ex.submit(
#                         self._write_enhanced_file_isolated,
#                         path, records_per_file, file_start_seq,
#                         output_format, use_gpu_batches, batch_size, record_type_ratio
#                     )
#                 )
            
#             completed = 0
#             for future in as_completed(futures):
#                 try:
#                     future.result()
#                     completed += 1
#                     self.logger.info(f"Enhanced generation: {completed}/{num_files} files completed")
#                 except Exception as e:
#                     self.logger.error(f"Error in enhanced parallel generation: {e}")
#                     raise
        
#         self.logger.info(f"Successfully generated all {num_files} enhanced files")
#         return paths

#     def _write_enhanced_file_isolated(
#         self, 
#         path: str, 
#         num_records: int, 
#         file_start_seq: int,
#         output_format: OutputFormat,
#         use_gpu_batches: bool,
#         batch_size: int,
#         record_type_ratio: Dict[RecordType, float]
#     ):
#         """כתיבת קובץ מתקדם במצב מבודד"""
#         try:
#             # יצירת קובץ schema זמני לthread המבודד
#             temp_schema_file = os.path.join(
#                 os.path.dirname(path) or ".", 
#                 f"temp_schema_{os.getpid()}_{threading.current_thread().ident}.json"
#             )
            
#             # כתיבת הsschema לקובץ זמני
#             with open(temp_schema_file, 'w') as f:
#                 json.dump(self.schema, f)
            
#             try:
#                 # יצירת instance מבודד שטוען מקובץ JSON
#                 isolated = EnhancedTelemetryGeneratorPro(
#                     schema_file=temp_schema_file,
#                     add_sequential_id=self.add_sequential_id,
#                     id_field_name=self.id_field_name,
#                     id_bits=self.id_bits,
#                     output_dir=os.path.dirname(path) or ".",
#                     record_padding_to_byte=self.record_padding_to_byte,
#                     default_record_type=self.default_record_type,
#                     enable_gpu=False,  # לא GPU בthreads מבודדים
#                     logger=self.logger
#                 )
                
#                 isolated._next_seq_id = file_start_seq
                
#                 # כתיבה רגילה (ללא GPU batches בthread מבודד)
#                 isolated.write_records_enhanced(
#                     path, num_records,
#                     output_format=output_format,
#                     record_type_ratio=record_type_ratio
#                 )
                
#             finally:
#                 # ניקוי קובץ schema זמני
#                 if os.path.exists(temp_schema_file):
#                     os.remove(temp_schema_file)
                
#         except Exception as e:
#             self.logger.error(f"Error in isolated enhanced file writing for {path}: {e}")
#             raise

#     def _write_with_gpu_batches(
#         self,
#         path: str,
#         num_records: int,
#         output_format: OutputFormat,
#         batch_size: int,
#         record_type_ratio: Dict[RecordType, float]
#     ):
#         """כתיבה עם batches של GPU"""
#         mode = "wb" if output_format == OutputFormat.BINARY else "w"
#         encoding = None if output_format == OutputFormat.BINARY else "utf-8"
        
#         remaining = num_records
        
#         with open(path, mode, encoding=encoding) as f:
#             while remaining > 0:
#                 current_batch_size = min(batch_size, remaining)
                
#                 # בחירת סוג רשומה לבatch
#                 record_type = RecordType.UPDATE
#                 if record_type_ratio:
#                     rand = random.random()
#                     cumulative = 0
#                     for rtype, ratio in record_type_ratio.items():
#                         cumulative += ratio
#                         if rand <= cumulative:
#                             record_type = rtype
#                             break
                
#                 # יצירת batch עם GPU
#                 batch = self.generate_batch_gpu_accelerated(current_batch_size, record_type)
                
#                 # כתיבת הbatch
#                 for record in batch:
#                     if output_format == OutputFormat.BINARY:
#                         f.write(self.pack_record_enhanced(record))
#                     elif output_format == OutputFormat.INFLUX_LINE:
#                         f.write(self.format_influx_line(record))
#                     elif output_format == OutputFormat.JSON:
#                         f.write(self.format_json(record) + "\n")
                
#                 remaining -= current_batch_size

#     # ---------- עטיפות async מתקדמות ----------
#     async def generate_multiple_files_enhanced_async(
#         self,
#         *,
#         num_files: int,
#         records_per_file: int,
#         file_prefix: str = "telemetry",
#         output_format: OutputFormat = OutputFormat.BINARY,
#         max_workers: int = 4,
#         start_seq_id: int = 0,
#         use_gpu_batches: bool = False,
#         batch_size: int = 1000,
#         record_type_ratio: Dict[RecordType, float] = None
#     ) -> List[str]:
#         loop = asyncio.get_running_loop()
#         return await loop.run_in_executor(
#             None,
#             lambda: self.generate_multiple_files_enhanced(
#                 num_files=num_files,
#                 records_per_file=records_per_file,
#                 file_prefix=file_prefix,
#                 output_format=output_format,
#                 max_workers=max_workers,
#                 start_seq_id=start_seq_id,
#                 use_gpu_batches=use_gpu_batches,
#                 batch_size=batch_size,
#                 record_type_ratio=record_type_ratio
#             )
#         )

#     # ---------- שיטות עזר מתקדמות ----------
#     def get_enhanced_schema_info(self) -> Dict[str, Any]:
#         """מידע מפורט על סכמה מתקדמת"""
#         total_bits = 0
#         field_details = []
        
#         # Record type + timestamp + seq_id overhead
#         overhead_bits = 1 + 64 + (self.id_bits if self.add_sequential_id else 0)
#         total_bits += overhead_bits
        
#         for name, info, nbits in self._compiled_fields:
#             if self.add_sequential_id and name == self.id_field_name:
#                 continue
            
#             field_details.append({
#                 "name": name,
#                 "type": info.get("type"),
#                 "bits": nbits,
#                 "details": info
#             })
#             total_bits += nbits
        
#         return {
#             "fields_count": len(self._compiled_fields),
#             "data_fields_count": len(field_details),
#             "overhead_bits": overhead_bits,
#             "total_bits_per_record": total_bits,
#             "bytes_per_record": math.ceil(total_bits / 8),
#             "supports_gpu": bool(self.gpu_generator),
#             "supported_formats": [f.value for f in OutputFormat],
#             "fields": field_details
#         }

#     def estimate_storage_requirements(
#         self, 
#         num_records: int, 
#         output_format: OutputFormat = OutputFormat.BINARY,
#         compression_ratio: float = 1.0
#     ) -> Dict[str, int]:
#         """הערכת דרישות אחסון"""
#         info = self.get_enhanced_schema_info()
        
#         if output_format == OutputFormat.BINARY:
#             base_size = num_records * info["bytes_per_record"]
#         elif output_format == OutputFormat.JSON:
#             # הערכה גסה - JSON גדול פי 3-5 מבינארי
#             base_size = num_records * info["bytes_per_record"] * 4
#         else:  # InfluxDB Line Protocol
#             # הערכה גסה - דומה לJSON אבל קצת יותר קומפקטי
#             base_size = num_records * info["bytes_per_record"] * 3
        
#         compressed_size = int(base_size * compression_ratio)
        
#         return {
#             "records": num_records,
#             "format": output_format.value,
#             "uncompressed_bytes": base_size,
#             "compressed_bytes": compressed_size,
#             "uncompressed_mb": base_size / (1024 * 1024),
#             "compressed_mb": compressed_size / (1024 * 1024),
#             "compression_ratio": compression_ratio
#         }

#     def benchmark_generation_speed(
#         self, 
#         test_records: int = 10000,
#         use_gpu: bool = False,
#         batch_size: int = 1000
#     ) -> Dict[str, float]:
#         """בנצ'מרק מהירות יצירה"""
#         import time
        
#         results = {}
        
#         # בדיקה רגילה
#         start = time.time()
#         for _ in range(test_records):
#             self.generate_enhanced_record()
#         regular_time = time.time() - start
#         results["regular_records_per_sec"] = test_records / regular_time
        
#         # בדיקה עם GPU (אם זמין)
#         if use_gpu and self.gpu_generator:
#             start = time.time()
#             batches = test_records // batch_size
#             for _ in range(batches):
#                 self.generate_batch_gpu_accelerated(batch_size)
#             gpu_time = time.time() - start
#             results["gpu_records_per_sec"] = (batches * batch_size) / gpu_time
#             results["gpu_speedup"] = results["gpu_records_per_sec"] / results["regular_records_per_sec"]
        
#         return results

import json
import math
import os
import random
import string
import struct
import time
import threading
from typing import Dict, Any, List, Union, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import logging
from enum import Enum
from dataclasses import dataclass
from pathlib import Path

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import cupy as cp
    HAS_CUPY = True
except ImportError:
    HAS_CUPY = False

Number = Union[int, float]

class RecordType(Enum):
    UPDATE = "update"
    EVENT = "event"

class OutputFormat(Enum):
    BINARY = "binary"
    INFLUX_LINE = "influx_line"
    JSON = "json"

@dataclass
class TelemetryRecord:
    """מבנה נתונים לרשומת טלמטריה"""
    record_type: RecordType
    timestamp: int
    sequence_id: int
    data: Dict[str, Any]

class EnhancedBitPacker:
    """BitPacker מתקדם עם תמיכה ב-Float ו-String"""
    
    def __init__(self):
        self._out: List[int] = []
        self._bitbuf: int = 0
        self._bitcount: int = 0

    def write(self, value: int, nbits: int):
        if nbits <= 0:
            return
        if value < 0 or value >= (1 << nbits):
            raise ValueError(f"value {value} doesn't fit in {nbits} bits")
        
        self._bitbuf = (self._bitbuf << nbits) | value
        self._bitcount += nbits
        
        while self._bitcount >= 8:
            self._bitcount -= 8
            byte = (self._bitbuf >> self._bitcount) & 0xFF
            self._out.append(byte)
            self._bitbuf &= (1 << self._bitcount) - 1

    def write_float(self, value: float, precision_bits: int = 16):
        """
        כותב float עם קוונטיזציה
        precision_bits: מספר ביטים לחלק השבר
        """
        # המרה לקוונטיזציה
        scale = 1 << precision_bits
        quantized = int(value * scale) & ((1 << 32) - 1)
        self.write(quantized, 32)

    def write_ieee754_float(self, value: float):
        """כותב float במפורמט IEEE 754 (32 bit)"""
        packed = struct.pack('>f', value)  # Big-endian float
        for byte in packed:
            self.write(byte, 8)

    def write_string(self, value: str, max_length: int = 255):
        """
        כותב string עם length prefix
        max_length: אורך מקסימלי (עד 255)
        """
        # חיתוך אם צריך
        truncated = value[:max_length]
        length = len(truncated)
        
        # כתיבת length (8 bit = עד 255 תווים)
        self.write(length, 8)
        
        # כתיבת התווים
        for char in truncated:
            self.write(ord(char), 8)

    def write_compressed_string(self, value: str, char_bits: int = 6):
        """
        כותב string דחוס (רק אותיות וספרות)
        char_bits: מספר ביטים לתו (6 = 64 תווים שונים)
        """
        # מיפוי תווים לערכים
        charset = string.ascii_letters + string.digits + " ._"
        char_to_val = {char: i for i, char in enumerate(charset)}
        
        # אורך
        length = min(len(value), (1 << 8) - 1)
        self.write(length, 8)
        
        # תווים
        for char in value[:length]:
            if char in char_to_val:
                self.write(char_to_val[char], char_bits)
            else:
                self.write(0, char_bits)  # תו ברירת מחדל

    def flush_to_byte_boundary(self, pad_with_zero: bool = True):
        if self._bitcount > 0:
            if pad_with_zero:
                byte = (self._bitbuf << (8 - self._bitcount)) & 0xFF
                self._out.append(byte)
            self._bitbuf = 0
            self._bitcount = 0

    def bytes(self) -> bytes:
        return bytes(self._out)

class InfluxDBLineProtocolWriter:
    """כותב בפורמט InfluxDB Line Protocol"""
    
    @staticmethod
    def format_record(measurement: str, tags: Dict[str, str], 
                     fields: Dict[str, Union[int, float, str]], 
                     timestamp: int) -> str:
        """
        יוצר שורת Line Protocol
        Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        """
        # Tags
        tag_str = ""
        if tags:
            tag_pairs = [f"{k}={v}" for k, v in tags.items()]
            tag_str = "," + ",".join(tag_pairs)
        
        # Fields
        field_pairs = []
        for k, v in fields.items():
            if isinstance(v, str):
                field_pairs.append(f'{k}="{v}"')
            elif isinstance(v, float):
                field_pairs.append(f'{k}={v}')
            elif isinstance(v, bool):
                field_pairs.append(f'{k}={str(v).lower()}')
            else:  # int
                field_pairs.append(f'{k}={v}i')
        
        field_str = ",".join(field_pairs)
        
        return f"{measurement}{tag_str} {field_str} {timestamp}\n"

class GPUAcceleratedGenerator:
    """מחלקה לייצור נתונים מואץ GPU"""
    
    def __init__(self, use_gpu: bool = True):
        self.use_gpu = False  # מתחיל כ-False
        self.xp = None
        
        if use_gpu:
            try:
                import cupy as cp
                # בדיקה אם CUDA זמין ועובד
                cp.cuda.Device(0).use()  # ננסה להשתמש במכשיר 0
                test_array = cp.array([1, 2, 3])  # בדיקה פשוטה
                test_result = cp.sum(test_array).get()  # בדיקה שהמרה מGPU עובדת
                
                self.use_gpu = True
                self.xp = cp
                
            except (ImportError, Exception) as e:
                # כשל בטעינת CUDA או בשימוש בו - נחזור לNumPy או Python רגיל
                self.use_gpu = False
                try:
                    import numpy as np
                    self.xp = np
                except ImportError:
                    self.xp = None
    
    def generate_batch_int(self, batch_size: int, num_fields: int, 
                          max_bits: int = 32) -> 'np.ndarray':
        """יוצר batch של ערכי int"""
        if not self.xp:
            # Fallback ל-Python רגיל
            return [[random.randint(0, (1 << max_bits) - 1) 
                    for _ in range(num_fields)] 
                   for _ in range(batch_size)]
        
        max_val = (1 << max_bits) - 1
        
        try:
            batch = self.xp.random.randint(0, max_val, (batch_size, num_fields))
            
            if self.use_gpu:
                return batch.get()  # העברה מ-GPU ל-CPU
            return batch
            
        except Exception as e:
            # אם GPU נכשל באמצע - חזור לPython רגיל
            self.use_gpu = False
            self.xp = None
            return [[random.randint(0, max_val) 
                    for _ in range(num_fields)] 
                   for _ in range(batch_size)]

    def generate_batch_float(self, batch_size: int, num_fields: int) -> 'np.ndarray':
        """יוצר batch של ערכי float"""
        if not self.xp:
            return [[random.random() for _ in range(num_fields)] 
                   for _ in range(batch_size)]
        
        try:
            batch = self.xp.random.random((batch_size, num_fields))
            
            if self.use_gpu:
                return batch.get()
            return batch
            
        except Exception as e:
            # fallback
            self.use_gpu = False
            self.xp = None
            return [[random.random() for _ in range(num_fields)] 
                   for _ in range(batch_size)]

class EnhancedTelemetryGeneratorPro:
    """
    גרסה מתקדמת של TelemetryGeneratorPro עם תמיכה מלאה ב:
    - Float/String
    - סוגי רשומות (Update/Event)
    - InfluxDB Line Protocol
    - GPU Acceleration
    - פורמטי פלט מרובים
    """

    def __init__(
        self,
        schema_file: str = None,  # הפוך לאופציונלי
        schema_dict: dict = None,  # הוסף אפשרות לקבל dict ישירות
        *,
        add_sequential_id: bool = True,
        id_field_name: str = "_seq_id",
        id_bits: int = 64,
        output_dir: str = ".",
        record_padding_to_byte: bool = True,
        default_record_type: RecordType = RecordType.UPDATE,
        enable_gpu: bool = False,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Enhanced TelemetryGeneratorPro
        
        Args:
            schema_file: Path to JSON schema file (optional if schema_dict provided)
            schema_dict: Schema dictionary (optional if schema_file provided)
            add_sequential_id: Whether to add sequential ID field
            id_field_name: Name of sequential ID field
            id_bits: Number of bits for sequential ID
            output_dir: Output directory for files
            record_padding_to_byte: Whether to pad records to byte boundary
            default_record_type: Default record type
            enable_gpu: Whether to enable GPU acceleration
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # GPU support
        self.gpu_generator = GPUAcceleratedGenerator(enable_gpu) if enable_gpu else None
        
        # טעינת סכמה - מקובץ או מ-dict
        if schema_file:
            self._load_and_validate_schema(schema_file)
        elif schema_dict:
            self.schema = schema_dict
            self._validate_schema_dict()
        else:
            raise ValueError("Either schema_file or schema_dict must be provided")
        
        self.add_sequential_id = add_sequential_id
        self.id_field_name = id_field_name
        self.id_bits = id_bits
        self.output_dir = output_dir
        self.record_padding_to_byte = record_padding_to_byte
        self.default_record_type = default_record_type
        self._next_seq_id = 0
        
        # קומפילצית סכמה
        self._compiled_fields: List[Tuple[str, Dict[str, Any], int]] = []
        self._compile_enhanced_schema()
        
        self.logger.info(f"Initialized Enhanced TelemetryGeneratorPro with {len(self._compiled_fields)} fields")

    def _validate_schema_dict(self):
        """Validate schema dictionary"""
        if not isinstance(self.schema, dict):
            raise ValueError("Schema must be a dictionary")
        
        if not self.schema:
            raise ValueError("Schema cannot be empty")
        
        # וולידציה מתקדמת
        supported_types = ["int", "bool", "enum", "time", "float", "string"]
        for field_name, field_info in self.schema.items():
            field_type = field_info.get("type", "int")
            if field_type not in supported_types:
                raise ValueError(f"Unsupported type '{field_type}' for field {field_name}")

    @classmethod
    def from_cli_params(cls, schema_path: str, **kwargs):
        """Factory method for CLI initialization"""
        # נרמל את הפרמטרים מה-CLI
        cli_params = {
            'schema_file': schema_path,
            'enable_gpu': kwargs.get('gpu', False),
            'output_dir': kwargs.get('output_dir', '.'),
            'logger': kwargs.get('logger')
        }
        
        # פילטר רק פרמטרים רלוונטיים
        init_params = {k: v for k, v in cli_params.items() if v is not None}
        
        return cls(**init_params)

    def _load_and_validate_schema(self, schema_file: str):
        """טעינה ווולידציה של סכמה מתקדמת"""
        if not os.path.exists(schema_file):
            raise FileNotFoundError(f"Schema file {schema_file} not found")
        
        try:
            with open(schema_file, "r", encoding='utf-8') as f:
                self.schema: Dict[str, Dict[str, Any]] = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON schema: {e}")
        
        # וולידציה מתקדמת
        supported_types = ["int", "bool", "enum", "time", "float", "string"]
        for field_name, field_info in self.schema.items():
            field_type = field_info.get("type", "int")
            if field_type not in supported_types:
                raise ValueError(f"Unsupported type '{field_type}' for field {field_name}")

    def _compile_enhanced_schema(self):
        """קומפילציה מתקדמת של סכמה"""
        for key, info in self.schema.items():
            ftype = info.get("type", "int")
            
            if ftype == "bool":
                nbits = info.get("bits", 1)
            elif ftype == "enum":
                values = info["values"]
                # טיפול בstring enums
                if isinstance(values[0], str):
                    # String enum - נשמור את השמות ונעבוד עם אינדקסים
                    nbits = max(1, math.ceil(math.log2(len(values))))
                    info["_string_enum_map"] = {i: val for i, val in enumerate(values)}
                    info["_reverse_enum_map"] = {val: i for i, val in enumerate(values)}
                else:
                    # Int enum - כמו קודם
                    max_val = max(values)
                    nbits = max(1, math.ceil(math.log2(max_val + 1)))
            elif ftype == "float":
                nbits = info.get("bits", 32)  # IEEE 754 או קוונטיזציה
            elif ftype == "string":
                max_length = info.get("max_length", 16)
                nbits = 8 + (max_length * 8)  # length byte + characters
            else:  # int, time
                nbits = info.get("bits", 32)
            
            self._compiled_fields.append((key, info, nbits))

        # הוספת Sequential ID
        if self.add_sequential_id:
            self._compiled_fields = (
                [(self.id_field_name, {"type": "int", "bits": self.id_bits}, self.id_bits)]
                + self._compiled_fields
            )

    # ---------- מחוללי ערכים מתקדמים ----------
    def _gen_float(self, min_val: float = 0.0, max_val: float = 1.0) -> float:
        """מחולל float עם טווח"""
        return random.uniform(min_val, max_val)

    def _gen_string(self, length: int = 8, charset: str = None) -> str:
        """מחולל string"""
        if charset is None:
            charset = string.ascii_letters + string.digits
        return ''.join(random.choices(charset, k=length))

    def _gen_enum(self, info: Dict[str, Any]) -> Union[int, str]:
        """מחולל enum - תומך בint וstring"""
        values = info["values"]
        
        if isinstance(values[0], str):
            # String enum - בוחר ישירות מהרשימה
            return random.choice(values)
        else:
            # Int enum - כמו קודם
            return random.choice(values)

    def _gen_time_advanced(self, bits: int, base_time: int = None, 
                          time_range: int = 3600) -> int:
        """מחולל זמן מתקדם עם base time"""
        if base_time is None:
            base_time = int(time.time())
        
        offset = random.randint(0, time_range)
        return base_time + offset

    def generate_enhanced_record(self, seq_id: Optional[int] = None, 
                               record_type: RecordType = None) -> TelemetryRecord:
        """יוצר רשומת טלמטריה מתקדמת"""
        if record_type is None:
            record_type = self.default_record_type
        
        timestamp = time.time_ns()  # נאנו-שניות לדיוק גבוה
        
        if seq_id is None:
            seq_id = self._next_seq_id
            self._next_seq_id += 1

        # יצירת נתונים
        data = {}
        for key, info, nbits in self._compiled_fields:
            if self.add_sequential_id and key == self.id_field_name:
                continue  # נטפל בזה בנפרד
            
            ftype = info.get("type", "int")
            
            if ftype == "bool":
                data[key] = bool(random.randint(0, 1))
            elif ftype == "enum":
                data[key] = self._gen_enum(info)
            elif ftype == "float":
                min_val = info.get("min", 0.0)
                max_val = info.get("max", 1.0)
                data[key] = self._gen_float(min_val, max_val)
            elif ftype == "string":
                length = info.get("length", 8)
                data[key] = self._gen_string(length)
            elif ftype == "time":
                base_time = info.get("base_time")
                time_range = info.get("range", 3600)
                data[key] = self._gen_time_advanced(nbits, base_time, time_range)
            else:  # int
                max_val = (1 << nbits) - 1
                data[key] = random.randint(0, max_val)

        return TelemetryRecord(
            record_type=record_type,
            timestamp=timestamp,
            sequence_id=seq_id,
            data=data
        )

    # ---------- פורמטי פלט מרובים ----------
    def pack_record_enhanced(self, record: TelemetryRecord) -> bytes:
        """אריזת רשומה מתקדמת לבינארי"""
        packer = EnhancedBitPacker()
        
        # Record type (1 bit: 0=UPDATE, 1=EVENT)
        packer.write(1 if record.record_type == RecordType.EVENT else 0, 1)
        
        # Timestamp (64 bit)
        packer.write(record.timestamp & ((1 << 64) - 1), 64)
        
        # Sequential ID
        if self.add_sequential_id:
            packer.write(record.sequence_id, self.id_bits)
        
        # Data fields
        for key, info, nbits in self._compiled_fields:
            if self.add_sequential_id and key == self.id_field_name:
                continue
            
            ftype = info.get("type", "int")
            value = record.data[key]
            
            if ftype == "bool":
                packer.write(1 if value else 0, 1)
            elif ftype == "float":
                if info.get("encoding") == "ieee754":
                    packer.write_ieee754_float(value)
                else:
                    precision = info.get("precision_bits", 16)
                    packer.write_float(value, precision)
            elif ftype == "string":
                if info.get("compressed", False):
                    char_bits = info.get("char_bits", 6)
                    packer.write_compressed_string(value, char_bits)
                else:
                    max_length = info.get("max_length", 255)
                    packer.write_string(value, max_length)
            elif ftype == "enum":
                value = record.data[key]
                # טיפול בstring enum לפני אריזה
                if isinstance(value, str) and "_reverse_enum_map" in info:
                    # המרת string לindex
                    enum_index = info["_reverse_enum_map"].get(value, 0)
                    packer.write(enum_index, nbits)
                elif isinstance(value, int):
                    # Int enum רגיל
                    clamped_value = value & ((1 << nbits) - 1)
                    packer.write(clamped_value, nbits)
                else:
                    # Fallback - נסה להמיר לint
                    int_value = int(value) & ((1 << nbits) - 1)
                    packer.write(int_value, nbits)
        
        if self.record_padding_to_byte:
            packer.flush_to_byte_boundary()
        
        return packer.bytes()

    def format_influx_line(self, record: TelemetryRecord, measurement: str = "telemetry") -> str:
        """המרת רשומה לפורמט InfluxDB Line Protocol"""
        # Tags - מידע מטא
        tags = {
            "type": record.record_type.value,
            "seq_id": str(record.sequence_id)
        }
        
        # Fields - הנתונים בפועל
        fields = record.data.copy()
        
        # המרת timestamp לנאנושניות
        timestamp = record.timestamp
        
        return InfluxDBLineProtocolWriter.format_record(measurement, tags, fields, timestamp)

    def format_json(self, record: TelemetryRecord) -> str:
        """המרת רשומה לפורמט JSON"""
        return json.dumps({
            "type": record.record_type.value,
            "timestamp": record.timestamp,
            "sequence_id": record.sequence_id,
            "data": record.data
        })

    def format_ndjson(self, record: TelemetryRecord) -> str:
        """Format record as NDJSON (single line JSON)"""
        data = {
            'type': record.record_type.value,
            'timestamp': record.timestamp,
            'seq_id': record.sequence_id,
            'data': record.data
        }
        return json.dumps(data, separators=(',', ':')) + '\n'

    # ---------- כתיבה לקובץ במרובים פורמטים ----------
    # def write_records_enhanced(
    #     self,
    #     path: str,
    #     num_records: int,
    #     *,
    #     output_format: Union[OutputFormat, str] = OutputFormat.BINARY,
    #     start_seq_id: Optional[int] = None,
    #     record_type_ratio: Dict[RecordType, float] = None,
    #     measurement_name: str = "telemetry",
    #     progress_callback: Optional[callable] = None
    # ):
    #     """כתיבת רשומות בפורמטים שונים"""
    #     if start_seq_id is not None:
    #         self._next_seq_id = start_seq_id
        
    #     # ברירת מחדל ליחס סוגי רשומות
    #     if record_type_ratio is None:
    #         record_type_ratio = {RecordType.UPDATE: 0.7, RecordType.EVENT: 0.3}
        
    #     # הוסף תמיכה ב-NDJSON
    #     use_ndjson = False
    #     if isinstance(output_format, str) and output_format == 'ndjson':
    #         output_format = OutputFormat.JSON  # fallback ל-JSON
    #         use_ndjson = True
        
    #     os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
    #     mode = "wb" if output_format == OutputFormat.BINARY else "w"
    #     encoding = None if output_format == OutputFormat.BINARY else "utf-8"
        
    #     try:
    #         with open(path, mode, encoding=encoding) as f:
    #             for i in range(num_records):
    #                 # בחירת סוג רשומה לפי היחס
    #                 rand = random.random()
    #                 cumulative = 0
    #                 selected_type = RecordType.UPDATE
                    
    #                 for rtype, ratio in record_type_ratio.items():
    #                     cumulative += ratio
    #                     if rand <= cumulative:
    #                         selected_type = rtype
    #                         break
                    
    #                 # יצירת רשומה
    #                 record = self.generate_enhanced_record(record_type=selected_type)
                    
    #                 # כתיבה לפי הפורמט
    #                 if use_ndjson:
    #                     f.write(self.format_ndjson(record))
    #                 elif output_format == OutputFormat.BINARY:
    #                     f.write(self.pack_record_enhanced(record))
    #                 elif output_format == OutputFormat.INFLUX_LINE:
    #                     f.write(self.format_influx_line(record, measurement_name))
    #                 elif output_format == OutputFormat.JSON:
    #                     f.write(self.format_json(record) + "\n")
                    
    #                 # עדכון התקדמות
    #                 if progress_callback and i % 1000 == 0:
    #                     progress_callback(i, num_records)
            
    #         self.logger.info(f"Successfully wrote {num_records} records to {path} in {output_format.value if hasattr(output_format, 'value') else output_format} format")
            
    #     except Exception as e:
    #         self.logger.error(f"Error writing enhanced records to {path}: {e}")
    #         raise
    def write_records_enhanced(
        self,
        path: str,
        num_records: int,
        *,
        output_format: Union[OutputFormat, str] = OutputFormat.BINARY,
        start_seq_id: Optional[int] = None,
        record_type_ratio: Dict[RecordType, float] = None,
        measurement_name: str = "telemetry",
        progress_callback: Optional[callable] = None
    ):
        """כתיבת רשומות בפורמטים שונים"""
        if start_seq_id is not None:
            self._next_seq_id = start_seq_id
        
        # ברירת מחדל ליחס סוגי רשומות
        if record_type_ratio is None:
            record_type_ratio = {RecordType.UPDATE: 0.7, RecordType.EVENT: 0.3}
        
        # הוסף תמיכה ב-NDJSON
        use_ndjson = False
        if isinstance(output_format, str) and output_format == 'ndjson':
            output_format = OutputFormat.JSON  # fallback ל-JSON
            use_ndjson = True
        
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
        mode = "wb" if output_format == OutputFormat.BINARY else "w"
        encoding = None if output_format == OutputFormat.BINARY else "utf-8"
        
        try:
            with open(path, mode, encoding=encoding) as f:
                # For JSON format, collect all records first
                if output_format == OutputFormat.JSON and not use_ndjson:
                    records_data = []
                
                for i in range(num_records):
                    # בחירת סוג רשומה לפי היחס
                    rand = random.random()
                    cumulative = 0
                    selected_type = RecordType.UPDATE
                    
                    for rtype, ratio in record_type_ratio.items():
                        cumulative += ratio
                        if rand <= cumulative:
                            selected_type = rtype
                            break
                    
                    # יצירת רשומה
                    record = self.generate_enhanced_record(record_type=selected_type)
                    
                    # כתיבה לפי הפורמט
                    if use_ndjson:
                        f.write(self.format_ndjson(record))
                    elif output_format == OutputFormat.BINARY:
                        f.write(self.pack_record_enhanced(record))
                    elif output_format == OutputFormat.INFLUX_LINE:
                        f.write(self.format_influx_line(record, measurement_name))
                    elif output_format == OutputFormat.JSON:
                        # Collect record data for JSON array
                        record_dict = {
                            "type": record.record_type.value,
                            "timestamp": record.timestamp,
                            "sequence_id": record.sequence_id,
                            "data": record.data
                        }
                        records_data.append(record_dict)
                    
                    # עדכון התקדמות
                    if progress_callback and i % 1000 == 0:
                        progress_callback(i, num_records)
                
                # Write JSON array at the end
                if output_format == OutputFormat.JSON and not use_ndjson:
                    json.dump(records_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Successfully wrote {num_records} records to {path} in {output_format.value if hasattr(output_format, 'value') else output_format} format")
            
        except Exception as e:
            self.logger.error(f"Error writing enhanced records to {path}: {e}")
            raise
    # ---------- יצירה עם GPU acceleration ----------
    def generate_batch_gpu_accelerated(
        self, 
        batch_size: int,
        record_type: RecordType = RecordType.UPDATE
    ) -> List[TelemetryRecord]:
        """יצירת batch של רשומות עם האצת GPU"""
        if not self.gpu_generator:
            # Fallback ליצירה רגילה
            return [self.generate_enhanced_record(record_type=record_type) 
                   for _ in range(batch_size)]
        
        records = []
        timestamp_base = time.time_ns()
        
        # חישוב מספר שדות מכל סוג
        int_fields = []
        float_fields = []
        
        for key, info, nbits in self._compiled_fields:
            if self.add_sequential_id and key == self.id_field_name:
                continue
            
            ftype = info.get("type", "int")
            if ftype in ["int", "enum", "bool", "time"]:
                int_fields.append((key, info, nbits))
            elif ftype == "float":
                float_fields.append((key, info))
        
        # יצירת batches עם GPU
        if int_fields:
            int_batch = self.gpu_generator.generate_batch_int(
                batch_size, len(int_fields), 
                max_bits=max(nbits for _, _, nbits in int_fields)
            )
        
        if float_fields:
            float_batch = self.gpu_generator.generate_batch_float(
                batch_size, len(float_fields)
            )
        
        # הרכבת רשומות
        for i in range(batch_size):
            data = {}
            
            # שדות int
            for j, (key, info, nbits) in enumerate(int_fields):
                ftype = info.get("type", "int")
                raw_value = int_batch[i][j] if int_fields else 0
                
                if ftype == "bool":
                    data[key] = bool(raw_value % 2)
                elif ftype == "enum":
                    values = info["values"]
                    if isinstance(values[0], str):
                        # String enum
                        data[key] = values[raw_value % len(values)]
                    else:
                        # Int enum
                        data[key] = values[raw_value % len(values)]
                else:
                    # חיתוך לטווח הנכון
                    max_val = (1 << nbits) - 1
                    data[key] = raw_value & max_val
            
            # שדות float
            for j, (key, info) in enumerate(float_fields):
                raw_value = float_batch[i][j] if float_fields else 0.0
                min_val = info.get("min", 0.0)
                max_val = info.get("max", 1.0)
                data[key] = min_val + raw_value * (max_val - min_val)
            
            # שדות string (לא מואצים בGPU)
            for key, info, _ in self._compiled_fields:
                if info.get("type") == "string":
                    length = info.get("length", 8)
                    data[key] = self._gen_string(length)
            
            record = TelemetryRecord(
                record_type=record_type,
                timestamp=timestamp_base + i,  # timestamps עוקבים
                sequence_id=self._next_seq_id + i,
                data=data
            )
            records.append(record)
        
        self._next_seq_id += batch_size
        return records

    # ---------- יצירה מקבילית מתקדמת ----------
    def generate_multiple_files_enhanced(
        self,
        *,
        num_files: int,
        records_per_file: int,
        file_prefix: str = "telemetry",
        output_format: OutputFormat = OutputFormat.BINARY,
        max_workers: int = 4,
        start_seq_id: int = 0,
        use_gpu_batches: bool = False,
        batch_size: int = 1000,
        record_type_ratio: Dict[RecordType, float] = None
    ) -> List[str]:
        """יצירת קבצים מרובים עם תכונות מתקדמות"""
        paths: List[str] = []
        futures = []
        
        # הגדרת סיומת לפי פורמט
        extensions = {
            OutputFormat.BINARY: ".bin",
            OutputFormat.INFLUX_LINE: ".txt",
            OutputFormat.JSON: ".json"
        }
        ext = extensions[output_format]
        
        total_records = num_files * records_per_file
        self.logger.info(f"Starting enhanced generation: {num_files} files × {records_per_file} records = {total_records} total")
        
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i in range(num_files):
                path = os.path.join(self.output_dir, f"{file_prefix}_{i:04d}{ext}")
                paths.append(path)
                
                file_start_seq = start_seq_id + i * records_per_file
                futures.append(
                    ex.submit(
                        self._write_enhanced_file_isolated,
                        path, records_per_file, file_start_seq,
                        output_format, use_gpu_batches, batch_size, record_type_ratio
                    )
                )
            
            completed = 0
            for future in as_completed(futures):
                try:
                    future.result()
                    completed += 1
                    self.logger.info(f"Enhanced generation: {completed}/{num_files} files completed")
                except Exception as e:
                    self.logger.error(f"Error in enhanced parallel generation: {e}")
                    raise
        
        self.logger.info(f"Successfully generated all {num_files} enhanced files")
        return paths

    def _write_enhanced_file_isolated(
        self, 
        path: str, 
        num_records: int, 
        file_start_seq: int,
        output_format: OutputFormat,
        use_gpu_batches: bool,
        batch_size: int,
        record_type_ratio: Dict[RecordType, float]
    ):
        """כתיבת קובץ מתקדם במצב מבודד"""
        try:
            # יצירת קובץ schema זמני לthread המבודד
            temp_schema_file = os.path.join(
                os.path.dirname(path) or ".", 
                f"temp_schema_{os.getpid()}_{threading.current_thread().ident}.json"
            )
            
            # כתיבת הsschema לקובץ זמני
            with open(temp_schema_file, 'w') as f:
                json.dump(self.schema, f)
            
            try:
                # יצירת instance מבודד שטוען מקובץ JSON
                isolated = EnhancedTelemetryGeneratorPro(
                    schema_file=temp_schema_file,
                    add_sequential_id=self.add_sequential_id,
                    id_field_name=self.id_field_name,
                    id_bits=self.id_bits,
                    output_dir=os.path.dirname(path) or ".",
                    record_padding_to_byte=self.record_padding_to_byte,
                    default_record_type=self.default_record_type,
                    enable_gpu=False,  # לא GPU בthreads מבודדים
                    logger=self.logger
                )
                
                isolated._next_seq_id = file_start_seq
                
                # כתיבה רגילה (ללא GPU batches בthread מבודד)
                isolated.write_records_enhanced(
                    path, num_records,
                    output_format=output_format,
                    record_type_ratio=record_type_ratio
                )
                
            finally:
                # ניקוי קובץ schema זמני
                if os.path.exists(temp_schema_file):
                    os.remove(temp_schema_file)
                
        except Exception as e:
            self.logger.error(f"Error in isolated enhanced file writing for {path}: {e}")
            raise

    def _write_with_gpu_batches(
        self,
        path: str,
        num_records: int,
        output_format: OutputFormat,
        batch_size: int,
        record_type_ratio: Dict[RecordType, float]
    ):
        """כתיבה עם batches של GPU"""
        mode = "wb" if output_format == OutputFormat.BINARY else "w"
        encoding = None if output_format == OutputFormat.BINARY else "utf-8"
        
        remaining = num_records
        
        with open(path, mode, encoding=encoding) as f:
            while remaining > 0:
                current_batch_size = min(batch_size, remaining)
                
                # בחירת סוג רשומה לבatch
                record_type = RecordType.UPDATE
                if record_type_ratio:
                    rand = random.random()
                    cumulative = 0
                    for rtype, ratio in record_type_ratio.items():
                        cumulative += ratio
                        if rand <= cumulative:
                            record_type = rtype
                            break
                
                # יצירת batch עם GPU
                batch = self.generate_batch_gpu_accelerated(current_batch_size, record_type)
                
                # כתיבת הbatch
                for record in batch:
                    if output_format == OutputFormat.BINARY:
                        f.write(self.pack_record_enhanced(record))
                    elif output_format == OutputFormat.INFLUX_LINE:
                        f.write(self.format_influx_line(record))
                    elif output_format == OutputFormat.JSON:
                        f.write(self.format_json(record) + "\n")
                
                remaining -= current_batch_size

    # ---------- עטיפות async מתקדמות ----------
    async def generate_multiple_files_enhanced_async(
        self,
        *,
        num_files: int,
        records_per_file: int,
        file_prefix: str = "telemetry",
        output_format: OutputFormat = OutputFormat.BINARY,
        max_workers: int = 4,
        start_seq_id: int = 0,
        use_gpu_batches: bool = False,
        batch_size: int = 1000,
        record_type_ratio: Dict[RecordType, float] = None
    ) -> List[str]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.generate_multiple_files_enhanced(
                num_files=num_files,
                records_per_file=records_per_file,
                file_prefix=file_prefix,
                output_format=output_format,
                max_workers=max_workers,
                start_seq_id=start_seq_id,
                use_gpu_batches=use_gpu_batches,
                batch_size=batch_size,
                record_type_ratio=record_type_ratio
            )
        )

    # ---------- שיטות עזר מתקדמות ----------
    def get_enhanced_schema_info(self) -> Dict[str, Any]:
        """מידע מפורט על סכמה מתקדמת"""
        total_bits = 0
        field_details = []
        
        # Record type + timestamp + seq_id overhead
        overhead_bits = 1 + 64 + (self.id_bits if self.add_sequential_id else 0)
        total_bits += overhead_bits
        
        for name, info, nbits in self._compiled_fields:
            if self.add_sequential_id and name == self.id_field_name:
                continue
            
            field_details.append({
                "name": name,
                "type": info.get("type"),
                "bits": nbits,
                "details": info
            })
            total_bits += nbits
        
        return {
            "fields_count": len(self._compiled_fields),
            "data_fields_count": len(field_details),
            "overhead_bits": overhead_bits,
            "total_bits_per_record": total_bits,
            "bytes_per_record": math.ceil(total_bits / 8),
            "supports_gpu": bool(self.gpu_generator),
            "supported_formats": [f.value for f in OutputFormat],
            "fields": field_details
        }

    def estimate_storage_requirements(
        self, 
        num_records: int, 
        output_format: OutputFormat = OutputFormat.BINARY,
        compression_ratio: float = 1.0
    ) -> Dict[str, int]:
        """הערכת דרישות אחסון"""
        info = self.get_enhanced_schema_info()
        
        if output_format == OutputFormat.BINARY:
            base_size = num_records * info["bytes_per_record"]
        elif output_format == OutputFormat.JSON:
            # הערכה גסה - JSON גדול פי 3-5 מבינארי
            base_size = num_records * info["bytes_per_record"] * 4
        else:  # InfluxDB Line Protocol
            # הערכה גסה - דומה לJSON אבל קצת יותר קומפקטי
            base_size = num_records * info["bytes_per_record"] * 3
        
        compressed_size = int(base_size * compression_ratio)
        
        return {
            "records": num_records,
            "format": output_format.value,
            "uncompressed_bytes": base_size,
            "compressed_bytes": compressed_size,
            "uncompressed_mb": base_size / (1024 * 1024),
            "compressed_mb": compressed_size / (1024 * 1024),
            "compression_ratio": compression_ratio
        }

    def benchmark_generation_speed(
        self, 
        test_records: int = 10000,
        use_gpu: bool = False,
        batch_size: int = 1000
    ) -> Dict[str, float]:
        """בנצ'מרק מהירות יצירה"""
        import time
        
        results = {}
        
        # בדיקה רגילה
        start = time.time()
        for _ in range(test_records):
            self.generate_enhanced_record()
        regular_time = time.time() - start
        results["regular_records_per_sec"] = test_records / regular_time
        
        # בדיקה עם GPU (אם זמין)
        if use_gpu and self.gpu_generator:
            start = time.time()
            batches = test_records // batch_size
            for _ in range(batches):
                self.generate_batch_gpu_accelerated(batch_size)
            gpu_time = time.time() - start
            results["gpu_records_per_sec"] = (batches * batch_size) / gpu_time
            results["gpu_speedup"] = results["gpu_records_per_sec"] / results["regular_records_per_sec"]
        
        return results

    # ---------- Context manager support ----------
    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # ניקוי אם צריך
        pass
















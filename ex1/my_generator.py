# import os
# import json
# import math
# import random
# from typing import Dict, Any, List, Union, Optional, Tuple
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import asyncio
# import logging

# Number = Union[int, float]

# class BitPacker:
#     """
#     אורז ערכים לפי מספר ביטים לכל ערך אל תוך buffer של bytes.
#     """
#     def __init__(self):
#         self._out: List[int] = []
#         self._bitbuf: int = 0
#         self._bitcount: int = 0

#     def write(self, value: int, nbits: int):
#         if nbits <= 0:
#             return
#         if value < 0 or value >= (1 << nbits):
#             raise ValueError(f"value {value} doesn't fit in {nbits} bits")
        
#         # דוחפים את הערך למאגר הביטים
#         self._bitbuf = (self._bitbuf << nbits) | value
#         self._bitcount += nbits
        
#         # מרוקנים כל פעם שיש לנו לפחות 8 ביטים
#         while self._bitcount >= 8:
#             self._bitcount -= 8
#             byte = (self._bitbuf >> self._bitcount) & 0xFF
#             self._out.append(byte)
#             self._bitbuf &= (1 << self._bitcount) - 1

#     def flush_to_byte_boundary(self, pad_with_zero: bool = True):
#         """ממלא בביטים 0 עד לגבול של בייט (אם צריך)"""
#         if self._bitcount > 0:
#             if pad_with_zero:
#                 # דוחפים את הביטים שנותרו לשמאל ומשלימים לאוקטט
#                 byte = (self._bitbuf << (8 - self._bitcount)) & 0xFF
#                 self._out.append(byte)
#             # איפוס המאגר
#             self._bitbuf = 0
#             self._bitcount = 0

#     def bytes(self) -> bytes:
#         return bytes(self._out)


# class TelemetryGeneratorPro:
#     """
#     - מקבל schema גנרי (JSON) עם שדות וסוגים/ביטים/טווחים/ערכי enum.
#     - מייצר רשומות רנדומליות בהתאם לסכמה.
#     - אורז לרצף בינארי ב-bit packing אמיתי לפי ה-bits של כל שדה.
#     - תומך ב-ID ייחודי עוקב (אופציונלי).
#     - יוצר ריבוי קבצים במקביל לדימוי עומס.
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
#         logger: Optional[logging.Logger] = None
#     ):
#         self.logger = logger or logging.getLogger(__name__)
        
#         # ---- וולידציה וקריאה מהקובץ JSON ----
#         if not os.path.exists(schema_file):
#             raise FileNotFoundError(f"Schema file {schema_file} not found")
        
#         try:
#             with open(schema_file, "r", encoding='utf-8') as f:
#                 self.schema: Dict[str, Dict[str, Any]] = json.load(f)
#         except json.JSONDecodeError as e:
#             raise ValueError(f"Invalid JSON schema in {schema_file}: {e}")
#         except Exception as e:
#             raise RuntimeError(f"Error reading schema file {schema_file}: {e}")
        
#         # וולידציה בסיסית של הסכמה
#         self._validate_schema()
        
#         self.add_sequential_id = add_sequential_id
#         self.id_field_name = id_field_name
#         self.id_bits = id_bits
#         self.output_dir = output_dir
#         self.record_padding_to_byte = record_padding_to_byte
#         self._next_seq_id = 0

#         # הכנה מוקדמת: חישוב כמה ביטים לכל שדה
#         self._compiled_fields: List[Tuple[str, Dict[str, Any], int]] = []
#         self._compile_schema()
        
#         self.logger.info(f"Initialized TelemetryGeneratorPro with {len(self._compiled_fields)} fields")

#     def _validate_schema(self):
#         """וולידציה של הסכמה"""
#         if not isinstance(self.schema, dict):
#             raise ValueError("Schema must be a dictionary")
        
#         for field_name, field_info in self.schema.items():
#             if not isinstance(field_info, dict):
#                 raise ValueError(f"Field {field_name} info must be a dictionary")
            
#             field_type = field_info.get("type", "int")
#             if field_type not in ["int", "bool", "enum", "time"]:
#                 raise ValueError(f"Unsupported field type '{field_type}' for field {field_name}")
            
#             if field_type == "enum" and "values" not in field_info:
#                 raise ValueError(f"Enum field {field_name} must have 'values' property")

#     def _compile_schema(self):
#         """חישוב מספר הביטים לכל שדה"""
#         for key, info in self.schema.items():
#             ftype = info.get("type", "int")
            
#             if ftype == "bool":
#                 nbits = info.get("bits", 1)
#             elif ftype == "enum":
#                 values = info["values"]
#                 if not values:
#                     raise ValueError(f"Enum field {key} has empty values list")
#                 max_val = max(values)
#                 nbits = max(1, math.ceil(math.log2(max_val + 1)))
#             else:  # int, time
#                 nbits = info.get("bits", 32)
            
#             # וולידציה של מספר הביטים
#             if nbits <= 0 or nbits > 64:
#                 raise ValueError(f"Invalid bits count {nbits} for field {key}")
            
#             self._compiled_fields.append((key, info, nbits))

#         if self.add_sequential_id:
#             self._compiled_fields = (
#                 [(self.id_field_name, {"type": "int", "bits": self.id_bits}, self.id_bits)]
#                 + self._compiled_fields
#             )

#     # ---------- יצירת ערכים לפי הסכמה ----------
#     def _gen_int(self, bits: int) -> int:
#         return random.randint(0, (1 << bits) - 1)

#     def _gen_bool(self) -> int:
#         return random.randint(0, 1)

#     def _gen_enum(self, values: List[int]) -> int:
#         return random.choice(values)

#     def _gen_time(self, bits: int, rng: int) -> int:
#         # ממפה 0..(2^bits-1) → 0..range
#         base = random.randint(0, (1 << bits) - 1)
#         return base * rng // ((1 << bits) - 1)

#     def generate_record_values(self, seq_id: Optional[int] = None) -> List[int]:
#         """
#         מחזיר רשימת ערכים לפי הסדר הקבוע של _compiled_fields.
#         (לא מילון – כדי לשמור סדר דטרמיניסטי ומיפוי פשוט ל-bit packing)
#         """
#         values: List[int] = []
        
#         for key, info, nbits in self._compiled_fields:
#             ftype = info.get("type", "int")
            
#             if self.add_sequential_id and key == self.id_field_name:
#                 val = self._next_seq_id if seq_id is None else seq_id
#             elif ftype == "bool":
#                 val = self._gen_bool()
#             elif ftype == "enum":
#                 val = self._gen_enum(info["values"])
#             elif ftype == "time":
#                 rng = info.get("range", (1 << nbits) - 1)
#                 val = self._gen_time(nbits, rng)
#             else:  # int
#                 val = self._gen_int(nbits)

#             # וידוא התאמה ל-nbits
#             if val < 0 or val >= (1 << nbits):
#                 val &= (1 << nbits) - 1
            
#             values.append(val)

#         # עדכון ה-ID העוקב
#         if self.add_sequential_id and seq_id is None:
#             self._next_seq_id += 1
        
#         return values

#     # ---------- אריזת רשומה לבינארי ----------
#     def pack_record(self, values: List[int]) -> bytes:
#         """אורז רשומה אחת לבינארי"""
#         packer = BitPacker()
        
#         for (_, _, nbits), val in zip(self._compiled_fields, values):
#             packer.write(val, nbits)
        
#         if self.record_padding_to_byte:
#             packer.flush_to_byte_boundary()
        
#         return packer.bytes()

#     # ---------- כתיבה לקובץ יחיד ----------
#     def write_records_to_file(
#         self,
#         path: str,
#         num_records: int,
#         *,
#         start_seq_id: Optional[int] = None,
#         progress_callback: Optional[callable] = None
#     ):
#         """
#         כותב num_records רשומות לקובץ path, בסדר דטרמיניסטי.
#         """
#         if start_seq_id is not None and self.add_sequential_id:
#             self._next_seq_id = start_seq_id

#         os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
#         try:
#             with open(path, "wb") as f:
#                 for i in range(num_records):
#                     seq = None
#                     if self.add_sequential_id:
#                         seq = (start_seq_id + i) if start_seq_id is not None else None
                    
#                     values = self.generate_record_values(seq_id=seq)
#                     f.write(self.pack_record(values))
                    
#                     # קריאה ל-callback לעדכון התקדמות
#                     if progress_callback and i % 1000 == 0:
#                         progress_callback(i, num_records)
            
#             self.logger.info(f"Successfully wrote {num_records} records to {path}")
            
#         except Exception as e:
#             self.logger.error(f"Error writing to {path}: {e}")
#             raise

#     # ---------- ריבוי קבצים במקביל (threads) ----------
#     def generate_multiple_files_parallel(
#         self,
#         *,
#         num_files: int,
#         records_per_file: int,
#         file_prefix: str = "telemetry",
#         max_workers: int = 4,
#         start_seq_id: int = 0
#     ) -> List[str]:
#         """
#         יוצר num_files קבצים במקביל, כל קובץ עם records_per_file רשומות.
#         כל קובץ מקבל טווח ID ייחודי, שומרת סדר דטרמיניסטי בתוך כל קובץ.
#         מחזיר רשימת נתיבי הקבצים.
#         """
#         paths: List[str] = []
#         futures = []
        
#         total_records = num_files * records_per_file
#         self.logger.info(f"Starting generation of {num_files} files with {records_per_file} records each (total: {total_records})")
        
#         with ThreadPoolExecutor(max_workers=max_workers) as ex:
#             for i in range(num_files):
#                 path = os.path.join(self.output_dir, f"{file_prefix}_{i:04d}.bin")
#                 paths.append(path)
                
#                 # לכל קובץ טווח ID נפרד, ללא התנגשויות
#                 file_start_seq = start_seq_id + i * records_per_file
#                 futures.append(
#                     ex.submit(self._write_file_isolated, path, records_per_file, file_start_seq)
#                 )
            
#             # המתנה לסיום כל המשימות
#             completed = 0
#             for future in as_completed(futures):
#                 try:
#                     future.result()  # מעלה חריגות אם היו
#                     completed += 1
#                     self.logger.info(f"Completed {completed}/{num_files} files")
#                 except Exception as e:
#                     self.logger.error(f"Error in parallel file generation: {e}")
#                     raise
        
#         self.logger.info(f"Successfully generated all {num_files} files")
#         return paths

#     def _write_file_isolated(self, path: str, num_records: int, file_start_seq: int):
#         """
#         כותב קובץ בקונטקסט 'מבודד' כדי שלא יתנגש ב-ID העוקב של אובייקטים אחרים.
#         יוצרת גנרטור חדש עם אותו schema ופרמטרים (ללא שיתוף מצב).
#         """
#         try:
#             # יצירת instance מבודד
#             isolated = TelemetryGeneratorPro(
#                 schema_file="",  # נעביר את הסכמה ישירות
#                 add_sequential_id=self.add_sequential_id,
#                 id_field_name=self.id_field_name,
#                 id_bits=self.id_bits,
#                 output_dir=os.path.dirname(path) or ".",
#                 record_padding_to_byte=self.record_padding_to_byte,
#                 logger=self.logger
#             )
            
#             # העברת הסכמה ישירות
#             isolated.schema = self.schema.copy()
#             isolated._compile_schema()
            
#             isolated.write_records_to_file(path, num_records, start_seq_id=file_start_seq)
            
#         except Exception as e:
#             self.logger.error(f"Error in isolated file writing for {path}: {e}")
#             raise

#     # ---------- עטיפות asyncio (אופציונלי) ----------
#     async def generate_multiple_files_async(
#         self,
#         *,
#         num_files: int,
#         records_per_file: int,
#         file_prefix: str = "telemetry",
#         max_workers: int = 4,
#         start_seq_id: int = 0
#     ) -> List[str]:
#         loop = asyncio.get_running_loop()
#         return await loop.run_in_executor(
#             None,
#             lambda: self.generate_multiple_files_parallel(
#                 num_files=num_files,
#                 records_per_file=records_per_file,
#                 file_prefix=file_prefix,
#                 max_workers=max_workers,
#                 start_seq_id=start_seq_id,
#             )
#         )

#     # ---------- שיטות עזר ----------
#     def get_schema_info(self) -> Dict[str, Any]:
#         """מחזיר מידע על הסכמה הנוכחית"""
#         total_bits = sum(nbits for _, _, nbits in self._compiled_fields)
#         total_bytes = math.ceil(total_bits / 8)
        
#         return {
#             "fields_count": len(self._compiled_fields),
#             "total_bits_per_record": total_bits,
#             "bytes_per_record": total_bytes,
#             "fields": [
#                 {"name": name, "type": info.get("type"), "bits": nbits}
#                 for name, info, nbits in self._compiled_fields
#             ]
#         }

#     def estimate_file_size(self, num_records: int) -> int:
#         """מעריך את גודל הקובץ ב-bytes עבור מספר רשומות נתון"""
#         info = self.get_schema_info()
#         return num_records * info["bytes_per_record"]
import os
import json
import math
import random
from typing import Dict, Any, List, Union, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import logging

Number = Union[int, float]

class BitPacker:
    """
    אורז ערכים לפי מספר ביטים לכל ערך אל תוך buffer של bytes.
    """
    def __init__(self):
        self._out: List[int] = []
        self._bitbuf: int = 0
        self._bitcount: int = 0

    def write(self, value: int, nbits: int):
        if nbits <= 0:
            return
        if value < 0 or value >= (1 << nbits):
            raise ValueError(f"value {value} doesn't fit in {nbits} bits")
        
        # דוחפים את הערך למאגר הביטים
        self._bitbuf = (self._bitbuf << nbits) | value
        self._bitcount += nbits
        
        # מרוקנים כל פעם שיש לנו לפחות 8 ביטים
        while self._bitcount >= 8:
            self._bitcount -= 8
            byte = (self._bitbuf >> self._bitcount) & 0xFF
            self._out.append(byte)
            self._bitbuf &= (1 << self._bitcount) - 1

    def flush_to_byte_boundary(self, pad_with_zero: bool = True):
        """ממלא בביטים 0 עד לגבול של בייט (אם צריך)"""
        if self._bitcount > 0:
            if pad_with_zero:
                # דוחפים את הביטים שנותרו לשמאל ומשלימים לאוקטט
                byte = (self._bitbuf << (8 - self._bitcount)) & 0xFF
                self._out.append(byte)
            # איפוס המאגר
            self._bitbuf = 0
            self._bitcount = 0

    def bytes(self) -> bytes:
        return bytes(self._out)


class TelemetryGeneratorPro:
    """
    - מקבל schema גנרי (JSON) עם שדות וסוגים/ביטים/טווחים/ערכי enum.
    - מייצר רשומות רנדומליות בהתאם לסכמה.
    - אורז לרצף בינארי ב-bit packing אמיתי לפי ה-bits של כל שדה.
    - תומך ב-ID ייחודי עוקב (אופציונלי).
    - יוצר ריבוי קבצים במקביל לדימוי עומס.
    """

    def __init__(
        self,
        schema_file: str = None,
        *,
        schema_dict: Optional[Dict[str, Dict[str, Any]]] = None,
        add_sequential_id: bool = True,
        id_field_name: str = "_seq_id",
        id_bits: int = 64,
        output_dir: str = ".",
        record_padding_to_byte: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.schema_file = schema_file

        # ---- וולידציה וקריאה מהקובץ JSON או מהדיקט ----
        if schema_dict is not None:
            # אם נתנו dictionary ישירות
            self.schema = schema_dict.copy()
        elif schema_file and os.path.exists(schema_file):
            # קריאה מקובץ
            try:
                with open(schema_file, "r", encoding='utf-8') as f:
                    self.schema: Dict[str, Dict[str, Any]] = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON schema in {schema_file}: {e}")
            except Exception as e:
                raise RuntimeError(f"Error reading schema file {schema_file}: {e}")
        elif schema_file:
            raise FileNotFoundError(f"Schema file {schema_file} not found")
        else:
            raise ValueError("Must provide either schema_file or schema_dict")
        
        # וולידציה בסיסית של הסכמה
        self._validate_schema()
        
        self.add_sequential_id = add_sequential_id
        self.id_field_name = id_field_name
        self.id_bits = id_bits
        self.output_dir = output_dir
        self.record_padding_to_byte = record_padding_to_byte
        self._next_seq_id = 0

        # הכנה מוקדמת: חישוב כמה ביטים לכל שדה
        self._compiled_fields: List[Tuple[str, Dict[str, Any], int]] = []
        self._compile_schema()
        
        self.logger.info(f"Initialized TelemetryGeneratorPro with {len(self._compiled_fields)} fields")

    def _validate_schema(self):
        """וולידציה של הסכמה"""
        if not isinstance(self.schema, dict):
            raise ValueError("Schema must be a dictionary")
        
        for field_name, field_info in self.schema.items():
            if not isinstance(field_info, dict):
                raise ValueError(f"Field {field_name} info must be a dictionary")
            
            field_type = field_info.get("type", "int")
            if field_type not in ["int", "bool", "enum", "time"]:
                raise ValueError(f"Unsupported field type '{field_type}' for field {field_name}")
            
            if field_type == "enum" and "values" not in field_info:
                raise ValueError(f"Enum field {field_name} must have 'values' property")

    def _compile_schema(self):
        """חישוב מספר הביטים לכל שדה"""
        for key, info in self.schema.items():
            ftype = info.get("type", "int")
            
            if ftype == "bool":
                nbits = info.get("bits", 1)
            elif ftype == "enum":
                values = info["values"]
                if not values:
                    raise ValueError(f"Enum field {key} has empty values list")
                max_val = max(values)
                nbits = max(1, math.ceil(math.log2(max_val + 1)))
            else:  # int, time
                nbits = info.get("bits", 32)
            
            # וולידציה של מספר הביטים
            if nbits <= 0 or nbits > 64:
                raise ValueError(f"Invalid bits count {nbits} for field {key}")
            
            self._compiled_fields.append((key, info, nbits))

        if self.add_sequential_id:
            self._compiled_fields = (
                [(self.id_field_name, {"type": "int", "bits": self.id_bits}, self.id_bits)]
                + self._compiled_fields
            )

    # ---------- יצירת ערכים לפי הסכמה ----------
    def _gen_int(self, bits: int) -> int:
        return random.randint(0, (1 << bits) - 1)

    def _gen_bool(self) -> int:
        return random.randint(0, 1)

    def _gen_enum(self, values: List[int]) -> int:
        return random.choice(values)

    def _gen_time(self, bits: int, rng: int) -> int:
        # ממפה 0..(2^bits-1) → 0..range
        base = random.randint(0, (1 << bits) - 1)
        return base * rng // ((1 << bits) - 1)

    def generate_record_values(self, seq_id: Optional[int] = None) -> List[int]:
        """
        מחזיר רשימת ערכים לפי הסדר הקבוע של _compiled_fields.
        (לא מילון – כדי לשמור סדר דטרמיניסטי ומיפוי פשוט ל-bit packing)
        """
        values: List[int] = []
        
        for key, info, nbits in self._compiled_fields:
            ftype = info.get("type", "int")
            
            if self.add_sequential_id and key == self.id_field_name:
                val = self._next_seq_id if seq_id is None else seq_id
            elif ftype == "bool":
                val = self._gen_bool()
            elif ftype == "enum":
                val = self._gen_enum(info["values"])
            elif ftype == "time":
                rng = info.get("range", (1 << nbits) - 1)
                val = self._gen_time(nbits, rng)
            else:  # int
                val = self._gen_int(nbits)

            # וידוא התאמה ל-nbits
            if val < 0 or val >= (1 << nbits):
                val &= (1 << nbits) - 1
            
            values.append(val)

        # עדכון ה-ID העוקב
        if self.add_sequential_id and seq_id is None:
            self._next_seq_id += 1
        
        return values

    # ---------- אריזת רשומה לבינארי ----------
    def pack_record(self, values: List[int]) -> bytes:
        """אורז רשומה אחת לבינארי"""
        packer = BitPacker()
        
        for (_, _, nbits), val in zip(self._compiled_fields, values):
            packer.write(val, nbits)
        
        if self.record_padding_to_byte:
            packer.flush_to_byte_boundary()
        
        return packer.bytes()

    # ---------- כתיבה לקובץ יחיד ----------
    def write_records_to_file(
        self,
        path: str,
        num_records: int,
        *,
        start_seq_id: Optional[int] = None,
        progress_callback: Optional[callable] = None
    ):
        """
        כותב num_records רשומות לקובץ path, בסדר דטרמיניסטי.
        """
        if start_seq_id is not None and self.add_sequential_id:
            self._next_seq_id = start_seq_id

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
        try:
            with open(path, "wb") as f:
                for i in range(num_records):
                    seq = None
                    if self.add_sequential_id:
                        seq = (start_seq_id + i) if start_seq_id is not None else None
                    
                    values = self.generate_record_values(seq_id=seq)
                    f.write(self.pack_record(values))
                    
                    # קריאה ל-callback לעדכון התקדמות
                    if progress_callback and i % 1000 == 0:
                        progress_callback(i, num_records)
            
            self.logger.info(f"Successfully wrote {num_records} records to {path}")
            
        except Exception as e:
            self.logger.error(f"Error writing to {path}: {e}")
            raise

    # ---------- ריבוי קבצים במקביל (threads) ----------
    def generate_multiple_files_parallel(
        self,
        *,
        num_files: int,
        records_per_file: int,
        file_prefix: str = "telemetry",
        max_workers: int = 4,
        start_seq_id: int = 0
    ) -> List[str]:
        """
        יוצר num_files קבצים במקביל, כל קובץ עם records_per_file רשומות.
        כל קובץ מקבל טווח ID ייחודי, שומרת סדר דטרמיניסטי בתוך כל קובץ.
        מחזיר רשימת נתיבי הקבצים.
        """
        paths: List[str] = []
        futures = []
        
        total_records = num_files * records_per_file
        self.logger.info(f"Starting generation of {num_files} files with {records_per_file} records each (total: {total_records})")
        
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i in range(num_files):
                path = os.path.join(self.output_dir, f"{file_prefix}_{i:04d}.bin")
                paths.append(path)
                
                # לכל קובץ טווח ID נפרד, ללא התנגשויות
                file_start_seq = start_seq_id + i * records_per_file
                futures.append(
                    ex.submit(self._write_file_isolated, path, records_per_file, file_start_seq)
                )
            
            # המתנה לסיום כל המשימות
            completed = 0
            for future in as_completed(futures):
                try:
                    future.result()  # מעלה חריגות אם היו
                    completed += 1
                    self.logger.info(f"Completed {completed}/{num_files} files")
                except Exception as e:
                    self.logger.error(f"Error in parallel file generation: {e}")
                    raise
        
        self.logger.info(f"Successfully generated all {num_files} files")
        return paths

    def _write_file_isolated(self, path: str, num_records: int, file_start_seq: int):
        """
        כותב קובץ בקונטקסט 'מבודד' כדי שלא יתנגש ב-ID העוקב של אובייקטים אחרים.
        יוצרת גנרטור חדש עם אותו schema ופרמטרים (ללא שיתוף מצב).
        """
        try:
            # יצירת instance מבודד עם העברת הסכמה ישירות
            isolated = TelemetryGeneratorPro(
                schema_dict=self.schema,  # מעבירים את הסכמה ישירות
                add_sequential_id=self.add_sequential_id,
                id_field_name=self.id_field_name,
                id_bits=self.id_bits,
                output_dir=os.path.dirname(path) or ".",
                record_padding_to_byte=self.record_padding_to_byte,
                logger=self.logger
            )
            
            isolated.write_records_to_file(path, num_records, start_seq_id=file_start_seq)
            
        except Exception as e:
            self.logger.error(f"Error in isolated file writing for {path}: {e}")
            raise

    # ---------- עטיפות asyncio (אופציונלי) ----------
    async def generate_multiple_files_async(
        self,
        *,
        num_files: int,
        records_per_file: int,
        file_prefix: str = "telemetry",
        max_workers: int = 4,
        start_seq_id: int = 0
    ) -> List[str]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.generate_multiple_files_parallel(
                num_files=num_files,
                records_per_file=records_per_file,
                file_prefix=file_prefix,
                max_workers=max_workers,
                start_seq_id=start_seq_id,
            )
        )

    # ---------- שיטות עזר ----------
    def get_schema_info(self) -> Dict[str, Any]:
        """מחזיר מידע על הסכמה הנוכחית"""
        total_bits = sum(nbits for _, _, nbits in self._compiled_fields)
        total_bytes = math.ceil(total_bits / 8)
        
        return {
            "fields_count": len(self._compiled_fields),
            "total_bits_per_record": total_bits,
            "bytes_per_record": total_bytes,
            "fields": [
                {"name": name, "type": info.get("type"), "bits": nbits}
                for name, info, nbits in self._compiled_fields
            ]
        }

    def estimate_file_size(self, num_records: int) -> int:
        """מעריך את גודל הקובץ ב-bytes עבור מספר רשומות נתון"""
        info = self.get_schema_info()
        return num_records * info["bytes_per_record"]
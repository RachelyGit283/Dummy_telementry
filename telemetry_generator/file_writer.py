"""
file_writer.py
כתיבת קבצים ויצירה מקבילית של קבצי טלמטריה
"""

import json
import os
import random
from typing import Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import logging

from .types_and_enums import RecordType, OutputFormat, TelemetryRecord
from .formatters import OutputFormatter

class TelemetryFileWriter:
    """כותב קבצי טלמטריה בפורמטים שונים"""
    
    # def __init__(self, generator_class, schema, logger: Optional[logging.Logger] = None):
    #     self.generator_class = generator_class
    #     self.schema = schema
    #     self.logger = logger or logging.getLogger(__name__)
    def __init__(self, generator_class, schema, logger: Optional[logging.Logger] = None, types_file: str = None):
        self.generator_class = generator_class
        self.schema = schema
        self.logger = logger or logging.getLogger(__name__)
        self.types_file = types_file
    def write_records_enhanced(
        self,
        generator,
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
            generator._next_seq_id = start_seq_id
        
        # ברירת מחדל ליחס סוגי רשומות
        if record_type_ratio is None:
            record_type_ratio = {RecordType.UPDATE: 0.7, RecordType.EVENT: 0.3}
        
        # הוסף תמיכה ב-NDJSON
        use_ndjson = False
        if isinstance(output_format, str) and output_format == 'ndjson':
            output_format = OutputFormat.JSON
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
                    record = generator.generate_enhanced_record(record_type=selected_type)
                    
                    # כתיבה לפי הפורמט
                    if use_ndjson:
                        f.write(generator.format_ndjson(record))
                    elif output_format == OutputFormat.BINARY:
                        f.write(generator.pack_record_enhanced(record))
                    elif output_format == OutputFormat.INFLUX_LINE:
                        f.write(generator.format_influx_line(record, measurement_name))
                    elif output_format == OutputFormat.JSON:
                        # Collect record data for JSON array
                        record_dict = generator.formatter.prepare_json_array_data(record)
                        records_data.append(record_dict)
                    
                    # עדכון התקדמות
                    if progress_callback and i % 1000 == 0:
                        progress_callback(i, num_records)
                
                # Write JSON array at the end
                if output_format == OutputFormat.JSON and not use_ndjson:
                    json.dump(records_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Successfully wrote {num_records} records to {path}")
            
        except Exception as e:
            self.logger.error(f"Error writing enhanced records to {path}: {e}")
            raise

    def generate_multiple_files_enhanced(
        self,
        generator,
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
        ext = extensions.get(output_format, ".bin")
        
        total_records = num_files * records_per_file
        self.logger.info(f"Starting enhanced generation: {num_files} files × {records_per_file} records = {total_records} total")
        
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i in range(num_files):
                path = os.path.join(generator.output_dir, f"{file_prefix}_{i:04d}{ext}")
                paths.append(path)
                
                file_start_seq = start_seq_id + i * records_per_file
                futures.append(
                    ex.submit(
                        self._write_enhanced_file_isolated,
                        path, records_per_file, file_start_seq,
                        output_format, use_gpu_batches, batch_size, record_type_ratio,
                        generator.default_record_type
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
        record_type_ratio: Dict[RecordType, float],
        default_record_type: RecordType
    ):
        """כתיבת קובץ מתקדם במצב מבודד"""
        try:
            # יצירת instance מבודד שטוען מdict
            isolated = self.generator_class(
                schema_dict=self.schema,
                output_dir=os.path.dirname(path) or ".",
                default_record_type=default_record_type,
                enable_gpu=False,  # לא GPU בthreads מבודדים
                logger=self.logger
            )
            
            isolated._next_seq_id = file_start_seq
            
            # כתיבה רגילה
            self.write_records_enhanced(
                isolated, path, num_records,
                output_format=output_format,
                record_type_ratio=record_type_ratio
            )
                
        except Exception as e:
            self.logger.error(f"Error in isolated enhanced file writing for {path}: {e}")
            raise

    async def generate_multiple_files_enhanced_async(
        self,
        generator,
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
        """עטיפה async ליצירת קבצים מרובים"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.generate_multiple_files_enhanced(
                generator,
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
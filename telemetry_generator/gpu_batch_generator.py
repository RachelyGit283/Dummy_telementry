"""
gpu_batch_generator.py
יצירת batches של רשומות עם האצת GPU
"""

import time
from typing import List

from .types_and_enums import RecordType, TelemetryRecord
from .data_generators import FieldDataGenerator

class GPUBatchGenerator:
    """מחלקה ליצירת batches עם GPU"""
    
    def __init__(self, schema_processor, gpu_generator):
        self.schema_processor = schema_processor
        self.gpu_generator = gpu_generator
        self.field_generator = FieldDataGenerator()
    
    def generate_batch_gpu_accelerated(
        self, 
        batch_size: int,
        record_type: RecordType = RecordType.UPDATE,
        next_seq_id: int = 1
    ) -> List[TelemetryRecord]:
        """יצירת batch של רשומות עם האצת GPU"""
        if not self.gpu_generator:
            # Fallback ליצירה רגילה
            return self._generate_batch_fallback(batch_size, record_type, next_seq_id)
        
        records = []
        timestamp_base = time.time_ns()
        
        # חישוב מספר שדות מכל סוג
        int_fields = []
        float_fields = []
        
        for field in self.schema_processor.fields:
            field_type = field["type"]
            if "int" in field_type or "uint" in field_type:
                int_fields.append(field)
            elif "float" in field_type:
                float_fields.append(field)
        
        # יצירת batches עם GPU
        int_batch = []
        float_batch = []
        
        if int_fields:
            max_bits = max(field["bits"] for field in int_fields)
            int_batch = self.gpu_generator.generate_batch_int(
                batch_size, len(int_fields), max_bits
            )
        
        if float_fields:
            float_batch = self.gpu_generator.generate_batch_float(
                batch_size, len(float_fields)
            )
        
        # הרכבת רשומות
        for i in range(batch_size):
            data = {}
            
            # שדות int
            int_idx = 0
            float_idx = 0
            
            for field in self.schema_processor.fields:
                field_name = field["name"]
                field_type = field["type"]
                
                if "int" in field_type or "uint" in field_type:
                    if int_batch and int_idx < len(int_fields):
                        raw_value = int_batch[i][int_idx]
                        data[field_name] = raw_value & ((1 << field["bits"]) - 1)
                        int_idx += 1
                    else:
                        data[field_name] = self.field_generator.generate_generic_field_value(field)
                
                elif "float" in field_type:
                    if float_batch and float_idx < len(float_fields):
                        data[field_name] = float_batch[i][float_idx] * 1000 - 500  # scale
                        float_idx += 1
                    else:
                        data[field_name] = self.field_generator.generate_generic_field_value(field)
                
                else:
                    # שדות אחרים (string, enum, etc.)
                    data[field_name] = self.field_generator.generate_generic_field_value(field)
            
            # override עם ערכים ספציפיים
            seq_id = next_seq_id + i
            data["seq_no"] = seq_id
            data["timestamp_ns"] = timestamp_base + i
            
            record = TelemetryRecord(
                record_type=record_type,
                timestamp=timestamp_base + i,
                sequence_id=seq_id,
                data=data
            )
            records.append(record)
        
        return records
    
    def _generate_batch_fallback(
        self, 
        batch_size: int, 
        record_type: RecordType, 
        next_seq_id: int
    ) -> List[TelemetryRecord]:
        """Fallback ליצירה רגילה ללא GPU"""
        from data_generators import RecordDataPopulator
        
        populator = RecordDataPopulator(self.schema_processor)
        records = []
        
        for i in range(batch_size):
            seq_id = next_seq_id + i
            timestamp = time.time_ns() + i
            data = populator.populate_record_data(seq_id, timestamp)
            
            record = TelemetryRecord(
                record_type=record_type,
                timestamp=timestamp,
                sequence_id=seq_id,
                data=data
            )
            records.append(record)
        
        return records
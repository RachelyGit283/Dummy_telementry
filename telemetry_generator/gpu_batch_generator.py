# """
# gpu_batch_generator.py
# יצירת batches של רשומות עם האצת GPU
# """

# import time
# from typing import List

# from .types_and_enums import RecordType, TelemetryRecord
# from .data_generators import FieldDataGenerator

# class GPUBatchGenerator:
#     """מחלקה ליצירת batches עם GPU"""
    
#     def __init__(self, schema_processor, gpu_generator):
#         self.schema_processor = schema_processor
#         self.gpu_generator = gpu_generator
#         self.field_generator = FieldDataGenerator()
    
#     def generate_batch_gpu_accelerated(
#         self, 
#         batch_size: int,
#         record_type: RecordType = RecordType.UPDATE,
#         next_seq_id: int = 1
#     ) -> List[TelemetryRecord]:
#         """יצירת batch של רשומות עם האצת GPU"""
#         if not self.gpu_generator:
#             # Fallback ליצירה רגילה
#             return self._generate_batch_fallback(batch_size, record_type, next_seq_id)
        
#         records = []
#         timestamp_base = time.time_ns()
        
#         # חישוב מספר שדות מכל סוג
#         int_fields = []
#         float_fields = []
        
#         for field in self.schema_processor.fields:
#             field_type = field["type"]
#             if "int" in field_type or "uint" in field_type:
#                 int_fields.append(field)
#             elif "float" in field_type:
#                 float_fields.append(field)
        
#         # יצירת batches עם GPU
#         int_batch = []
#         float_batch = []
        
#         if int_fields:
#             max_bits = max(field["bits"] for field in int_fields)
#             int_batch = self.gpu_generator.generate_batch_int(
#                 batch_size, len(int_fields), max_bits
#             )
        
#         if float_fields:
#             float_batch = self.gpu_generator.generate_batch_float(
#                 batch_size, len(float_fields)
#             )
        
#         # הרכבת רשומות
#         for i in range(batch_size):
#             data = {}
            
#             # שדות int
#             int_idx = 0
#             float_idx = 0
            
#             for field in self.schema_processor.fields:
#                 field_name = field["name"]
#                 field_type = field["type"]
                
#                 if "int" in field_type or "uint" in field_type:
#                     if int_batch and int_idx < len(int_fields):
#                         raw_value = int_batch[i][int_idx]
#                         data[field_name] = raw_value & ((1 << field["bits"]) - 1)
#                         int_idx += 1
#                     else:
#                         data[field_name] = self.field_generator.generate_generic_field_value(field)
                
#                 elif "float" in field_type:
#                     if float_batch and float_idx < len(float_fields):
#                         data[field_name] = float_batch[i][float_idx] * 1000 - 500  # scale
#                         float_idx += 1
#                     else:
#                         data[field_name] = self.field_generator.generate_generic_field_value(field)
                
#                 else:
#                     # שדות אחרים (string, enum, etc.)
#                     data[field_name] = self.field_generator.generate_generic_field_value(field)
            
#             # override עם ערכים ספציפיים
#             seq_id = next_seq_id + i
#             data["seq_no"] = seq_id
#             data["timestamp_ns"] = timestamp_base + i
            
#             record = TelemetryRecord(
#                 record_type=record_type,
#                 timestamp=timestamp_base + i,
#                 sequence_id=seq_id,
#                 data=data
#             )
#             records.append(record)
        
#         return records
    
#     def _generate_batch_fallback(
#         self, 
#         batch_size: int, 
#         record_type: RecordType, 
#         next_seq_id: int
#     ) -> List[TelemetryRecord]:
#         """Fallback ליצירה רגילה ללא GPU"""
#         from data_generators import RecordDataPopulator
        
#         populator = RecordDataPopulator(self.schema_processor)
#         records = []
        
#         for i in range(batch_size):
#             seq_id = next_seq_id + i
#             timestamp = time.time_ns() + i
#             data = populator.populate_record_data(seq_id, timestamp)
            
#             record = TelemetryRecord(
#                 record_type=record_type,
#                 timestamp=timestamp,
#                 sequence_id=seq_id,
#                 data=data
#             )
#             records.append(record)
        
#         return records
"""
gpu_batch_generator.py
GPU-accelerated batch generation for telemetry records
"""

import time
from typing import List

from .types_and_enums import RecordType, TelemetryRecord
from .data_generators import FieldDataGenerator

class GPUBatchGenerator:
    """Class for generating batches with GPU acceleration"""
    
    def __init__(self, schema_processor, gpu_generator):
        self.schema_processor = schema_processor
        self.gpu_generator = gpu_generator
        try:
            self.field_generator = FieldDataGenerator()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize field generator: {e}")
    
    def generate_batch_gpu_accelerated(
        self, 
        batch_size: int,
        record_type: RecordType = RecordType.UPDATE,
        next_seq_id: int = 1
    ) -> List[TelemetryRecord]:
        """Generate batch of records with GPU acceleration"""
        try:
            if not self.gpu_generator:
                # Fallback to regular generation
                return self._generate_batch_fallback(batch_size, record_type, next_seq_id)
            
            records = []
            timestamp_base = time.time_ns()
            
            # Calculate number of fields of each type
            int_fields = []
            float_fields = []
            
            try:
                for field in self.schema_processor.fields:
                    field_type = field.get("type", "")
                    if "int" in field_type or "uint" in field_type:
                        int_fields.append(field)
                    elif "float" in field_type:
                        float_fields.append(field)
            except (AttributeError, KeyError) as e:
                raise ValueError(f"Invalid schema field structure: {e}")
            
            # Generate batches with GPU
            int_batch = []
            float_batch = []
            
            try:
                if int_fields:
                    max_bits = max(field.get("bits", 32) for field in int_fields)
                    int_batch = self.gpu_generator.generate_batch_int(
                        batch_size, len(int_fields), max_bits
                    )
                
                if float_fields:
                    float_batch = self.gpu_generator.generate_batch_float(
                        batch_size, len(float_fields)
                    )
            except Exception as e:
                raise RuntimeError(f"GPU batch generation failed: {e}")
            
            # Assemble records
            for i in range(batch_size):
                try:
                    data = {}
                    
                    # Integer and float field indices
                    int_idx = 0
                    float_idx = 0
                    
                    for field in self.schema_processor.fields:
                        try:
                            field_name = field.get("name", f"field_{int_idx + float_idx}")
                            field_type = field.get("type", "")
                            
                            if "int" in field_type or "uint" in field_type:
                                if int_batch and int_idx < len(int_fields):
                                    raw_value = int_batch[i][int_idx]
                                    field_bits = field.get("bits", 32)
                                    data[field_name] = raw_value & ((1 << field_bits) - 1)
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
                                # Other fields (string, enum, etc.)
                                data[field_name] = self.field_generator.generate_generic_field_value(field)
                                
                        except Exception as e:
                            # Continue with fallback value for this field
                            data[field_name] = 0
                            continue
                    
                    # Override with specific values
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
                    
                except Exception as e:
                    # Log error but continue with next record
                    continue
            
            if not records:
                raise RuntimeError("Failed to generate any records in batch")
                
            return records
            
        except Exception as e:
            if isinstance(e, (ValueError, RuntimeError)):
                raise
            raise RuntimeError(f"Unexpected error in GPU batch generation: {e}")
    
    def _generate_batch_fallback(
        self, 
        batch_size: int, 
        record_type: RecordType, 
        next_seq_id: int
    ) -> List[TelemetryRecord]:
        """Fallback to regular generation without GPU"""
        try:
            from .data_generators import RecordDataPopulator
            
            populator = RecordDataPopulator(self.schema_processor)
            records = []
            
            for i in range(batch_size):
                try:
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
                    
                except Exception as e:
                    # Continue with next record
                    continue
            
            if not records:
                raise RuntimeError("Failed to generate any records in fallback batch")
                
            return records
            
        except ImportError as e:
            raise ImportError(f"Failed to import RecordDataPopulator: {e}")
        except Exception as e:
            raise RuntimeError(f"Fallback batch generation failed: {e}")
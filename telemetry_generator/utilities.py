# """
# utilities.py
# כלי עזר, הערכות ביצועים ו-benchmarks
# """

# import math
# import time
# from typing import Dict, Union

# from .types_and_enums import OutputFormat

# class TelemetryUtilities:
#     """כלי עזר למערכת הטלמטריה"""
    
#     def __init__(self, schema_processor):
#         self.schema_processor = schema_processor
    
#     def estimate_storage_requirements(
#         self, 
#         num_records: int, 
#         output_format: OutputFormat = OutputFormat.BINARY,
#         compression_ratio: float = 1.0
#     ) -> Dict[str, Union[int, float, str]]:
#         """הערכת דרישות אחסון"""
#         info = self.get_schema_info()
        
#         if output_format == OutputFormat.BINARY:
#             base_size = num_records * info["total_bytes"]
#         elif output_format == OutputFormat.JSON:
#             # הערכה בסיס - JSON גדול פי 3-5 מבינארי
#             base_size = num_records * info["total_bytes"] * 4
#         else:  # InfluxDB Line Protocol
#             # הערכה בסיס - דומה לJSON אבל קצת יותר קומפקטי
#             base_size = num_records * info["total_bytes"] * 3
        
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
    
#     def get_schema_info(self) -> Dict[str, Union[str, int, list]]:
#         """מידע מפורט על הסכמה"""
#         return {
#             "schema_name": self.schema_processor.schema_name,
#             "endianness": self.schema_processor.endianness,
#             "total_bits": self.schema_processor.total_bits,
#             "total_bytes": math.ceil(self.schema_processor.total_bits / 8),
#             "fields_count": len(self.schema_processor.fields),
#             "validation": self.schema_processor.validation,
#             "fields": [
#                 {
#                     "name": field["name"],
#                     "type": field["type"], 
#                     "bits": field["bits"],
#                     "position": f"{field['start_bit']}-{field['end_bit']}",
#                     "description": field["desc"]
#                 }
#                 for field in self.schema_processor.fields
#             ]
#         }

# class BenchmarkRunner:
#     """מריץ בנצ'מרקים לביצועים"""
    
#     def __init__(self, generator, gpu_generator=None):
#         self.generator = generator
#         self.gpu_generator = gpu_generator
    
#     def benchmark_generation_speed(
#         self, 
#         test_records: int = 10000,
#         use_gpu: bool = False,
#         batch_size: int = 1000
#     ) -> Dict[str, float]:
#         """בנצ'מרק מהירות יצירה"""
#         results = {}
        
#         # בדיקה רגילה
#         start = time.time()
#         for _ in range(test_records):
#             self.generator.generate_enhanced_record()
#         regular_time = time.time() - start
#         results["regular_records_per_sec"] = test_records / regular_time
        
#         # בדיקה עם GPU (אם זמין)
#         if use_gpu and self.gpu_generator:
#             from gpu_batch_generator import GPUBatchGenerator
#             batch_gen = GPUBatchGenerator(
#                 self.generator.schema_processor, 
#                 self.generator.gpu_generator
#             )
            
#             start = time.time()
#             batches = test_records // batch_size
#             next_seq = 1
#             for _ in range(batches):
#                 batch_gen.generate_batch_gpu_accelerated(batch_size, next_seq_id=next_seq)
#                 next_seq += batch_size
#             gpu_time = time.time() - start
#             results["gpu_records_per_sec"] = (batches * batch_size) / gpu_time
#             results["gpu_speedup"] = results["gpu_records_per_sec"] / results["regular_records_per_sec"]
        
#         return results

# class FactoryMethods:
#     """Factory methods להתאמה עם CLI ישן"""
    
#     @staticmethod
#     def from_cli_params(generator_class, schema_path: str, **kwargs):
#         """Factory method for CLI initialization"""
#         cli_params = {
#             'schema_file': schema_path,
#             'enable_gpu': kwargs.get('gpu', False),
#             'output_dir': kwargs.get('output_dir', '.'),
#             'logger': kwargs.get('logger')
#         }
        
#         # פילטר רק פרמטרים רלוונטיים
#         init_params = {k: v for k, v in cli_params.items() if v is not None}
        
#         return generator_class(**init_params)
"""
utilities.py
Utility tools, performance estimations and benchmarks
"""

import math
import time
from typing import Dict, Union

from .types_and_enums import OutputFormat

class TelemetryUtilities:
    """Utility tools for the telemetry system"""
    
    def __init__(self, schema_processor):
        self.schema_processor = schema_processor
    
    def estimate_storage_requirements(
        self, 
        num_records: int, 
        output_format: OutputFormat = OutputFormat.BINARY,
        compression_ratio: float = 1.0
    ) -> Dict[str, Union[int, float, str]]:
        """Estimate storage requirements"""
        try:
            info = self.get_schema_info()
            
            if output_format == OutputFormat.BINARY:
                base_size = num_records * info["total_bytes"]
            elif output_format == OutputFormat.JSON:
                # Base estimation - JSON is 3-5x larger than binary
                base_size = num_records * info["total_bytes"] * 4
            else:  # InfluxDB Line Protocol
                # Base estimation - similar to JSON but slightly more compact
                base_size = num_records * info["total_bytes"] * 3
            
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
            
        except Exception as e:
            raise RuntimeError(f"Failed to estimate storage requirements: {e}")
    
    def get_schema_info(self) -> Dict[str, Union[str, int, list]]:
        """Detailed information about the schema"""
        try:
            fields_info = []
            for field in self.schema_processor.fields:
                try:
                    field_info = {
                        "name": field.get("name", "unknown"),
                        "type": field.get("type", "unknown"),
                        "bits": field.get("bits", 0),
                        "position": f"{field.get('start_bit', 0)}-{field.get('end_bit', 0)}",
                        "description": field.get("desc", "")
                    }
                    fields_info.append(field_info)
                except Exception as e:
                    # Continue with next field if one fails
                    continue
            
            return {
                "schema_name": getattr(self.schema_processor, 'schema_name', 'unknown'),
                "endianness": getattr(self.schema_processor, 'endianness', 'little'),
                "total_bits": getattr(self.schema_processor, 'total_bits', 0),
                "total_bytes": math.ceil(getattr(self.schema_processor, 'total_bits', 0) / 8),
                "fields_count": len(self.schema_processor.fields),
                "validation": getattr(self.schema_processor, 'validation', {}),
                "fields": fields_info
            }
            
        except AttributeError as e:
            raise ValueError(f"Invalid schema processor structure: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to get schema info: {e}")

class BenchmarkRunner:
    """Runs performance benchmarks"""
    
    def __init__(self, generator, gpu_generator=None):
        self.generator = generator
        self.gpu_generator = gpu_generator
    
    def benchmark_generation_speed(
        self, 
        test_records: int = 10000,
        use_gpu: bool = False,
        batch_size: int = 1000
    ) -> Dict[str, float]:
        """Benchmark generation speed"""
        try:
            results = {}
            
            # Regular test
            try:
                start = time.time()
                for _ in range(test_records):
                    self.generator.generate_enhanced_record()
                regular_time = time.time() - start
                
                if regular_time > 0:
                    results["regular_records_per_sec"] = test_records / regular_time
                else:
                    results["regular_records_per_sec"] = float('inf')
                    
            except Exception as e:
                raise RuntimeError(f"Regular benchmark failed: {e}")
            
            # GPU test (if available)
            if use_gpu and self.gpu_generator:
                try:
                    from .gpu_batch_generator import GPUBatchGenerator
                    batch_gen = GPUBatchGenerator(
                        self.generator.schema_processor, 
                        self.generator.gpu_generator
                    )
                    
                    start = time.time()
                    batches = test_records // batch_size
                    next_seq = 1
                    for _ in range(batches):
                        batch_gen.generate_batch_gpu_accelerated(batch_size, next_seq_id=next_seq)
                        next_seq += batch_size
                    gpu_time = time.time() - start
                    
                    if gpu_time > 0:
                        results["gpu_records_per_sec"] = (batches * batch_size) / gpu_time
                        if results.get("regular_records_per_sec", 0) > 0:
                            results["gpu_speedup"] = results["gpu_records_per_sec"] / results["regular_records_per_sec"]
                    
                except ImportError as e:
                    # GPU benchmark not available
                    pass
                except Exception as e:
                    # Continue without GPU results
                    pass
            
            return results
            
        except Exception as e:
            raise RuntimeError(f"Benchmark failed: {e}")

class FactoryMethods:
    """Factory methods for compatibility with old CLI"""
    
    @staticmethod
    def from_cli_params(generator_class, schema_path: str, **kwargs):
        """Factory method for CLI initialization"""
        try:
            cli_params = {
                'schema_file': schema_path,
                'enable_gpu': kwargs.get('gpu', False),
                'output_dir': kwargs.get('output_dir', '.'),
                'logger': kwargs.get('logger')
            }
            
            # Filter only relevant parameters
            init_params = {k: v for k, v in cli_params.items() if v is not None}
            
            return generator_class(**init_params)
            
        except TypeError as e:
            raise ValueError(f"Invalid parameters for generator class: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to create generator from CLI params: {e}")
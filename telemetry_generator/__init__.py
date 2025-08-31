# """
# __init__.py
# קובץ החיבור הראשי למערכת טלמטריה מתקדמת
# מאחד בין המבנה הישן לחדש
# """

# # -------------------------
# # Core types and enums
# # -------------------------
# from .types_and_enums import (
#     Number,
#     RecordType, 
#     OutputFormat,
#     TelemetryRecord
# )

# # -------------------------
# # Schema & Binary Processing
# # -------------------------
# from .binary_schema import BinarySchemaProcessor
# from .binary_packer import BinaryRecordPacker

# # -------------------------
# # GPU Acceleration
# # -------------------------
# from .gpu_accelerator import GPUAcceleratedGenerator
# from .gpu_batch_generator import GPUBatchGenerator

# # -------------------------
# # Data Generators
# # -------------------------
# from .data_generators import FieldDataGenerator, RecordDataPopulator

# # -------------------------
# # Formatters
# # -------------------------
# from .formatters import OutputFormatter

# # -------------------------
# # File Writers
# # -------------------------
# from .file_writer import TelemetryFileWriter
# from .rolling_writer import RollingFileWriter  # מהגרסה הישנה

# # -------------------------
# # Rate Control (מהגרסה הישנה)
# # -------------------------
# from .rate_control import RateLimiter, BurstController, VariableRateController

# # -------------------------
# # Load Profiles (מהגרסה הישנה)
# # -------------------------
# from .load_profiles import LOAD_PROFILES, LoadProfile, ProfileManager

# # -------------------------
# # LEB128 Encoding/Decoding (מהגרסה הישנה)
# # -------------------------
# from .formats.leb128 import (
#     encode_leb128,
#     decode_leb128,
#     encode_signed_leb128,
#     decode_signed_leb128,
#     LEB128Encoder,
#     LEB128Decoder
# )

# # -------------------------
# # Utilities
# # -------------------------
# from .utilities import TelemetryUtilities, BenchmarkRunner, FactoryMethods

# # -------------------------
# # CLI
# # -------------------------
# from .cli import cli

# # -------------------------
# # Main Generator
# # -------------------------
# from .telemetry_generator import EnhancedTelemetryGeneratorPro

# # -------------------------
# # Package Metadata
# # -------------------------
# __version__ = "1.0.0"
# __author__ = "Telemetry System Team"

# __all__ = [
#     # Main class
#     'EnhancedTelemetryGeneratorPro',

#     # Core types
#     'RecordType',
#     'OutputFormat', 
#     'TelemetryRecord',
#     'Number',

#     # Core components
#     'BinarySchemaProcessor',
#     'BinaryRecordPacker',
#     'GPUAcceleratedGenerator',
#     'GPUBatchGenerator',
#     'FieldDataGenerator',
#     'RecordDataPopulator',
#     'OutputFormatter',
#     'TelemetryFileWriter',
#     'RollingFileWriter',

#     # Rate control
#     'RateLimiter',
#     'BurstController',
#     'VariableRateController',

#     # Load profiles
#     'LOAD_PROFILES',
#     'LoadProfile',
#     'ProfileManager',

#     # LEB128
#     'encode_leb128',
#     'decode_leb128',
#     'encode_signed_leb128',
#     'decode_signed_leb128',
#     'LEB128Encoder',
#     'LEB128Decoder',

#     # Utilities
#     'TelemetryUtilities',
#     'BenchmarkRunner',
#     'FactoryMethods',

#     # CLI
#     'cli',
# ]

# # -------------------------
# # Convenience Functions
# # -------------------------
# def create_generator(schema_file=None, schema_dict=None, **kwargs):
#     """
#     יצירת generator בצורה קלה
#     """
#     return EnhancedTelemetryGeneratorPro(
#         schema_file=schema_file,
#         schema_dict=schema_dict,
#         **kwargs
#     )

# def create_from_cli(schema_path, **cli_args):
#     """
#     יצירת generator מפרמטרי CLI
#     """
#     return EnhancedTelemetryGeneratorPro.from_cli_params(schema_path, **cli_args)

# def get_version():
#     """Return package version"""
#     return __version__

# def get_supported_formats():
#     """Return list of supported output formats"""
#     return [fmt.value for fmt in OutputFormat]

# def get_record_types():
#     """Return list of supported record types"""
#     return [rtype.value for rtype in RecordType]
"""
__init__.py
קובץ החיבור הראשי למערכת טלמטריה מתקדמת
מאחד בין המבנה הישן לחדש + Fault Injection
"""

# -------------------------
# Core types and enums
# -------------------------
from .types_and_enums import (
    Number,
    RecordType, 
    OutputFormat,
    TelemetryRecord
)

# -------------------------
# Schema & Binary Processing
# -------------------------
from .binary_schema import BinarySchemaProcessor
from .binary_packer import BinaryRecordPacker

# -------------------------
# GPU Acceleration
# -------------------------
from .gpu_accelerator import GPUAcceleratedGenerator
from .gpu_batch_generator import GPUBatchGenerator

# -------------------------
# Data Generators (Updated)
# -------------------------
from .data_generators import (
    FieldDataGenerator, 
    RecordDataPopulator,          # Original - backwards compatible
    FaultAwareRecordDataPopulator, # New - with fault injection
    create_clean_populator,
    create_development_populator,
    create_testing_populator,
    create_stress_populator
)

# -------------------------
# NEW: Fault Injection
# -------------------------
from .fault_injector import (
    FaultInjector,
    FaultType,
    FaultConfig,
    FaultStatistics,
    create_development_fault_injector,
    create_testing_fault_injector,
    create_stress_fault_injector
)

# -------------------------
# Formatters
# -------------------------
from .formatters import OutputFormatter

# -------------------------
# File Writers
# -------------------------
from .file_writer import TelemetryFileWriter
from .rolling_writer import RollingFileWriter

# -------------------------
# Rate Control
# -------------------------
from .rate_control import RateLimiter, BurstController, VariableRateController

# -------------------------
# Load Profiles
# -------------------------
from .load_profiles import LOAD_PROFILES, LoadProfile, ProfileManager

# -------------------------
# LEB128 Encoding/Decoding
# -------------------------
from .formats.leb128 import (
    encode_leb128,
    decode_leb128,
    encode_signed_leb128,
    decode_signed_leb128,
    LEB128Encoder,
    LEB128Decoder
)

# -------------------------
# Utilities
# -------------------------
from .utilities import TelemetryUtilities, BenchmarkRunner, FactoryMethods

# -------------------------
# CLI
# -------------------------
from .cli import cli

# -------------------------
# Main Generator (Updated)
# -------------------------
from .telemetry_generator import EnhancedTelemetryGeneratorPro

# -------------------------
# Package Metadata
# -------------------------
__version__ = "1.1.0"  # Updated for Fault Injection support
__author__ = "Telemetry System Team"

__all__ = [
    # Main class
    'EnhancedTelemetryGeneratorPro',

    # Core types
    'RecordType',
    'OutputFormat', 
    'TelemetryRecord',
    'Number',

    # Core components
    'BinarySchemaProcessor',
    'BinaryRecordPacker',
    'GPUAcceleratedGenerator',
    'GPUBatchGenerator',
    
    # Data generators (both old and new)
    'FieldDataGenerator',
    'RecordDataPopulator',           # Original
    'FaultAwareRecordDataPopulator', # New
    'create_clean_populator',
    'create_development_populator',
    'create_testing_populator', 
    'create_stress_populator',
    
    # NEW: Fault injection
    'FaultInjector',
    'FaultType',
    'FaultConfig',
    'FaultStatistics',
    'create_development_fault_injector',
    'create_testing_fault_injector',
    'create_stress_fault_injector',
    
    # Formatters and writers
    'OutputFormatter',
    'TelemetryFileWriter',
    'RollingFileWriter',

    # Rate control
    'RateLimiter',
    'BurstController',
    'VariableRateController',

    # Load profiles
    'LOAD_PROFILES',
    'LoadProfile',
    'ProfileManager',

    # LEB128
    'encode_leb128',
    'decode_leb128',
    'encode_signed_leb128',
    'decode_signed_leb128',
    'LEB128Encoder',
    'LEB128Decoder',

    # Utilities
    'TelemetryUtilities',
    'BenchmarkRunner',
    'FactoryMethods',

    # CLI
    'cli',
]

# -------------------------
# Convenience Functions (Updated)
# -------------------------
def create_generator(schema_file=None, schema_dict=None, enable_faults=False, fault_rate=0.05, **kwargs):
    """
    יצירת generator בצורה קלה עם תמיכה ב-fault injection
    
    Args:
        schema_file: נתיב לקובץ סכמה
        schema_dict: סכמה כ-dict
        enable_faults: האם להפעיל fault injection
        fault_rate: שיעור שגיאות (0.0-1.0)
        **kwargs: פרמטרים נוספים
    """
    return EnhancedTelemetryGeneratorPro(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_fault_injection=enable_faults,
        fault_rate=fault_rate,
        **kwargs
    )

def create_from_cli(schema_path, **cli_args):
    """
    יצירת generator מפרמטרי CLI
    """
    return EnhancedTelemetryGeneratorPro.from_cli_params(schema_path, **cli_args)

def create_clean_generator(schema_file=None, schema_dict=None, **kwargs):
    """יצירת generator ללא שגיאות (נתונים נקיים בלבד)"""
    return create_generator(
        schema_file=schema_file, 
        schema_dict=schema_dict, 
        enable_faults=False, 
        **kwargs
    )

def create_development_generator(schema_file=None, schema_dict=None, **kwargs):
    """יצירת generator לפיתוח (שגיאות קלות)"""
    return create_generator(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_faults=True,
        fault_rate=0.02,  # 2%
        fault_types=['out_of_range', 'null_values'],
        **kwargs
    )

def create_testing_generator(schema_file=None, schema_dict=None, **kwargs):
    """יצירת generator לבדיקות (שגיאות מגוונות)"""
    return create_generator(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_faults=True,
        fault_rate=0.05,  # 5%
        **kwargs
    )

def create_stress_generator(schema_file=None, schema_dict=None, **kwargs):
    """יצירת generator לבדיקות עומס (שגיאות רבות)"""
    return create_generator(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_faults=True,
        fault_rate=0.15,  # 15%
        **kwargs
    )

def get_version():
    """Return package version"""
    return __version__

def get_supported_formats():
    """Return list of supported output formats"""
    return [fmt.value for fmt in OutputFormat]

def get_record_types():
    """Return list of supported record types"""
    return [rtype.value for rtype in RecordType]

def get_fault_types():
    """Return list of supported fault injection types"""
    return [ftype.value for ftype in FaultType]

# -------------------------
# Migration helpers
# -------------------------
def migrate_from_v1(old_generator_instance):
    """
    עזרה למעבר מגרסה 1.0 לגרסה 1.1 עם Fault Injection
    """
    # This is a placeholder for future migration needs
    pass

# -------------------------
# Quick start examples
# -------------------------
def quick_example():
    """דוגמה מהירה לשימוש במערכת"""
    # יצירת generator בסיסי
    generator = create_generator(schema_file="schema.json")
    
    # יצירת רשומה אחת
    record, faults = generator.generate_enhanced_record()
    
    # ארוז לבינארי
    binary_data = generator.pack_record_enhanced(record)
    
    # המרה ל-JSON
    json_data = generator.format_json(record)
    
    return record, binary_data, json_data

def quick_fault_example():
    """דוגמה מהירה ל-Fault Injection"""
    # יצירת generator עם שגיאות
    generator = create_testing_generator(schema_file="schema.json")
    
    # יצירת batch עם שגיאות
    records, fault_details = generator.generate_batch_with_faults(100)
    
    # בדיקת סטטיסטיקות
    stats = generator.get_fault_statistics()
    
    return records, fault_details, stats
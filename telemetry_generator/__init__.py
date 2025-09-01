
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
# Data Generators
# -------------------------
from .data_generators import (
    FieldDataGenerator, 
    RecordDataPopulator,          
    FaultAwareRecordDataPopulator, 
    create_clean_populator,
    create_development_populator,
    create_testing_populator,
    create_stress_populator
)

# -------------------------
# Fault Injection
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
# Main Generator
# -------------------------
from .telemetry_generator import EnhancedTelemetryGeneratorPro

# -------------------------
# Package Metadata
# -------------------------
__version__ = "1.1.0"  
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
    
    # Data generators 
    'FieldDataGenerator',
    'RecordDataPopulator',           
    'FaultAwareRecordDataPopulator', 
    'create_clean_populator',
    'create_development_populator',
    'create_testing_populator', 
    'create_stress_populator',
    
    # Fault injection
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
# Convenience Functions 
# -------------------------
def create_generator(schema_file=None, schema_dict=None, enable_faults=False, fault_rate=0.05, **kwargs):
    """
    Create a generator easily with support for fault injection
    
    Args:
        schema_file: Path to the schema file
        schema_dict: Schema as a dict
        enable_faults: Whether to enable fault injection
        fault_rate: Fault rate (0.0-1.0)
        **kwargs: Additional parameters
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
    Create a generator from CLI parameters
    """
    return EnhancedTelemetryGeneratorPro.from_cli_params(schema_path, **cli_args)


def create_clean_generator(schema_file=None, schema_dict=None, **kwargs):
    """Create a generator without faults (clean data only)"""
    return create_generator(
        schema_file=schema_file, 
        schema_dict=schema_dict, 
        enable_faults=False, 
        **kwargs
    )


def create_development_generator(schema_file=None, schema_dict=None, **kwargs):
    """Create a generator for development (light faults)"""
    return create_generator(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_faults=True,
        fault_rate=0.02,  # 2%
        fault_types=['out_of_range', 'null_values'],
        **kwargs
    )


def create_testing_generator(schema_file=None, schema_dict=None, **kwargs):
    """Create a generator for testing (diverse faults)"""
    return create_generator(
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_faults=True,
        fault_rate=0.05,  # 5%
        **kwargs
    )


def create_stress_generator(schema_file=None, schema_dict=None, **kwargs):
    """Create a generator for stress testing (high fault rate)"""
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
    Helper for migrating from version 1.0 to 1.1 with Fault Injection
    """
    # This is a placeholder for future migration needs
    pass


# -------------------------
# Quick start examples
# -------------------------
def quick_example():
    """Quick example of using the system"""
    # Create a basic generator
    generator = create_generator(schema_file="schema.json")
    
    # Generate a single record
    record, faults = generator.generate_enhanced_record()
    
    # Pack into binary
    binary_data = generator.pack_record_enhanced(record)
    
    # Convert to JSON
    json_data = generator.format_json(record)
    
    return record, binary_data, json_data


def quick_fault_example():
    """Quick example with Fault Injection"""
    # Create a generator with faults
    generator = create_testing_generator(schema_file="schema.json")
    
    # Generate a batch with faults
    records, fault_details = generator.generate_batch_with_faults(100)
    
    # Check statistics
    stats = generator.get_fault_statistics()
    
    return records, fault_details, stats

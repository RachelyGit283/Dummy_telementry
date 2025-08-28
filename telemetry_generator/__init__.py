"""
Telemetry Generator Package
High-performance telemetry data generator
"""

__version__ = "1.0.0"

from .generator import (
    EnhancedTelemetryGeneratorPro,
    TelemetryRecord,
    RecordType,
    OutputFormat
)
from .cli import cli
from .rolling_writer import RollingFileWriter
from .rate_control import RateLimiter, BurstController, VariableRateController
from .load_profiles import LOAD_PROFILES, LoadProfile, ProfileManager
from .formats.leb128 import (
    encode_leb128,
    decode_leb128,
    encode_signed_leb128,
    decode_signed_leb128,
    LEB128Encoder,
    LEB128Decoder
)

__all__ = [
    # Main classes
    'EnhancedTelemetryGeneratorPro',
    'TelemetryRecord',
    'RecordType',
    'OutputFormat',
    
    # CLI
    'cli',
    
    # Writers
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
]

"""
types_and_enums.py
Basic definitions, enums and data types for the telemetry system
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Union

# Type aliases
Number = Union[int, float]

class RecordType(Enum):
    """Telemetry record types"""
    UPDATE = "update"
    EVENT = "event"

class OutputFormat(Enum):
    """Supported output formats"""
    BINARY = "binary"
    INFLUX_LINE = "influx_line"
    JSON = "json"

@dataclass
class TelemetryRecord:
    """Data structure for telemetry record"""
    record_type: RecordType
    timestamp: int
    sequence_id: int
    data: Dict[str, Any]
    
    def __post_init__(self):
        """Validate record data after initialization"""
        try:
            # Validate record_type
            if not isinstance(self.record_type, RecordType):
                raise TypeError(f"record_type must be RecordType, got {type(self.record_type)}")
            
            # Validate timestamp
            if not isinstance(self.timestamp, int):
                raise TypeError(f"timestamp must be int, got {type(self.timestamp)}")
            if self.timestamp < 0:
                raise ValueError(f"timestamp must be non-negative, got {self.timestamp}")
            
            # Validate sequence_id
            if not isinstance(self.sequence_id, int):
                raise TypeError(f"sequence_id must be int, got {type(self.sequence_id)}")
            if self.sequence_id < 0:
                raise ValueError(f"sequence_id must be non-negative, got {self.sequence_id}")
            
            # Validate data
            if not isinstance(self.data, dict):
                raise TypeError(f"data must be dict, got {type(self.data)}")
            if not self.data:
                raise ValueError("data dictionary cannot be empty")
                
        except Exception as e:
            raise ValueError(f"Invalid TelemetryRecord initialization: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary representation"""
        try:
            return {
                "record_type": self.record_type.value,
                "timestamp": self.timestamp,
                "sequence_id": self.sequence_id,
                "data": self.data.copy()
            }
        except Exception as e:
            raise RuntimeError(f"Failed to convert record to dict: {e}")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TelemetryRecord':
        """Create record from dictionary representation"""
        try:
            record_type_str = data.get("record_type")
            if record_type_str not in [rt.value for rt in RecordType]:
                raise ValueError(f"Invalid record_type: {record_type_str}")
            
            return cls(
                record_type=RecordType(record_type_str),
                timestamp=data.get("timestamp", 0),
                sequence_id=data.get("sequence_id", 0),
                data=data.get("data", {})
            )
        except KeyError as e:
            raise ValueError(f"Missing required field in data: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to create record from dict: {e}")
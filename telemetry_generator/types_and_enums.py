"""
types_and_enums.py
הגדרות בסיסיות, enums וטיפוסי נתונים למערכת הטלמטריה
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Union

# Type aliases
Number = Union[int, float]

class RecordType(Enum):
    """סוגי רשומות טלמטריה"""
    UPDATE = "update"
    EVENT = "event"

class OutputFormat(Enum):
    """פורמטי פלט נתמכים"""
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
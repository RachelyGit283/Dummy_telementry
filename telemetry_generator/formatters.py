"""
formatters.py
מעצבי פלט לפורמטים שונים (JSON, InfluxDB, NDJSON)
"""

import json
from typing import Dict, Any

from .types_and_enums import TelemetryRecord

class OutputFormatter:
    """מעצב פלט לפורמטים שונים"""
    
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
    
    def format_json(self, record: TelemetryRecord) -> str:
        """המרת רשומה לפורמט JSON"""
        return json.dumps({
            "schema": self.schema_name,
            "type": record.record_type.value,
            "timestamp": record.timestamp,
            "sequence_id": record.sequence_id,
            "data": record.data
        })
    
    def format_ndjson(self, record: TelemetryRecord) -> str:
        """Format record as NDJSON (single line JSON)"""
        data = {
            'schema': self.schema_name,
            'type': record.record_type.value,
            'timestamp': record.timestamp,
            'seq_id': record.sequence_id,
            'data': record.data
        }
        return json.dumps(data, separators=(',', ':')) + '\n'
    
    def format_influx_line(self, record: TelemetryRecord, measurement: str = "telemetry") -> str:
        """המרת רשומה לפורמט InfluxDB Line Protocol"""
        # Tags - מידע מטא
        tags = {
            "schema": self.schema_name,
            "type": record.record_type.value,
            "seq_id": str(record.sequence_id)
        }
        
        # Fields - הנתונים בפועל
        fields = record.data.copy()
        
        # המרת timestamp לננו-שניות
        timestamp = record.timestamp
        
        # Tags string
        tags_str = ',' + ','.join(f"{k}={v}" for k, v in tags.items()) if tags else ''
        
        # Fields string
        field_pairs = []
        for key, value in fields.items():
            if isinstance(value, str):
                field_pairs.append(f'{key}="{value}"')
            elif isinstance(value, bool):
                field_pairs.append(f'{key}={str(value).lower()}')
            elif isinstance(value, float):
                field_pairs.append(f'{key}={value}')
            else:  # int
                field_pairs.append(f'{key}={value}i')
        
        fields_str = ','.join(field_pairs)
        
        return f"{measurement}{tags_str} {fields_str} {timestamp}\n"
    
    def prepare_json_array_data(self, record: TelemetryRecord) -> Dict[str, Any]:
        """הכנת נתונים למערך JSON"""
        return {
            "schema": self.schema_name,
            "type": record.record_type.value,
            "timestamp": record.timestamp,
            "sequence_id": record.sequence_id,
            "data": record.data
        }
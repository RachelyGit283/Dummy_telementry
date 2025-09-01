# """
# formatters.py
# מעצבי פלט לפורמטים שונים (JSON, InfluxDB, NDJSON)
# """

# import json
# from typing import Dict, Any

# from .types_and_enums import TelemetryRecord

# class OutputFormatter:
#     """מעצב פלט לפורמטים שונים"""
    
#     def __init__(self, schema_name: str):
#         self.schema_name = schema_name
    
#     def format_json(self, record: TelemetryRecord) -> str:
#         """המרת רשומה לפורמט JSON"""
#         return json.dumps({
#             "schema": self.schema_name,
#             "type": record.record_type.value,
#             "timestamp": record.timestamp,
#             "sequence_id": record.sequence_id,
#             "data": record.data
#         })
    
#     def format_ndjson(self, record: TelemetryRecord) -> str:
#         """Format record as NDJSON (single line JSON)"""
#         data = {
#             'schema': self.schema_name,
#             'type': record.record_type.value,
#             'timestamp': record.timestamp,
#             'seq_id': record.sequence_id,
#             'data': record.data
#         }
#         return json.dumps(data, separators=(',', ':')) + '\n'
    
#     def format_influx_line(self, record: TelemetryRecord, measurement: str = "telemetry") -> str:
#         """המרת רשומה לפורמט InfluxDB Line Protocol"""
#         # Tags - מידע מטא
#         tags = {
#             "schema": self.schema_name,
#             "type": record.record_type.value,
#             "seq_id": str(record.sequence_id)
#         }
        
#         # Fields - הנתונים בפועל
#         fields = record.data.copy()
        
#         # המרת timestamp לננו-שניות
#         timestamp = record.timestamp
        
#         # Tags string
#         tags_str = ',' + ','.join(f"{k}={v}" for k, v in tags.items()) if tags else ''
        
#         # Fields string
#         field_pairs = []
#         for key, value in fields.items():
#             if isinstance(value, str):
#                 field_pairs.append(f'{key}="{value}"')
#             elif isinstance(value, bool):
#                 field_pairs.append(f'{key}={str(value).lower()}')
#             elif isinstance(value, float):
#                 field_pairs.append(f'{key}={value}')
#             else:  # int
#                 field_pairs.append(f'{key}={value}i')
        
#         fields_str = ','.join(field_pairs)
        
#         return f"{measurement}{tags_str} {fields_str} {timestamp}\n"
    
#     def prepare_json_array_data(self, record: TelemetryRecord) -> Dict[str, Any]:
#         """הכנת נתונים למערך JSON"""
#         return {
#             "schema": self.schema_name,
#             "type": record.record_type.value,
#             "timestamp": record.timestamp,
#             "sequence_id": record.sequence_id,
#             "data": record.data
#         }
"""
formatters.py
Output formatters for different formats (JSON, InfluxDB, NDJSON)
"""
import json
from typing import Dict, Any
from .types_and_enums import TelemetryRecord

class OutputFormatter:
    """Output formatter for various formats"""
   
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
   
    def format_json(self, record: TelemetryRecord) -> str:
        """Convert record to JSON format"""
        try:
            data = {
                "schema": self.schema_name,
                "type": record.record_type.value,
                "timestamp": record.timestamp,
                "sequence_id": record.sequence_id,
                "data": record.data
            }
            return json.dumps(data)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize record to JSON: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error formatting JSON: {e}")
   
    def format_ndjson(self, record: TelemetryRecord) -> str:
        """Format record as NDJSON (single line JSON)"""
        try:
            data = {
                'schema': self.schema_name,
                'type': record.record_type.value,
                'timestamp': record.timestamp,
                'seq_id': record.sequence_id,
                'data': record.data
            }
            return json.dumps(data, separators=(',', ':')) + '\n'
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize record to NDJSON: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error formatting NDJSON: {e}")
   
    def format_influx_line(self, record: TelemetryRecord, measurement: str = "telemetry") -> str:
        """Convert record to InfluxDB Line Protocol format"""
        try:
            # Tags - metadata information
            tags = {
                "schema": self.schema_name,
                "type": record.record_type.value,
                "seq_id": str(record.sequence_id)
            }
           
            # Fields - actual data
            try:
                fields = record.data.copy()
            except AttributeError:
                fields = dict(record.data) if record.data else {}
           
            # Convert timestamp to nanoseconds
            timestamp = record.timestamp
           
            # Tags string
            try:
                tags_str = ',' + ','.join(f"{k}={v}" for k, v in tags.items()) if tags else ''
            except Exception as e:
                raise ValueError(f"Error formatting tags: {e}")
           
            # Fields string
            field_pairs = []
            for key, value in fields.items():
                try:
                    if isinstance(value, str):
                        # Escape quotes and special characters in string values
                        escaped_value = value.replace('"', '\\"')
                        field_pairs.append(f'{key}="{escaped_value}"')
                    elif isinstance(value, bool):
                        field_pairs.append(f'{key}={str(value).lower()}')
                    elif isinstance(value, float):
                        if not (float('-inf') < value < float('inf')):
                            raise ValueError(f"Invalid float value for field {key}: {value}")
                        field_pairs.append(f'{key}={value}')
                    else:  # int or other numeric types
                        field_pairs.append(f'{key}={value}i')
                except Exception as e:
                    raise ValueError(f"Error formatting field {key} with value {value}: {e}")
           
            if not field_pairs:
                raise ValueError("No valid fields found for InfluxDB line protocol")
           
            try:
                fields_str = ','.join(field_pairs)
            except Exception as e:
                raise ValueError(f"Error joining field pairs: {e}")
           
            return f"{measurement}{tags_str} {fields_str} {timestamp}\n"
            
        except Exception as e:
            if isinstance(e, (ValueError, RuntimeError)):
                raise
            raise RuntimeError(f"Unexpected error formatting InfluxDB line: {e}")
   
    def prepare_json_array_data(self, record: TelemetryRecord) -> Dict[str, Any]:
        """Prepare data for JSON array"""
        try:
            return {
                "schema": self.schema_name,
                "type": record.record_type.value,
                "timestamp": record.timestamp,
                "sequence_id": record.sequence_id,
                "data": record.data
            }
        except AttributeError as e:
            raise ValueError(f"Invalid record structure: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error preparing JSON array data: {e}")
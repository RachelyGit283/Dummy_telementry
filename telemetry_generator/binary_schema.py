# """
# מעבד סכמה בינארית עם מיקומי ביטים קבועים - מעודכן לפורמט החדש
# """

# from typing import Dict, Any, Optional
# import json
# import os

# # בדיקת זמינות numpy
# try:
#     import numpy as np
#     HAS_NUMPY = True
# except ImportError:
#     HAS_NUMPY = False

# class BinarySchemaProcessor:
#     """מעבד סכמה בינארית חדשה עם מיקומי ביטים קבועים"""
    
#     def __init__(self, schema: Dict[str, Any], types_file: Optional[str] = None):
#         self.schema = schema
#         self.schema_name = schema.get("schema_name", "unknown")
#         self.endianness = schema.get("endianness", "little")
#         self.total_bits = schema.get("total_bits", 0)
#         self.validation = schema.get("validation", {})
        
#         # טעינת מיפוי טיפוסים
#         self.type_mapping = self._load_type_mapping(types_file)
        
#         # פרסור שדות הסכמה
#         self.fields = []
#         self.fields_by_name = {}
        
#         for field_name, field_info in schema.items():
#             if field_name in ["schema_name", "endianness", "total_bits", "validation"]:
#                 continue
                
#             # פרסור מיקום ביטים
#             pos_str = field_info.get("pos", "0-7")
#             start_bit, end_bit = map(int, pos_str.split("-"))
            
#             # המרת טיפוס לפי מיפוי
#             original_type = field_info.get("type", "uint8")
#             mapped_type = self._map_type(original_type)
            
#             field_data = {
#                 "name": field_name,
#                 "type": mapped_type,
#                 "original_type": original_type,
#                 "bits": field_info.get("bits", 8),
#                 "start_bit": start_bit,
#                 "end_bit": end_bit,
#                 "desc": field_info.get("desc", ""),
#                 "enum": self._process_enum(field_info),
#                 "original_info": field_info
#             }
            
#             self.fields.append(field_data)
#             self.fields_by_name[field_name] = field_data
        
#         # מיון לפי מיקום ביטים
#         self.fields.sort(key=lambda x: x["start_bit"])
        
#         # וולידציה
#         self._validate_schema()
    
#     def _load_type_mapping(self, types_file: Optional[str]) -> Dict[str, str]:
#         """טעינת מיפוי טיפוסים מקובץ חיצוני"""
#         if types_file and os.path.exists(types_file):
#             try:
#                 with open(types_file, 'r', encoding='utf-8') as f:
#                     return json.load(f)
#             except Exception as e:
#                 print(f"Warning: Could not load types file: {e}")
        
#         # Default mapping
#         return {
#             "uint8": "np.uint8",
#             "int8": "np.int8",
#             "uint16": "np.uint16",
#             "uint32": "np.uint32",
#             "uint64": "np.uint64",
#             "int64": "np.int64",
#             "float32": "np.float32",
#             "bytes": "np.bytes_",
#             "enum": "np.uint8"  # Default for enums
#         }
    
#     def _map_type(self, original_type: str) -> str:
#         """ממפה טיפוס מהסכמה לטיפוס Python/NumPy"""
#         if original_type == "enum":
#             return "np.uint8"  # enums are stored as uint8
#         return self.type_mapping.get(original_type, "np.uint8")
    
#     def _process_enum(self, field_info: Dict[str, Any]) -> Dict[str, Any]:
#         """מעבד enum מהפורמט החדש"""
#         if field_info.get("type") != "enum":
#             return {}
        
#         values = field_info.get("values", [])
#         if not values:
#             return {}
        
#         # יצירת מיפוי מספרי לערכים
#         enum_map = {}
#         for i, value in enumerate(values):
#             enum_map[str(i)] = value
        
#         return enum_map
    
#     def _validate_schema(self):
#         """וולידציה של הסכמה החדשה"""
#         if not self.fields:
#             raise ValueError("Schema contains no valid fields")
        
#         # בדיקת חפיפות ביטים
#         for i in range(len(self.fields) - 1):
#             current = self.fields[i]
#             next_field = self.fields[i + 1]
            
#             if current["end_bit"] >= next_field["start_bit"]:
#                 raise ValueError(
#                     f"Bit overlap between {current['name']} ({current['start_bit']}-{current['end_bit']}) "
#                     f"and {next_field['name']} ({next_field['start_bit']}-{next_field['end_bit']})"
#                 )
        
#         # בדיקת סך הביטים
#         if self.fields:
#             last_field = self.fields[-1]
#             actual_bits = last_field["end_bit"] + 1
            
#             if self.total_bits > 0 and actual_bits > self.total_bits:
#                 raise ValueError(
#                     f"Schema fields extend beyond total_bits ({self.total_bits}): actual bits used = {actual_bits}"
#                 )
    
#     def get_numpy_type(self, type_str: str):
#         """המרת string type לnumpy type"""
#         if not HAS_NUMPY:
#             return None
            
#         type_map = {
#             "np.uint8": np.uint8,
#             "np.uint16": np.uint16,
#             "np.uint32": np.uint32,
#             "np.uint64": np.uint64,
#             "np.int8": np.int8,
#             "np.int16": np.int16,
#             "np.int32": np.int32,
#             "np.int64": np.int64,
#             "np.float32": np.float32,
#             "np.float64": np.float64,
#             "np.bytes_": np.bytes_
#         }
        
#         return type_map.get(type_str, np.uint8)


"""
Binary schema processor with fixed bit positions - updated for new format
"""

from typing import Dict, Any, Optional
import json
import os

# Check numpy availability
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

class BinarySchemaProcessor:
    """Binary schema processor with fixed bit positions"""
    
    def __init__(self, schema: Dict[str, Any], types_file: Optional[str] = None):
        """
        Initialize the binary schema processor
        
        Args:
            schema: Schema dictionary containing field definitions
            types_file: Optional path to external types mapping file
        """
        self.schema = schema
        self.schema_name = schema.get("schema_name", "unknown")
        self.endianness = schema.get("endianness", "little")
        self.total_bits = schema.get("total_bits", 0)
        self.validation = schema.get("validation", {})
        
        # Load type mapping
        self.type_mapping = self._load_type_mapping(types_file)
        
        # Parse schema fields
        self.fields = []
        self.fields_by_name = {}
        
        for field_name, field_info in schema.items():
            if field_name in ["schema_name", "endianness", "total_bits", "validation"]:
                continue
                
            # Parse bit position
            pos_str = field_info.get("pos", "0-7")
            start_bit, end_bit = map(int, pos_str.split("-"))
            
            # Map type according to mapping
            original_type = field_info.get("type", "uint8")
            mapped_type = self._map_type(original_type)
            
            field_data = {
                "name": field_name,
                "type": mapped_type,
                "original_type": original_type,
                "bits": field_info.get("bits", 8),
                "start_bit": start_bit,
                "end_bit": end_bit,
                "desc": field_info.get("desc", ""),
                "enum": self._process_enum(field_info),
                "original_info": field_info
            }
            
            self.fields.append(field_data)
            self.fields_by_name[field_name] = field_data
        
        # Sort by bit position
        self.fields.sort(key=lambda x: x["start_bit"])
        
        # Validate schema
        self._validate_schema()
    
    def _load_type_mapping(self, types_file: Optional[str]) -> Dict[str, str]:
        """
        Load type mapping from external file
        
        Args:
            types_file: Path to JSON file containing type mappings
            
        Returns:
            Dictionary mapping original types to target types
        """
        if types_file and os.path.exists(types_file):
            try:
                with open(types_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load types file: {e}")
        
        # Default mapping
        return {
            "uint8": "np.uint8",
            "int8": "np.int8",
            "uint16": "np.uint16",
            "uint32": "np.uint32",
            "uint64": "np.uint64",
            "int64": "np.int64",
            "float32": "np.float32",
            "bytes": "np.bytes_",
            "enum": "np.uint8"  # Default for enums
        }
    
    def _map_type(self, original_type: str) -> str:
        """
        Map type from schema to Python/NumPy type
        
        Args:
            original_type: Original type string from schema
            
        Returns:
            Mapped type string
        """
        if original_type == "enum":
            return "np.uint8"  # enums are stored as uint8
        return self.type_mapping.get(original_type, "np.uint8")
    
    def _process_enum(self, field_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process enum from new format
        
        Args:
            field_info: Field information dictionary
            
        Returns:
            Dictionary mapping enum indices to values
        """
        if field_info.get("type") != "enum":
            return {}
        
        values = field_info.get("values", [])
        if not values:
            return {}
        
        # Create numeric mapping to values
        enum_map = {}
        for i, value in enumerate(values):
            enum_map[str(i)] = value
        
        return enum_map
    
    def _validate_schema(self):
        """
        Validate the new schema format
        
        Raises:
            ValueError: If schema validation fails
        """
        if not self.fields:
            raise ValueError("Schema contains no valid fields")
        
        # Check for bit overlaps
        for i in range(len(self.fields) - 1):
            current = self.fields[i]
            next_field = self.fields[i + 1]
            
            if current["end_bit"] >= next_field["start_bit"]:
                raise ValueError(
                    f"Bit overlap between {current['name']} ({current['start_bit']}-{current['end_bit']}) "
                    f"and {next_field['name']} ({next_field['start_bit']}-{next_field['end_bit']})"
                )
        
        # Check total bits
        if self.fields:
            last_field = self.fields[-1]
            actual_bits = last_field["end_bit"] + 1
            
            if self.total_bits > 0 and actual_bits > self.total_bits:
                raise ValueError(
                    f"Schema fields extend beyond total_bits ({self.total_bits}): actual bits used = {actual_bits}"
                )
    
    def get_numpy_type(self, type_str: str):
        """
        Convert string type to numpy type
        
        Args:
            type_str: Type string (e.g., 'np.uint8')
            
        Returns:
            Corresponding numpy type or None if numpy not available
        """
        if not HAS_NUMPY:
            return None
            
        type_map = {
            "np.uint8": np.uint8,
            "np.uint16": np.uint16,
            "np.uint32": np.uint32,
            "np.uint64": np.uint64,
            "np.int8": np.int8,
            "np.int16": np.int16,
            "np.int32": np.int32,
            "np.int64": np.int64,
            "np.float32": np.float32,
            "np.float64": np.float64,
            "np.bytes_": np.bytes_
        }
        
        return type_map.get(type_str, np.uint8)
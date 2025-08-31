# # """
# # data_generators.py
# # מחוללי נתונים ספציפיים לשדות שונים
# # """

# # import random
# # import string
# # import struct
# # import time
# # from typing import Dict, Any, Union

# # from .types_and_enums import RecordType

# # class FieldDataGenerator:
# #     """מחולל נתונים לשדות שונים"""
    
# #     @staticmethod
# #     def generate_device_id() -> str:
# #         """יוצר device ID של 8 תווים ASCII"""
# #         chars = string.ascii_uppercase + string.digits
# #         return ''.join(random.choices(chars, k=8))
    
# #     @staticmethod
# #     def generate_enum_value(field: Dict[str, Any]) -> Union[int, str]:
# #         """יוצר ערך enum"""
# #         enum_map = field.get("enum", {})
# #         if enum_map:
# #             key = random.choice(list(enum_map.keys()))
# #             return enum_map[key]  # מחזיר את הערך (string)
# #         return 0
    
# #     @staticmethod
# #     def generate_value_bits(value_type: Union[int, str]) -> int:
# #         """יוצר value_bits לפי value_type"""
# #         if isinstance(value_type, str):
# #             # Convert string enum value to int
# #             type_map = {"float32": 0, "uint64": 1, "int64": 2, "bool": 3}
# #             value_type = type_map.get(value_type, 0)
        
# #         if value_type == 0:  # float32
# #             return struct.unpack('I', struct.pack('f', random.uniform(-1000, 1000)))[0]
# #         elif value_type == 1:  # uint64
# #             return random.randint(0, (1 << 32) - 1)  # מוגבל ל32 bit בפועל
# #         elif value_type == 2:  # int64
# #             return random.randint(-(1 << 31), (1 << 31) - 1) & ((1 << 64) - 1)
# #         elif value_type == 3:  # bool
# #             return 1 if random.random() > 0.5 else 0
# #         return 0
    
# #     @staticmethod
# #     def generate_generic_field_value(field: Dict[str, Any]) -> Any:
# #         """יצירה גנרית של ערך שדה"""
# #         field_type = field["type"]
# #         bits = field["bits"]
        
# #         if "int" in field_type:
# #             if "uint" in field_type:
# #                 return random.randint(0, (1 << bits) - 1)
# #             else:
# #                 max_val = (1 << (bits - 1)) - 1
# #                 return random.randint(-max_val - 1, max_val)
# #         elif "float" in field_type:
# #             return random.uniform(-1000, 1000)
# #         elif "bytes" in field_type:
# #             length = bits // 8
# #             return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
# #         else:
# #             return random.randint(0, (1 << bits) - 1)

# # class RecordDataPopulator:
# #     """מאכלס נתונים ברשומת טלמטריה"""
    
# #     def __init__(self, schema_processor):
# #         self.schema_processor = schema_processor
# #         self.field_generator = FieldDataGenerator()
    
# #     def populate_record_data(self, seq_id: int, timestamp: int) -> Dict[str, Any]:
# #         """מאכלס נתונים ברשומה לפי הסכמה"""
# #         data = {}
        
# #         for field in self.schema_processor.fields:
# #             field_name = field["name"]
# #             field_type = field["type"]
            
# #             if field_name == "schema_version":
# #                 data[field_name] = 1  # גרסה 1
# #             elif field_name == "device_id_ascii":
# #                 data[field_name] = self.field_generator.generate_device_id()
# #             elif field_name == "gpu_index":
# #                 data[field_name] = random.randint(0, 15)  # GPU 0-15
# #             elif field_name == "seq_no":
# #                 data[field_name] = seq_id
# #             elif field_name == "timestamp_ns":
# #                 data[field_name] = timestamp
# #             elif field_name == "scope":
# #                 data[field_name] = self.field_generator.generate_enum_value(field)
# #             elif field_name == "block_id":
# #                 data[field_name] = random.randint(0, 2048) if random.random() > 0.1 else 0xFFFF
# #             elif field_name == "thread_id":
# #                 data[field_name] = random.randint(0, 1024) if random.random() > 0.1 else 0xFFFF
# #             elif field_name == "metric_id":
# #                 data[field_name] = random.randint(1, 1000)
# #             elif field_name == "value_type":
# #                 data[field_name] = self.field_generator.generate_enum_value(field)
# #             elif field_name == "value_bits":
# #                 data[field_name] = self.field_generator.generate_value_bits(data.get("value_type", 0))
# #             elif field_name == "unit_code":
# #                 data[field_name] = random.randint(0, 255)
# #             elif field_name == "scale_1eN":
# #                 data[field_name] = random.randint(-9, 9)
# #             elif field_name == "crc32c":
# #                 data[field_name] = 0  # יחושב בארוז
# #             else:
# #                 # שדות אחרים - יצירה גנרית
# #                 data[field_name] = self.field_generator.generate_generic_field_value(field)
        
# #         return data
# """
# data_generators.py
# מחוללי נתונים ספציפיים לשדות שונים - מעודכן לסכמה החדשה
# """

# import random
# import string
# import struct
# import time
# from typing import Dict, Any, Union, List

# from .types_and_enums import RecordType

# class FieldDataGenerator:
#     """מחולל נתונים לשדות שונים"""
    
#     @staticmethod
#     def generate_device_id() -> str:
#         """יוצר device ID של 8 תווים ASCII"""
#         chars = string.ascii_uppercase + string.digits
#         return ''.join(random.choices(chars, k=8))
    
#     @staticmethod
#     def generate_enum_value(field: Dict[str, Any]) -> Union[int, str]:
#         """יוצר ערך enum מהפורמט החדש"""
#         enum_map = field.get("enum", {})
#         if enum_map:
#             # בפורמט החדש, המפתחות הם מספרים כstring והערכים הם strings
#             keys = list(enum_map.keys())
#             if keys:
#                 key = random.choice(keys)
#                 return int(key)  # מחזיר את האינדקס כמספר
#         return 0
    
#     @staticmethod
#     def get_enum_string_value(field: Dict[str, Any], index: int) -> str:
#         """מחזיר את הערך הstring של enum לפי אינדקס"""
#         enum_map = field.get("enum", {})
#         return enum_map.get(str(index), "UNKNOWN")
    
#     @staticmethod
#     def generate_value_bits(value_type: int) -> int:
#         """יוצר value_bits לפי value_type (מותאם לסכמה החדשה)"""
#         # בסכמה החדשה: 0=FLOAT32, 1=UINT64, 2=INT64, 3=BOOL
#         if value_type == 0:  # FLOAT32
#             float_val = random.uniform(-1000, 1000)
#             return struct.unpack('I', struct.pack('f', float_val))[0]
#         elif value_type == 1:  # UINT64
#             return random.randint(0, (1 << 48) - 1)  # משתמש ב-48 bit לבטיחות
#         elif value_type == 2:  # INT64
#             val = random.randint(-(1 << 47), (1 << 47) - 1)
#             if val < 0:
#                 val = val & ((1 << 64) - 1)  # Two's complement
#             return val
#         elif value_type == 3:  # BOOL
#             return 1 if random.random() > 0.5 else 0
#         return 0
    
#     @staticmethod
#     def generate_metric_id() -> int:
#         """יוצר metric ID (1-65535)"""
#         # משקל גבוה יותר למטריקות נפוצות (1-100)
#         if random.random() < 0.7:
#             return random.randint(1, 100)
#         else:
#             return random.randint(101, 1000)
    
#     @staticmethod
#     def generate_unit_code() -> int:
#         """יוצר unit code"""
#         # יחידות נפוצות: 0=none, 1=celsius, 2=watts, 3=percent, 4=MHz, etc.
#         common_units = [0, 1, 2, 3, 4, 5, 10, 11, 12]
#         if random.random() < 0.8:
#             return random.choice(common_units)
#         else:
#             return random.randint(0, 255)
    
#     @staticmethod
#     def generate_scale_1eN() -> int:
#         """יוצר scale factor (-9 to 9)"""
#         # רוב הערכים קרובים ל-0
#         if random.random() < 0.6:
#             return random.randint(-3, 3)
#         else:
#             return random.randint(-9, 9)
    
#     @staticmethod
#     def generate_generic_field_value(field: Dict[str, Any]) -> Any:
#         """יצירה גנרית של ערך שדה"""
#         field_type = field.get("original_type", field["type"])
#         bits = field["bits"]
        
#         if field_type == "enum":
#             return FieldDataGenerator.generate_enum_value(field)
#         elif "int" in field_type:
#             if "uint" in field_type:
#                 return random.randint(0, min((1 << bits) - 1, 2**63 - 1))
#             else:
#                 max_val = min((1 << (bits - 1)) - 1, 2**62 - 1)
#                 return random.randint(-max_val - 1, max_val)
#         elif "float" in field_type:
#             return random.uniform(-1000, 1000)
#         elif field_type == "bytes" or "bytes" in field_type:
#             length = bits // 8
#             return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
#         else:
#             return random.randint(0, min((1 << bits) - 1, 2**63 - 1))

# class RecordDataPopulator:
#     """מאכלס נתונים ברשומת טלמטריה לפי הסכמה החדשה"""
    
#     def __init__(self, schema_processor):
#         self.schema_processor = schema_processor
#         self.field_generator = FieldDataGenerator()
    
#     def populate_record_data(self, seq_id: int, timestamp: int) -> Dict[str, Any]:
#         """מאכלס נתונים ברשומה לפי הסכמה החדשה"""
#         data = {}
        
#         for field in self.schema_processor.fields:
#             field_name = field["name"]
            
#             # טיפול בשדות ספציפיים של הסכמה החדשה
#             if field_name == "schema_version":
#                 data[field_name] = 1  # גרסה 1
                
#             elif field_name == "device_id_ascii":
#                 data[field_name] = self.field_generator.generate_device_id()
                
#             elif field_name == "gpu_index":
#                 # GPU index 0-7 בדרך כלל, עד 255 לכל היותר
#                 data[field_name] = random.randint(0, min(7, (1 << field["bits"]) - 1))
                
#             elif field_name == "seq_no":
#                 data[field_name] = seq_id
                
#             elif field_name == "timestamp_ns":
#                 data[field_name] = timestamp
                
#             elif field_name == "scope":
#                 # enum: 0=DEVICE, 1=BLOCK, 2=THREAD
#                 scope_value = self.field_generator.generate_enum_value(field)
#                 data[field_name] = scope_value
                
#             elif field_name == "block_id":
#                 # 0xFFFF אם לא רלוונטי, אחרת 0-2047
#                 if data.get("scope", 0) >= 1:  # BLOCK או THREAD
#                     data[field_name] = random.randint(0, min(2047, (1 << field["bits"]) - 2))
#                 else:
#                     data[field_name] = 0xFFFF
                    
#             elif field_name == "thread_id":
#                 # 0xFFFF אם לא רלוונטי, אחרת 0-1023
#                 if data.get("scope", 0) == 2:  # THREAD
#                     data[field_name] = random.randint(0, min(1023, (1 << field["bits"]) - 2))
#                 else:
#                     data[field_name] = 0xFFFF
                    
#             elif field_name == "metric_id":
#                 data[field_name] = self.field_generator.generate_metric_id()
                
#             elif field_name == "value_type":
#                 # enum: 0=FLOAT32, 1=UINT64, 2=INT64, 3=BOOL
#                 value_type = self.field_generator.generate_enum_value(field)
#                 data[field_name] = value_type
                
#             elif field_name == "value_bits":
#                 # תלוי ב-value_type
#                 value_type = data.get("value_type", 0)
#                 data[field_name] = self.field_generator.generate_value_bits(value_type)
                
#             elif field_name == "unit_code":
#                 data[field_name] = self.field_generator.generate_unit_code()
                
#             elif field_name == "scale_1eN":
#                 data[field_name] = self.field_generator.generate_scale_1eN()
                
#             elif field_name == "crc32c":
#                 # יחושב בשלב הארוז
#                 data[field_name] = 0
                
#             else:
#                 # שדות אחרים - יצירה גנרית
#                 data[field_name] = self.field_generator.generate_generic_field_value(field)
        
#         return data
"""
data_generators.py
מחוללי נתונים ספציפיים לשדות שונים - מעודכן עם תמיכה ב-Fault Injection
"""

import random
import string
import struct
import time
from typing import Dict, Any, Union, List, Optional, Tuple

from .types_and_enums import RecordType
from .fault_injector import FaultInjector

class FieldDataGenerator:
    """מחולל נתונים לשדות שונים"""
    
    @staticmethod
    def generate_device_id() -> str:
        """יוצר device ID של 8 תווים ASCII"""
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choices(chars, k=8))
    
    @staticmethod
    def generate_enum_value(field: Dict[str, Any]) -> Union[int, str]:
        """יוצר ערך enum מהפורמט החדש"""
        enum_map = field.get("enum", {})
        if enum_map:
            # בפורמט החדש, המפתחות הם מספרים כstring והערכים הם strings
            keys = list(enum_map.keys())
            if keys:
                key = random.choice(keys)
                return int(key)  # מחזיר את האינדקס כמספר
        return 0
    
    @staticmethod
    def get_enum_string_value(field: Dict[str, Any], index: int) -> str:
        """מחזיר את הערך הstring של enum לפי אינדקס"""
        enum_map = field.get("enum", {})
        return enum_map.get(str(index), "UNKNOWN")
    
    @staticmethod
    def generate_value_bits(value_type: int) -> int:
        """יוצר value_bits לפי value_type (מותאם לסכמה החדשה)"""
        # בסכמה החדשה: 0=FLOAT32, 1=UINT64, 2=INT64, 3=BOOL
        if value_type == 0:  # FLOAT32
            float_val = random.uniform(-1000, 1000)
            return struct.unpack('I', struct.pack('f', float_val))[0]
        elif value_type == 1:  # UINT64
            return random.randint(0, (1 << 48) - 1)  # משתמש ב-48 bit לבטיחות
        elif value_type == 2:  # INT64
            val = random.randint(-(1 << 47), (1 << 47) - 1)
            if val < 0:
                val = val & ((1 << 64) - 1)  # Two's complement
            return val
        elif value_type == 3:  # BOOL
            return 1 if random.random() > 0.5 else 0
        return 0
    
    @staticmethod
    def generate_metric_id() -> int:
        """יוצר metric ID (1-65535)"""
        # משקל גבוה יותר למטריקות נפוצות (1-100)
        if random.random() < 0.7:
            return random.randint(1, 100)
        else:
            return random.randint(101, 1000)
    
    @staticmethod
    def generate_unit_code() -> int:
        """יוצר unit code"""
        # יחידות נפוצות: 0=none, 1=celsius, 2=watts, 3=percent, 4=MHz, etc.
        common_units = [0, 1, 2, 3, 4, 5, 10, 11, 12]
        if random.random() < 0.8:
            return random.choice(common_units)
        else:
            return random.randint(0, 255)
    
    @staticmethod
    def generate_scale_1eN() -> int:
        """יוצר scale factor (-9 to 9)"""
        # רוב הערכים קרובים ל-0
        if random.random() < 0.6:
            return random.randint(-3, 3)
        else:
            return random.randint(-9, 9)
    
    @staticmethod
    def generate_generic_field_value(field: Dict[str, Any]) -> Any:
        """יצירה גנרית של ערך שדה"""
        field_type = field.get("original_type", field["type"])
        bits = field["bits"]
        
        if field_type == "enum":
            return FieldDataGenerator.generate_enum_value(field)
        elif "int" in field_type:
            if "uint" in field_type:
                return random.randint(0, min((1 << bits) - 1, 2**63 - 1))
            else:
                max_val = min((1 << (bits - 1)) - 1, 2**62 - 1)
                return random.randint(-max_val - 1, max_val)
        elif "float" in field_type:
            return random.uniform(-1000, 1000)
        elif field_type == "bytes" or "bytes" in field_type:
            length = bits // 8
            return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        else:
            return random.randint(0, min((1 << bits) - 1, 2**63 - 1))


class FaultAwareRecordDataPopulator:
    """מאכלס נתונים ברשומת טלמטריה עם תמיכה ב-Fault Injection"""
    
    def __init__(self, schema_processor, fault_injector: Optional[FaultInjector] = None):
        self.schema_processor = schema_processor
        self.field_generator = FieldDataGenerator()
        self.fault_injector = fault_injector
        
        # מטמון לביצועים
        self._field_types_cache = {}
        self._enum_fields_cache = {}
    
    def populate_record_data(
        self, 
        seq_id: int, 
        timestamp: int,
        inject_faults: bool = True
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        מאכלס נתונים ברשומה עם אפשרות הכנסת שגיאות
        
        Returns:
            Tuple of (data_dict, fault_details)
        """
        data = self._generate_clean_data(seq_id, timestamp)
        fault_details = []
        
        # הכנסת שגיאות אם נדרש
        if inject_faults and self.fault_injector:
            # יצירת רשומה זמנית להכנסת שגיאות
            from .types_and_enums import TelemetryRecord, RecordType
            temp_record = TelemetryRecord(
                record_type=RecordType.UPDATE,
                timestamp=timestamp,
                sequence_id=seq_id,
                data=data
            )
            
            # הכנסת שגיאות
            faulty_record, fault_details = self.fault_injector.inject_faults(temp_record)
            data = faulty_record.data
        
        return data, fault_details
    
    def _generate_clean_data(self, seq_id: int, timestamp: int) -> Dict[str, Any]:
        """יצירת נתונים נקיים ללא שגיאות"""
        data = {}
        
        for field in self.schema_processor.fields:
            field_name = field["name"]
            
            # טיפול בשדות ספציפיים של הסכמה החדשה
            if field_name == "schema_version":
                data[field_name] = 1  # גרסה 1
                
            elif field_name == "device_id_ascii":
                data[field_name] = self.field_generator.generate_device_id()
                
            elif field_name == "gpu_index":
                # GPU index 0-7 בדרך כלל, עד 255 לכל היותר
                data[field_name] = random.randint(0, min(7, (1 << field["bits"]) - 1))
                
            elif field_name == "seq_no":
                data[field_name] = seq_id
                
            elif field_name == "timestamp_ns":
                data[field_name] = timestamp
                
            elif field_name == "scope":
                # enum: 0=DEVICE, 1=BLOCK, 2=THREAD
                scope_value = self.field_generator.generate_enum_value(field)
                data[field_name] = scope_value
                
            elif field_name == "block_id":
                # 0xFFFF אם לא רלוונטי, אחרת 0-2047
                if data.get("scope", 0) >= 1:  # BLOCK או THREAD
                    data[field_name] = random.randint(0, min(2047, (1 << field["bits"]) - 2))
                else:
                    data[field_name] = 0xFFFF
                    
            elif field_name == "thread_id":
                # 0xFFFF אם לא רלוונטי, אחרת 0-1023
                if data.get("scope", 0) == 2:  # THREAD
                    data[field_name] = random.randint(0, min(1023, (1 << field["bits"]) - 2))
                else:
                    data[field_name] = 0xFFFF
                    
            elif field_name == "metric_id":
                data[field_name] = self.field_generator.generate_metric_id()
                
            elif field_name == "value_type":
                # enum: 0=FLOAT32, 1=UINT64, 2=INT64, 3=BOOL
                value_type = self.field_generator.generate_enum_value(field)
                data[field_name] = value_type
                
            elif field_name == "value_bits":
                # תלוי ב-value_type
                value_type = data.get("value_type", 0)
                data[field_name] = self.field_generator.generate_value_bits(value_type)
                
            elif field_name == "unit_code":
                data[field_name] = self.field_generator.generate_unit_code()
                
            elif field_name == "scale_1eN":
                data[field_name] = self.field_generator.generate_scale_1eN()
                
            elif field_name == "crc32c":
                # יחושב בשלב הארוז
                data[field_name] = 0
                
            else:
                # שדות אחרים - יצירה גנרית
                data[field_name] = self.field_generator.generate_generic_field_value(field)
        
        return data
    
    def enable_fault_injection(self, fault_injector: FaultInjector):
        """הפעלת מנגנון שגיאות"""
        self.fault_injector = fault_injector
    
    def disable_fault_injection(self):
        """כיבוי מנגנון שגיאות"""
        self.fault_injector = None
    
    def get_fault_statistics(self) -> Optional[Dict[str, Any]]:
        """קבלת סטטיסטיקות שגיאות"""
        if self.fault_injector:
            return self.fault_injector.get_statistics()
        return None


# Backwards compatibility - maintain original class name
class RecordDataPopulator(FaultAwareRecordDataPopulator):
    """Alias לתאימות לאחור"""
    
    def __init__(self, schema_processor):
        super().__init__(schema_processor, None)
    
    def populate_record_data(self, seq_id: int, timestamp: int,inject_faults: bool = False, fault_config: dict = None) -> Dict[str, Any]:
        """מתודה מקורית ללא fault injection"""
        data, _ = super().populate_record_data(seq_id, timestamp, inject_faults=inject_faults, fault_config=fault_config)
        return data

   
# Factory functions for different scenarios
def create_clean_populator(schema_processor) -> RecordDataPopulator:
    """יוצר populator ללא שגיאות"""
    return RecordDataPopulator(schema_processor)

def create_development_populator(schema_processor, logger=None) -> FaultAwareRecordDataPopulator:
    """יוצר populator עם שגיאות קלות לפיתוח"""
    from .fault_injector import create_development_fault_injector
    fault_injector = create_development_fault_injector(schema_processor, logger)
    return FaultAwareRecordDataPopulator(schema_processor, fault_injector)

def create_testing_populator(schema_processor, logger=None) -> FaultAwareRecordDataPopulator:
    """יוצר populator עם שגיאות מגוונות לבדיקות"""
    from .fault_injector import create_testing_fault_injector
    fault_injector = create_testing_fault_injector(schema_processor, logger)
    return FaultAwareRecordDataPopulator(schema_processor, fault_injector)

def create_stress_populator(schema_processor, logger=None) -> FaultAwareRecordDataPopulator:
    """יוצר populator עם שגיאות רבות לבדיקות עומס"""
    from .fault_injector import create_stress_fault_injector
    fault_injector = create_stress_fault_injector(schema_processor, logger)
    return FaultAwareRecordDataPopulator(schema_processor, fault_injector)

"""
fault_injector.py
מנגנון הכנסת שגיאות מבוקרות לדמות מציאות אמיתית
"""

import random
import struct
import json
import logging
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Union, Set
from dataclasses import dataclass, field

from .types_and_enums import RecordType, TelemetryRecord

class FaultType(Enum):
    """סוגי שגיאות שניתן להכניס"""
    OUT_OF_RANGE = "out_of_range"           # ערכים מחוץ לטווח
    WRONG_TYPE = "wrong_type"               # טיפוס לא נכון
    MISSING_FIELD = "missing_field"         # שדה חסר
    INVALID_STRUCTURE = "invalid_structure"  # מבנה לא תקין
    NULL_VALUES = "null_values"             # ערכים ריקים/null
    ENCODING_ERROR = "encoding_error"       # שגיאות קידוד
    ENUM_INVALID = "enum_invalid"           # ערכי enum לא חוקיים
    STRING_CORRUPTION = "string_corruption"  # השחתת מחרוזות
    TIMESTAMP_DRIFT = "timestamp_drift"     # סטיית זמן
    SEQUENCE_BREAK = "sequence_break"       # שבירת רצף

@dataclass
class FaultConfig:
    """הגדרת שגיאה ספציפית"""
    fault_type: FaultType
    probability: float  # סבירות להתרחש (0.0-1.0)
    severity: str = "medium"  # low, medium, high, critical
    field_patterns: List[str] = field(default_factory=list)  # שדות שעלולים להיות מושפעים
    exclude_fields: List[str] = field(default_factory=list)  # שדות לא לגעת בהם
    parameters: Dict[str, Any] = field(default_factory=dict)  # פרמטרים נוספים
    
    def should_inject(self) -> bool:
        """האם להכניס שגיאה עכשיו"""
        return random.random() < self.probability

class FaultStatistics:
    """סטטיסטיקות שגיאות"""
    
    def __init__(self):
        self.total_records = 0
        self.faulty_records = 0
        self.fault_counts: Dict[FaultType, int] = {}
        self.field_faults: Dict[str, int] = {}
        self.severity_counts: Dict[str, int] = {}
    
    def record_fault(self, fault_type: FaultType, field_name: str, severity: str):
        """רישום שגיאה"""
        self.faulty_records += 1
        self.fault_counts[fault_type] = self.fault_counts.get(fault_type, 0) + 1
        self.field_faults[field_name] = self.field_faults.get(field_name, 0) + 1
        self.severity_counts[severity] = self.severity_counts.get(severity, 0) + 1
    
    def record_normal(self):
        """רישום רשומה תקינה"""
        self.total_records += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """סיכום סטטיסטיקות"""
        fault_rate = (self.faulty_records / self.total_records * 100) if self.total_records > 0 else 0
        
        return {
            "total_records": self.total_records,
            "faulty_records": self.faulty_records,
            "fault_rate_percent": round(fault_rate, 2),
            "fault_types": dict(self.fault_counts),
            "affected_fields": dict(self.field_faults),
            "severity_distribution": dict(self.severity_counts)
        }

class FaultInjector:
    """המחלקה הראשית להכנסת שגיאות"""
    
    def __init__(
        self,
        schema_processor,
        fault_configs: List[FaultConfig] = None,
        global_fault_rate: float = 0.05,  # 5% שגיאות כברירת מחדל
        logger: Optional[logging.Logger] = None
    ):
        self.schema_processor = schema_processor
        self.global_fault_rate = global_fault_rate
        self.logger = logger or logging.getLogger(__name__)
        
        # הגדרת שגיאות
        self.fault_configs = fault_configs or self._create_default_configs()
        
        # סטטיסטיקות
        self.statistics = FaultStatistics()
        
        # מטמון לביצועים
        self._field_cache = {}
        self._enum_cache = {}
        
        self.logger.info(f"FaultInjector initialized with {len(self.fault_configs)} fault types, "
                        f"global rate: {global_fault_rate:.1%}")
    
    def _create_default_configs(self) -> List[FaultConfig]:
        """יצירת הגדרות שגיאות ברירת מחדל"""
        configs = [
            # ערכים מחוץ לטווח
            FaultConfig(
                fault_type=FaultType.OUT_OF_RANGE,
                probability=0.25,  # 2%
                severity="medium",
                field_patterns=["*_id", "metric_id", "gpu_index", "value_*"],
                parameters={
                    "multiplier_range": (2, 10),  # כפול ב 2-10
                    "negative_chance": 0.3,       # 30% סיכוי לשלילי
                    "extreme_values": [0, -1, 999999, 0xFFFFFFFF]
                }
            ),
            
            # טיפוס לא נכון
            FaultConfig(
                fault_type=FaultType.WRONG_TYPE,
                probability=0.25,  # 1.5%
                severity="high",
                field_patterns=["timestamp_*", "seq_*", "*_id"],
                exclude_fields=["device_id_ascii"],  # זה תמיד צריך להיות string
                parameters={
                    "string_instead_of_int": True,
                    "int_instead_of_string": True,
                    "bool_instead_of_int": True
                }
            ),
            
            # שדות חסרים
            FaultConfig(
                fault_type=FaultType.MISSING_FIELD,
                probability=0.01,  # 1%
                severity="high",
                exclude_fields=["schema_version", "crc32c"],  # שדות קריטיים
                parameters={
                    "max_missing_fields": 2,
                    "prefer_optional_fields": True
                }
            ),
            
            # ערכי enum לא חוקיים
            FaultConfig(
                fault_type=FaultType.ENUM_INVALID,
                probability=0.015,  # 1.5%
                severity="medium",
                field_patterns=["scope", "value_type", "*_type"],
                parameters={
                    "invalid_values": [99, 255, -1, "INVALID", "UNKNOWN"]
                }
            ),
            
            # ערכים null/ריקים
            FaultConfig(
                fault_type=FaultType.NULL_VALUES,
                probability=0.01,  # 1%
                severity="medium",
                exclude_fields=["schema_version", "timestamp_ns"],
                parameters={
                    "null_chance": 0.5,
                    "empty_string_chance": 0.3,
                    "zero_chance": 0.2
                }
            ),
            
            # השחתת מחרוזות
            FaultConfig(
                fault_type=FaultType.STRING_CORRUPTION,
                probability=0.008,  # 0.8%
                severity="low",
                field_patterns=["device_id_*", "*_ascii"],
                parameters={
                    "corruption_types": ["truncate", "pad_null", "invalid_chars", "encoding_error"]
                }
            ),
            
            # סטיית זמן
            FaultConfig(
                fault_type=FaultType.TIMESTAMP_DRIFT,
                probability=0.005,  # 0.5%
                severity="low",
                field_patterns=["timestamp_*"],
                parameters={
                    "drift_seconds": (-3600, 3600),  # עד שעה אחורה/קדימה
                    "future_chance": 0.1,  # 10% לעתיד
                    "way_past_chance": 0.05  # 5% לעבר הרחוק
                }
            ),
            
            # שבירת רצף
            FaultConfig(
                fault_type=FaultType.SEQUENCE_BREAK,
                probability=0.003,  # 0.3%
                severity="medium",
                field_patterns=["seq_*"],
                parameters={
                    "jump_forward": (100, 10000),
                    "jump_backward": (1, 50),
                    "duplicate_chance": 0.2
                }
            )
        ]
        
        return configs
    
    def should_inject_fault(self) -> bool:
        """האם להכניס שגיאה ברשומה הנוכחית"""
        return random.random() < self.global_fault_rate
    
    def inject_faults(self, record: TelemetryRecord) -> Tuple[TelemetryRecord, List[Dict[str, Any]]]:
        """
        הכנסת שגיאות לרשומה
        
        Returns:
            Tuple of (modified_record, fault_details)
        """
        self.statistics.total_records += 1
        
        if not self.should_inject_fault():
            self.statistics.record_normal()
            return record, []
        
        # יצירת עותק של הרשומה
        import copy
        faulty_record = copy.deepcopy(record)
        fault_details = []
        
        # בחירת שגיאות להכנסה
        for config in self.fault_configs:
            if config.should_inject():
                try:
                    fault_applied = self._apply_fault(faulty_record, config)
                    if fault_applied:
                        fault_details.extend(fault_applied)
                except Exception as e:
                    self.logger.warning(f"Failed to apply fault {config.fault_type}: {e}")
        
        if fault_details:
            self.logger.debug(f"Applied {len(fault_details)} faults to record {record.sequence_id}")
        
        return faulty_record, fault_details
    
    def _apply_fault(self, record: TelemetryRecord, config: FaultConfig) -> List[Dict[str, Any]]:
        """
        יישום שגיאה ספציפית
        
        Returns:
            רשימת פרטי השגיאות שיושמו
        """
        fault_details = []
        
        # בחירת שדות מתאימים
        target_fields = self._select_target_fields(record, config)
        
        if not target_fields:
            return fault_details
        
        # יישום השגיאה לכל שדה נבחר
        for field_name in target_fields:
            try:
                fault_detail = self._inject_field_fault(record, field_name, config)
                if fault_detail:
                    fault_details.append(fault_detail)
                    self.statistics.record_fault(config.fault_type, field_name, config.severity)
            except Exception as e:
                self.logger.warning(f"Failed to inject {config.fault_type} in field {field_name}: {e}")
        
        return fault_details
    
    def _select_target_fields(self, record: TelemetryRecord, config: FaultConfig) -> List[str]:
        """בחירת שדות מטרה לשגיאה"""
        available_fields = set(record.data.keys())
        
        # סינון לפי exclude_fields
        if config.exclude_fields:
            for exclude_pattern in config.exclude_fields:
                if exclude_pattern in available_fields:
                    available_fields.discard(exclude_pattern)
        
        # בחירה לפי field_patterns
        target_fields = set()
        
        if config.field_patterns:
            for pattern in config.field_patterns:
                if pattern == "*":
                    target_fields.update(available_fields)
                elif "*" in pattern:
                    # Pattern matching
                    prefix = pattern.replace("*", "")
                    for field in available_fields:
                        if field.startswith(prefix) or field.endswith(prefix.replace("*", "")):
                            target_fields.add(field)
                elif pattern in available_fields:
                    target_fields.add(pattern)
        else:
            # אם אין patterns, בחר שדה אקראי
            if available_fields:
                target_fields.add(random.choice(list(available_fields)))
        
        # הגבלת מספר השדות
        max_fields = config.parameters.get("max_fields", 2)
        if len(target_fields) > max_fields:
            target_fields = set(random.sample(list(target_fields), max_fields))
        
        return list(target_fields & available_fields)
    
    def _inject_field_fault(
        self, 
        record: TelemetryRecord, 
        field_name: str, 
        config: FaultConfig
    ) -> Optional[Dict[str, Any]]:
        """הכנסת שגיאה לשדה ספציפי"""
        
        if field_name not in record.data:
            return None
        
        original_value = record.data[field_name]
        fault_detail = {
            "fault_type": config.fault_type.value,
            "field_name": field_name,
            "original_value": original_value,
            "severity": config.severity
        }
        
        try:
            new_value = original_value  # Default fallback
            
            if config.fault_type == FaultType.OUT_OF_RANGE:
                new_value = self._inject_out_of_range(field_name, original_value, config)
                
            elif config.fault_type == FaultType.WRONG_TYPE:
                new_value = self._inject_wrong_type(field_name, original_value, config)
                
            elif config.fault_type == FaultType.MISSING_FIELD:
                # מחיקת השדה
                del record.data[field_name]
                fault_detail["new_value"] = "<DELETED>"
                return fault_detail
                
            elif config.fault_type == FaultType.ENUM_INVALID:
                new_value = self._inject_invalid_enum(field_name, original_value, config)
                
            elif config.fault_type == FaultType.NULL_VALUES:
                new_value = self._inject_null_value(field_name, original_value, config)
                
            elif config.fault_type == FaultType.STRING_CORRUPTION:
                new_value = self._inject_string_corruption(field_name, original_value, config)
                
            elif config.fault_type == FaultType.TIMESTAMP_DRIFT:
                new_value = self._inject_timestamp_drift(field_name, original_value, config)
                
            elif config.fault_type == FaultType.SEQUENCE_BREAK:
                new_value = self._inject_sequence_break(field_name, original_value, config)
                
            else:
                self.logger.warning(f"Unknown fault type: {config.fault_type}")
                return None
            
            # וודא שהערך החדש לא None
            if new_value is None:
                new_value = 0
            
            record.data[field_name] = new_value
            fault_detail["new_value"] = new_value
            
            return fault_detail
            
        except Exception as e:
            self.logger.error(f"Error injecting {config.fault_type} in {field_name}: {e}")
            return None
    
    def _inject_out_of_range(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """הכנסת ערכים מחוץ לטווח"""
        params = config.parameters
        field_info = self._get_field_info(field_name)

        # אם הערך המקורי אינו מספרי – נחזיר ערך מספרי מוגדר מראש
        if not isinstance(original_value, (int, float)):
            return random.choice(params.get("extreme_values", [999999, 0xFFFFFFFF]))

        # ערכים קיצוניים מוגדרים מראש
        extreme_values = params.get("extreme_values", [])
        if extreme_values and random.random() < 0.3:
            return random.choice(extreme_values)

        # כפל בטווח
        multiplier_range = params.get("multiplier_range", (2, 5))
        multiplier = random.uniform(*multiplier_range)
        new_value = original_value * multiplier

        # סיכוי לערך שלילי
        if params.get("negative_chance", 0) > random.random():
            new_value = -abs(new_value)

        # ווידוא שהערך באמת מחוץ לטווח
        if field_info and field_info.get("bits"):
            max_val = (1 << field_info["bits"]) - 1
            if new_value <= max_val:
                new_value = max_val + random.randint(1, 1000)

        # תמיד להחזיר int אם השדה הוא int
        if isinstance(original_value, int):
            new_value = int(new_value)
        return new_value

    
    def _inject_wrong_type(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """הכנסת טיפוס שגוי"""
        params = config.parameters
        
        if isinstance(original_value, int):
            if params.get("string_instead_of_int", True) and random.random() < 0.6:
                return f"invalid_{random.randint(1, 999)}"
            elif params.get("bool_instead_of_int", True):
                return random.choice([True, False, "true", "false"])
        
        elif isinstance(original_value, str):
            if params.get("int_instead_of_string", True):
                return random.randint(-999, 999)
        
        elif isinstance(original_value, bool):
            return random.choice([2, "maybe", "yes"])
        
        # ברירת מחדל - החזר ערך מוזר
        return f"TYPE_ERROR_{type(original_value).__name__}"
    
    def _inject_invalid_enum(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """הכנסת ערך enum לא חוקי"""
        invalid_values = config.parameters.get("invalid_values", [99, 255, -1])
        return random.choice(invalid_values)
    
    def _inject_null_value(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """הכנסת ערכים null/ריקים"""
        params = config.parameters
        rand = random.random()
        
        if rand < params.get("null_chance", 0.4):
            # במקום None שגורם לבעיות, נחזיר 0
            return 0
        elif rand < params.get("empty_string_chance", 0.3) + params.get("null_chance", 0.4):
            if isinstance(original_value, str):
                return ""
            return 0
        elif rand < params.get("zero_chance", 0.2) + params.get("empty_string_chance", 0.3) + params.get("null_chance", 0.4):
            return 0
        else:
            return original_value
    
    def _inject_string_corruption(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """השחתת מחרוזות"""
        if not isinstance(original_value, str):
            return original_value
        
        corruption_types = config.parameters.get("corruption_types", ["truncate"])
        corruption_type = random.choice(corruption_types)
        
        if corruption_type == "truncate":
            if len(original_value) > 1:
                cut_at = random.randint(0, len(original_value) - 1)
                return original_value[:cut_at]
        
        elif corruption_type == "pad_null":
            return original_value + "\x00" * random.randint(1, 3)
        
        elif corruption_type == "invalid_chars":
            # החלפת תו אקראי בתו לא חוקי
            if original_value:
                pos = random.randint(0, len(original_value) - 1)
                invalid_char = random.choice(["\xFF", "\x00", "�", "🚫"])
                return original_value[:pos] + invalid_char + original_value[pos+1:]
        
        elif corruption_type == "encoding_error":
            try:
                # נסיון קידוד שגוי
                return original_value.encode('utf-8').decode('latin1', errors='ignore')
            except:
                return original_value + "�"
        
        return original_value
    
    def _inject_timestamp_drift(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """סטיית זמן"""
        if not isinstance(original_value, int):
            return original_value
        
        params = config.parameters
        drift_range = params.get("drift_seconds", (-3600, 3600))
        
        # סטייה רגילה
        if random.random() < 0.8:
            drift_seconds = random.randint(*drift_range)
            return original_value + (drift_seconds * 1_000_000_000)  # ns
        
        # עתיד
        elif random.random() < params.get("future_chance", 0.1):
            future_drift = random.randint(86400, 365*86400)  # יום עד שנה
            return original_value + (future_drift * 1_000_000_000)
        
        # עבר הרחוק
        else:
            past_drift = random.randint(365*86400, 10*365*86400)  # שנה עד 10 שנים
            return max(0, original_value - (past_drift * 1_000_000_000))
    
    def _inject_sequence_break(self, field_name: str, original_value: Any, config: FaultConfig) -> Any:
        """שבירת רצף"""
        if not isinstance(original_value, int):
            return original_value
        
        params = config.parameters
        
        # דילוג קדימה
        if random.random() < 0.5:
            jump = random.randint(*params.get("jump_forward", (100, 1000)))
            return original_value + jump
        
        # דילוג אחורה
        elif random.random() < 0.3:
            jump = random.randint(*params.get("jump_backward", (1, 50)))
            return max(0, original_value - jump)
        
        # כפילות
        else:
            return original_value  # יישאר אותו ערך (יצור כפילות)
    
    def _get_field_info(self, field_name: str) -> Optional[Dict[str, Any]]:
        """קבלת מידע על שדה מהסכמה"""
        if field_name in self._field_cache:
            return self._field_cache[field_name]
        
        field_info = self.schema_processor.fields_by_name.get(field_name)
        self._field_cache[field_name] = field_info
        return field_info
    
    def get_statistics(self) -> Dict[str, Any]:
        """קבלת סטטיסטיקות שגיאות"""
        return self.statistics.get_summary()
    
    def reset_statistics(self):
        """איפוס סטטיסטיקות"""
        self.statistics = FaultStatistics()
    
    def load_config_from_file(self, config_file: str):
        """טעינת הגדרות שגיאות מקובץ JSON"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            self.global_fault_rate = config_data.get("global_fault_rate", self.global_fault_rate)
            
            # טעינת הגדרות שגיאות
            fault_configs = []
            for fault_data in config_data.get("fault_configs", []):
                config = FaultConfig(
                    fault_type=FaultType(fault_data["fault_type"]),
                    probability=fault_data["probability"],
                    severity=fault_data.get("severity", "medium"),
                    field_patterns=fault_data.get("field_patterns", []),
                    exclude_fields=fault_data.get("exclude_fields", []),
                    parameters=fault_data.get("parameters", {})
                )
                fault_configs.append(config)
            
            self.fault_configs = fault_configs
            self.logger.info(f"Loaded fault configuration from {config_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to load fault config from {config_file}: {e}")
            raise
    
    def save_config_to_file(self, config_file: str):
        """שמירת הגדרות שגיאות לקובץ JSON"""
        config_data = {
            "global_fault_rate": self.global_fault_rate,
            "fault_configs": []
        }
        
        for config in self.fault_configs:
            config_data["fault_configs"].append({
                "fault_type": config.fault_type.value,
                "probability": config.probability,
                "severity": config.severity,
                "field_patterns": config.field_patterns,
                "exclude_fields": config.exclude_fields,
                "parameters": config.parameters
            })
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved fault configuration to {config_file}")


# Factory functions for common scenarios
def create_development_fault_injector(schema_processor, logger=None) -> FaultInjector:
    """יצירת fault injector לפיתוח - שגיאות קלות"""
    configs = [
        FaultConfig(FaultType.OUT_OF_RANGE, 0.01, "low"),
        FaultConfig(FaultType.NULL_VALUES, 0.005, "low")
    ]
    return FaultInjector(schema_processor, configs, 0.02, logger)

def create_testing_fault_injector(schema_processor, logger=None) -> FaultInjector:
    """יצירת fault injector לבדיקות - שגיאות מגוונות"""
    return FaultInjector(schema_processor, None, 0.05, logger)

def create_stress_fault_injector(schema_processor, logger=None) -> FaultInjector:
    """יצירת fault injector לבדיקות עומס - שגיאות רבות"""
    configs = [
        FaultConfig(FaultType.OUT_OF_RANGE, 0.05, "high"),
        FaultConfig(FaultType.WRONG_TYPE, 0.03, "high"), 
        FaultConfig(FaultType.MISSING_FIELD, 0.02, "high"),
        FaultConfig(FaultType.ENUM_INVALID, 0.03, "medium"),
        FaultConfig(FaultType.STRING_CORRUPTION, 0.02, "medium")
    ]
    return FaultInjector(schema_processor, configs, 0.15, logger)
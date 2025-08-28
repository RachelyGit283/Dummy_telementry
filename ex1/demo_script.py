#!/usr/bin/env python3
"""
Demo Script - דוגמא מעשית לשימוש במחלקת Enhanced TelemetryGeneratorPro

שימוש:
    python demo_script.py
    
מה הסקריפט עושה:
1. יוצר קובץ schema.json לדוגמא
2. טוען אותו במחלקה
3. יוצר קבצים בינאריים
4. מציג סטטיסטיקות
"""

import os
import json
import time
from pathlib import Path

# ייבוא המחלקות שלנו
from generator import (
    EnhancedTelemetryGeneratorPro,
    RecordType,
    OutputFormat
)

def create_demo_schema():
    """יוצר schema לדוגמא עם מגוון סוגי שדות"""
    schema = {
        # Float fields
        "cpu_temperature": {
            "type": "float", 
            "bits": 32, 
            "min": 30.0, 
            "max": 90.0,
            "encoding": "ieee754"
        },
        "voltage": {
            "type": "float", 
            "bits": 32, 
            "min": 11.5, 
            "max": 12.5
        },
        
        # Integer fields
        "cpu_usage": {"type": "int", "bits": 8},  # 0-255
        "memory_usage": {"type": "int", "bits": 8},  # 0-255
        "fan_speed": {"type": "int", "bits": 12},  # 0-4095 RPM
        
        # String fields
        "hostname": {
            "type": "string", 
            "length": 12, 
            "compressed": True,
            "char_bits": 6
        },
        "device_id": {
            "type": "string", 
            "length": 8, 
            "max_length": 16
        },
        
        # Boolean field
        "is_online": {"type": "bool"},
        
        # String enums
        "system_status": {
            "type": "enum", 
            "values": ["healthy", "warning", "critical", "maintenance", "offline"]
        },
        "alert_level": {
            "type": "enum", 
            "values": ["none", "low", "medium", "high"]
        },
        
        # Integer enum
        "error_code": {
            "type": "enum", 
            "values": [0, 100, 200, 404, 500, 503]
        },
        
        # Time field
        "uptime_seconds": {
            "type": "time", 
            "bits": 32, 
            "range": 604800  # 1 week in seconds
        }
    }
    
    return schema

def main():
    print("=== Enhanced TelemetryGeneratorPro Demo ===")
    print("יוצר נתוני טלמטריה מזוייפים לדימוי עומס\n")
    
    # יצירת תיקיות
    output_dir = Path("./demo_output")
    output_dir.mkdir(exist_ok=True)
    
    schema_file = "demo_schema.json"
    
    try:
        # שלב 1: יצירת Schema
        print("📄 יוצר קובץ schema...")
        demo_schema = create_demo_schema()
        
        with open(schema_file, 'w', encoding='utf-8') as f:
            json.dump(demo_schema, f, indent=2, ensure_ascii=False)
        
        print(f"   ✓ נוצר: {schema_file}")
        print(f"   📊 {len(demo_schema)} שדות בסכמה")
        
        # שלב 2: טעינת המחלקה
        print("\n🔧 טוען schema ויוצר מחולל...")
        generator = EnhancedTelemetryGeneratorPro(
            schema_file=schema_file,
            output_dir=str(output_dir),
            add_sequential_id=True,
            enable_gpu=False  # לדוגמא ללא GPU
        )
        
        # הצגת מידע על הסכמה
        info = generator.get_enhanced_schema_info()
        print(f"   ✓ נטען בהצלחה")
        print(f"   📏 {info['data_fields_count']} שדות נתונים")
        print(f"   💾 {info['bytes_per_record']} bytes לרשומה")
        print(f"   🔧 {info['overhead_bits']} bits overhead")
        
        # שלב 3: יצירת קובץ בינארי יחיד
        print("\n⚡ יוצר קובץ בינארי יחיד...")
        single_file = output_dir / "telemetry_single.bin"
        
        start_time = time.time()
        generator.write_records_enhanced(
            str(single_file),
            10000,  # 10K records
            output_format=OutputFormat.BINARY,
            record_type_ratio={
                RecordType.UPDATE: 0.8,  # 80% updates
                RecordType.EVENT: 0.2    # 20% events
            }
        )
        elapsed = time.time() - start_time
        
        file_size = single_file.stat().st_size
        print(f"   ✓ נוצר: {single_file.name}")
        print(f"   📊 10,000 records ב-{elapsed:.2f} שניות")
        print(f"   💾 גודל: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        print(f"   ⚡ מהירות: {10000/elapsed:,.0f} records/sec")
        
        # שלב 4: יצירת קבצים מרובים במקביל
        print("\n🚀 יוצר קבצים מרובים במקביל...")
        
        start_time = time.time()
        binary_paths = generator.generate_multiple_files_enhanced(
            num_files=5,
            records_per_file=20000,  # 100K total records
            file_prefix="telemetry_parallel",
            output_format=OutputFormat.BINARY,
            max_workers=4,
            record_type_ratio={
                RecordType.UPDATE: 0.7,
                RecordType.EVENT: 0.3
            }
        )
        elapsed = time.time() - start_time
        
        total_size = sum(Path(p).stat().st_size for p in binary_paths)
        total_records = 5 * 20000
        
        print(f"   ✓ נוצרו {len(binary_paths)} קבצים")
        print(f"   📊 {total_records:,} records ב-{elapsed:.2f} שניות")
        print(f"   💾 סה\"כ גודל: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        print(f"   ⚡ מהירות: {total_records/elapsed:,.0f} records/sec")
        
        # שלב 5: יצירת קובץ JSON לדיבוגינג
        print("\n🐛 יוצר קובץ JSON לדיבוגינג...")
        
        json_file = output_dir / "telemetry_debug.json"
        generator.write_records_enhanced(
            str(json_file),
            100,  # רק 100 records לדיבוגינג
            output_format=OutputFormat.JSON
        )
        
        print(f"   ✓ נוצר: {json_file.name}")
        
        # הצגת דוגמא מהקובץ
        with open(json_file, 'r', encoding='utf-8') as f:
            first_record = json.loads(f.readline())
            sample_data = first_record['data']
            
        print("   📄 דוגמא מהנתונים:")
        print(f"      🌡️ טמפרטורה: {sample_data['cpu_temperature']:.1f}°C")
        print(f"      💻 שימוש CPU: {sample_data['cpu_usage']}%")
        print(f"      🖥️ שם מארח: {sample_data['hostname']}")
        print(f"      ⚠️ רמת התראה: {sample_data['alert_level']}")
        print(f"      ✅ מצב מערכת: {sample_data['system_status']}")
        
        # שלב 6: יצירת קובץ InfluxDB
        print("\n📈 יוצר קובץ InfluxDB Line Protocol...")
        
        influx_file = output_dir / "telemetry_influxdb.txt"
        generator.write_records_enhanced(
            str(influx_file),
            1000,
            output_format=OutputFormat.INFLUX_LINE
        )
        
        print(f"   ✓ נוצר: {influx_file.name}")
        print(f"   💡 לייבא ל-InfluxDB: influx write -b your-bucket -f {influx_file}")
        
        # הצגת דוגמא מ-InfluxDB
        with open(influx_file, 'r') as f:
            sample_line = f.readline().strip()
        print(f"   📄 דוגמא: {sample_line[:80]}...")
        
        # שלב 7: סיכום וסטטיסטיקות
        print("\n📊 סיכום הדמו:")
        
        all_files = list(output_dir.glob("*"))
        total_files = len(all_files)
        total_size_all = sum(f.stat().st_size for f in all_files if f.is_file())
        
        print(f"   📁 נוצרו {total_files} קבצים")
        print(f"   💾 סה\"כ: {total_size_all:,} bytes ({total_size_all/1024/1024:.2f} MB)")
        print(f"   📊 סה\"כ records: ~{total_records + 10000 + 100 + 1000:,}")
        
        # הערכת דרישות אחסון
        estimation = generator.estimate_storage_requirements(
            1000000,  # 1M records
            OutputFormat.BINARY,
            compression_ratio=0.6
        )
        
        print(f"\n🔮 הערכה ל-1M records:")
        print(f"   💾 בינארי: {estimation['compressed_mb']:.1f} MB (דחוס)")
        print(f"   ⚡ זמן צפוי: ~{1000000/10000:.0f} שניות")
        
        print(f"\n✅ הדמו הושלם בהצלחה!")
        print(f"📂 כל הקבצים ב: {output_dir.absolute()}")
        
    except Exception as e:
        print(f"\n❌ שגיאה: {e}")
        raise
    
    finally:
        # ניקוי קובץ schema (אופציונלי)
        if os.path.exists(schema_file):
            # אל תמחק - שמור לשימוש חוזר
            print(f"\n💡 קובץ Schema נשמר: {schema_file}")

if __name__ == "__main__":
    main()

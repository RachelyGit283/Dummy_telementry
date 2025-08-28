#!/usr/bin/env python3
"""
דוגמאות שימוש מעשיות ל-Enhanced TelemetryGeneratorPro
"""
from typing import List, Dict, Any

# from ast import Dict, List
import os
import json
import asyncio
import random
import time
from pathlib import Path

from pyparsing import Any
from generator import EnhancedBitPacker,Enum,EnhancedTelemetryGeneratorPro,GPUAcceleratedGenerator, OutputFormat, RecordType
# from generator import *

def create_sample_schemas():
    """יוצר schemas לדוגמא למקרי שימוש שונים"""
    schemas = {}
    
    # 1. IoT Sensors - חיישנים פשוטים
    schemas["iot_sensors"] = {
        "temperature": {"type": "float", "bits": 32, "min": -50.0, "max": 150.0, "encoding": "ieee754"},
        "humidity": {"type": "float", "bits": 32, "min": 0.0, "max": 100.0},
        "pressure": {"type": "int", "bits": 16, "min": 800, "max": 1200},
        "battery_level": {"type": "int", "bits": 8},
        "sensor_id": {"type": "string", "length": 8, "compressed": True, "char_bits": 6},
        "is_active": {"type": "bool"},
        "last_seen": {"type": "time", "bits": 32, "range": 86400}  # 24 hours
    }
    
    # 2. Server Monitoring - ניטור שרתים
    schemas["server_monitoring"] = {
        "cpu_usage": {"type": "int", "bits": 8},  # 0-100%
        "memory_usage": {"type": "int", "bits": 8},  # 0-100%
        "disk_usage": {"type": "int", "bits": 8},  # 0-100%
        "network_in": {"type": "int", "bits": 32},  # bytes/sec
        "network_out": {"type": "int", "bits": 32},  # bytes/sec
        "active_connections": {"type": "int", "bits": 16},
        "server_status": {"type": "enum", "values": [0, 1, 2, 3, 4]},  # ok, warning, error, critical, unknown
        "hostname": {"type": "string", "length": 16, "compressed": True},
        "uptime": {"type": "time", "bits": 32, "range": 2592000},  # 30 days
        "load_average": {"type": "float", "bits": 32, "min": 0.0, "max": 100.0}
    }
    
    # 3. Financial Trading - מסחר פיננסי
    schemas["trading_data"] = {
        "symbol": {"type": "string", "length": 6, "compressed": True},  # AAPL, MSFT, etc.
        "price": {"type": "float", "bits": 32, "min": 0.01, "max": 10000.0, "encoding": "ieee754"},
        "volume": {"type": "int", "bits": 32},
        "bid_price": {"type": "float", "bits": 32, "encoding": "ieee754"},
        "ask_price": {"type": "float", "bits": 32, "encoding": "ieee754"},
        "market_status": {"type": "enum", "values": [0, 1, 2, 3]},  # pre_market, open, closed, after_hours
        "exchange": {"type": "enum", "values": [0, 1, 2, 3, 4]},  # NYSE, NASDAQ, etc.
        "trade_timestamp": {"type": "time", "bits": 32, "range": 86400}
    }
    
    # 4. Vehicle Telemetry - טלמטריית רכבים
    schemas["vehicle_telemetry"] = {
        "vehicle_id": {"type": "string", "length": 12, "compressed": True},
        "speed": {"type": "int", "bits": 8},  # km/h 0-255
        "engine_temp": {"type": "int", "bits": 8, "min": 60, "max": 120},  # Celsius
        "fuel_level": {"type": "int", "bits": 8},  # 0-100%
        "latitude": {"type": "float", "bits": 32, "encoding": "ieee754"},
        "longitude": {"type": "float", "bits": 32, "encoding": "ieee754"},
        "engine_status": {"type": "enum", "values": [0, 1, 2, 3]},  # off, idle, running, error
        "gear": {"type": "enum", "values": [0, 1, 2, 3, 4, 5, 6, 7]},  # neutral, 1-6, reverse
        "odometer": {"type": "int", "bits": 24},  # km
        "door_status": {"type": "int", "bits": 4}  # bitmask for 4 doors
    }
    
    return schemas

def example_basic_usage():
    """דוגמא בסיסית לשימוש"""
    print("🚀 דוגמא 1: שימוש בסיסי")
    
    # יצירת schema
    schemas = create_sample_schemas()
    schema_file = "iot_sensors_schema.json"
    
    with open(schema_file, 'w') as f:
        json.dump(schemas["iot_sensors"], f, indent=2)
    
    try:
        # יצירת מחולל
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output",
            default_record_type=RecordType.UPDATE
        )
        
        # מידע על הסכמה
        info = generator.get_enhanced_schema_info()
        print(f"📊 {info['data_fields_count']} שדות, {info['bytes_per_record']} bytes לרשומה")
        
        # יצירת קובץ יחיד
        print("📝 יוצר קובץ JSON לדיבוגינג...")
        generator.write_records_enhanced(
            "./output/iot_debug.json",
            100,  # 100 records
            output_format=OutputFormat.JSON
        )
        
        # יצירת קובץ בינארי לפרודקציה
        print("⚡ יוצר קובץ בינארי מהיר...")
        generator.write_records_enhanced(
            "./output/iot_production.bin",
            10000,  # 10K records
            output_format=OutputFormat.BINARY
        )
        
        print("✅ הסתיים בהצלחה!")
        
    finally:
        # ניקוי
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_multiple_formats():
    """דוגמא ליצירת מספר פורמטים"""
    print("\n🎯 דוגמא 2: פורמטים מרובים")
    
    schemas = create_sample_schemas()
    schema_file = "server_monitoring_schema.json"
    
    with open(schema_file, 'w') as f:
        json.dump(schemas["server_monitoring"], f, indent=2)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/monitoring"
        )
        
        formats_config = [
            {
                "format": OutputFormat.BINARY,
                "records": 50000,
                "prefix": "monitoring_binary",
                "description": "הפרודקציה המהירה"
            },
            {
                "format": OutputFormat.INFLUX_LINE,
                "records": 10000,
                "prefix": "monitoring_influx",
                "description": "יבוא ל-InfluxDB"
            },
            {
                "format": OutputFormat.JSON,
                "records": 1000,
                "prefix": "monitoring_debug",
                "description": "דיבוגינג ופיתוח"
            }
        ]
        
        for config in formats_config:
            print(f"📄 יוצר {config['description']} - {config['records']:,} records...")
            
            # הערכת גודל
            storage = generator.estimate_storage_requirements(
                config["records"], 
                config["format"],
                compression_ratio=0.65
            )
            
            print(f"   💾 גודל צפוי: {storage['compressed_mb']:.1f} MB")
            
            # יצירה בפועל
            start_time = time.time()
            
            paths = generator.generate_multiple_files_enhanced(
                num_files=3,
                records_per_file=config["records"] // 3,
                file_prefix=config["prefix"],
                output_format=config["format"],
                max_workers=4,
                record_type_ratio={
                    RecordType.UPDATE: 0.8,  # 80% updates
                    RecordType.EVENT: 0.2    # 20% events
                }
            )
            
            elapsed = time.time() - start_time
            total_size = sum(os.path.getsize(path) for path in paths)
            actual_mb = total_size / (1024 * 1024)
            
            print(f"   ✅ {len(paths)} קבצים, {actual_mb:.1f} MB, {elapsed:.1f}s")
            print(f"   ⚡ {config['records']/elapsed:.0f} records/sec")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_high_performance():
    """דוגמא לביצועים גבוהים עם GPU"""
    print("\n⚡ דוגמא 3: ביצועים גבוהים")
    
    schemas = create_sample_schemas()
    schema_file = "trading_schema.json"
    
    with open(schema_file, 'w') as f:
        json.dump(schemas["trading_data"], f, indent=2)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/trading",
            enable_gpu=True  # ינסה להשתמש בGPU
        )
        
        # בדיקת יכולות GPU
        info = generator.get_enhanced_schema_info()
        print(f"🔧 GPU זמין: {info['supports_gpu']}")
        
        # benchmark
        print("🏃 מריץ benchmark...")
        results = generator.benchmark_generation_speed(
            test_records=10000,
            use_gpu=True,
            batch_size=2000
        )
        
        for metric, value in results.items():
            print(f"   {metric}: {value:.1f}")
        
        # יצירה עם batches גדולים
        print("🚀 יוצר נתונים בvolume גבוה...")
        
        start_time = time.time()
        paths = generator.generate_multiple_files_enhanced(
            num_files=10,  # 10 קבצים
            records_per_file=100000,  # 100K records כל אחד = 1M total
            file_prefix="trading_high_volume",
            output_format=OutputFormat.BINARY,
            max_workers=8,
            use_gpu_batches=True,
            batch_size=5000,
            record_type_ratio={
                RecordType.UPDATE: 0.9,  # רוב עדכוני מחירים
                RecordType.EVENT: 0.1    # מעט אירועי מסחר מיוחדים
            }
        )
        elapsed = time.time() - start_time
        
        total_records = 10 * 100000
        total_size = sum(os.path.getsize(path) for path in paths)
        
        print(f"✅ יצר {total_records:,} רשומות במהירות!")
        print(f"   📁 {len(paths)} קבצים")
        print(f"   💾 {total_size / (1024**3):.2f} GB")
        print(f"   ⏱️ {elapsed:.1f} שניות")
        print(f"   ⚡ {total_records/elapsed:,.0f} records/sec")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

async def example_async_pipeline():
    """דוגמא ל-pipeline אסינכרוני"""
    print("\n🔄 דוגמא 4: Pipeline אסינכרוני")
    
    schemas = create_sample_schemas()
    schema_file = "vehicle_schema.json"
    
    with open(schema_file, 'w') as f:
        json.dump(schemas["vehicle_telemetry"], f, indent=2)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/vehicles"
        )
        
        print("🚗 יוצר נתוני רכבים במקביל...")
        
        # יצירה אסינכרונית של מספר datasets
        tasks = [
            generator.generate_multiple_files_enhanced_async(
                num_files=5,
                records_per_file=20000,
                file_prefix="fleet_morning",
                output_format=OutputFormat.INFLUX_LINE,
                record_type_ratio={RecordType.UPDATE: 0.95, RecordType.EVENT: 0.05}
            ),
            generator.generate_multiple_files_enhanced_async(
                num_files=5,
                records_per_file=20000,
                file_prefix="fleet_evening",
                output_format=OutputFormat.BINARY,
                record_type_ratio={RecordType.UPDATE: 0.85, RecordType.EVENT: 0.15}
            )
        ]
        
        # המתנה לכל המשימות
        start_time = time.time()
        results = await asyncio.gather(*tasks)
        elapsed = time.time() - start_time
        
        total_files = sum(len(paths) for paths in results)
        total_records = 5 * 20000 * 2  # 2 datasets
        
        print(f"✅ Pipeline אסינכרוני הושלם!")
        print(f"   📁 {total_files} קבצים")
        print(f"   📊 {total_records:,} רשומות")
        print(f"   ⏱️ {elapsed:.1f} שניות")
        print(f"   ⚡ {total_records/elapsed:,.0f} records/sec")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_influxdb_integration():
    """דוגמא לאינטגרציה עם InfluxDB"""
    print("\n📈 דוגמא 5: אינטגרציה עם InfluxDB")
    
    schemas = create_sample_schemas()
    schema_file = "mixed_telemetry_schema.json"
    
    # schema מעורבת לדוגמא
    mixed_schema = {
        **schemas["iot_sensors"],
        **{k: v for k, v in schemas["server_monitoring"].items() 
           if k not in schemas["iot_sensors"]}
    }
    
    with open(schema_file, 'w') as f:
        json.dump(mixed_schema, f, indent=2)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/influxdb_import"
        )
        
        # יצירת נתונים בפורמט InfluxDB
        print("📊 יוצר נתונים לimport ל-InfluxDB...")
        
        influx_paths = generator.generate_multiple_files_enhanced(
            num_files=3,
            records_per_file=5000,
            file_prefix="influx_data",
            output_format=OutputFormat.INFLUX_LINE,
            record_type_ratio={
                RecordType.UPDATE: 0.7,
                RecordType.EVENT: 0.3
            }
        )
        
        print(f"✅ יצר {len(influx_paths)} קבצים לInfluxDB")
        
        # הצגת דוגמא של Line Protocol
        print("\n📄 דוגמא מקובץ InfluxDB:")
        with open(influx_paths[0], 'r') as f:
            for i, line in enumerate(f):
                if i < 3:  # הצג 3 שורות ראשונות
                    print(f"   {line.strip()}")
                else:
                    break
        
        # הוראות import
        print(f"\n💡 לייבא ל-InfluxDB:")
        print(f"   influx write -b your-bucket -o your-org -f {influx_paths[0]}")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_performance_analysis():
    """דוגמא לניתוח ביצועים מפורט"""
    print("\n📊 דוגמא 6: ניתוח ביצועים")
    
    schemas = create_sample_schemas()
    test_configs = [
        ("iot_sensors", "IoT Sensors", 1000),
        ("server_monitoring", "Server Monitoring", 2000),
        ("trading_data", "Trading Data", 5000)
    ]
    
    results = {}
    
    for schema_name, display_name, num_records in test_configs:
        print(f"\n🔬 בודק {display_name}...")
        
        schema_file = f"{schema_name}_perf.json"
        with open(schema_file, 'w') as f:
            json.dump(schemas[schema_name], f)
        
        try:
            generator = EnhancedTelemetryGeneratorPro(
                schema_file,
                output_dir="./output/performance"
            )
            
            # מידע על סכמה
            info = generator.get_enhanced_schema_info()
            
            # benchmark
            bench_results = generator.benchmark_generation_speed(
                test_records=num_records,
                use_gpu=True,
                batch_size=min(500, num_records // 4)
            )
            
            # הערכת אחסון
            storage_binary = generator.estimate_storage_requirements(
                num_records, OutputFormat.BINARY, compression_ratio=0.6
            )
            storage_json = generator.estimate_storage_requirements(
                num_records, OutputFormat.JSON, compression_ratio=0.4
            )
            
            results[schema_name] = {
                "fields": info["data_fields_count"],
                "bytes_per_record": info["bytes_per_record"],
                "records_per_sec": bench_results["regular_records_per_sec"],
                "binary_mb": storage_binary["compressed_mb"],
                "json_mb": storage_json["compressed_mb"],
                "compression_ratio": storage_json["compressed_mb"] / storage_binary["compressed_mb"]
            }
            
            print(f"   📏 {info['data_fields_count']} שדות, {info['bytes_per_record']} bytes/record")
            print(f"   ⚡ {bench_results['regular_records_per_sec']:,.0f} records/sec")
            print(f"   💾 Binary: {storage_binary['compressed_mb']:.1f} MB")
            print(f"   📄 JSON: {storage_json['compressed_mb']:.1f} MB (פי {storage_json['compressed_mb']/storage_binary['compressed_mb']:.1f})")
            
        finally:
            if os.path.exists(schema_file):
                os.remove(schema_file)
    
    # סיכום השוואתי
    print(f"\n📋 סיכום ביצועים:")
    print(f"{'Schema':<20} {'Fields':<8} {'Rec/Sec':<10} {'Binary MB':<12} {'JSON Ratio':<12}")
    print("-" * 70)
    for name, data in results.items():
        print(f"{name:<20} {data['fields']:<8} {data['records_per_sec']:<10.0f} "
              f"{data['binary_mb']:<12.1f} {data['compression_ratio']:<12.1f}")

def example_production_simulation():
    """דוגמא לסימולציית פרודקציה מלאה"""
    print("\n🏭 דוגמא 7: סימולציית פרודקציה")
    
    schemas = create_sample_schemas()
    
    # תרחיש: חברת logistics עם צי רכבים
    scenario_config = {
        "num_vehicles": 1000,
        "records_per_vehicle_per_hour": 360,  # כל 10 שניות
        "simulation_hours": 24,
        "files_per_hour": 12,  # כל 5 דקות קובץ חדש
        "output_formats": [OutputFormat.BINARY, OutputFormat.INFLUX_LINE]
    }
    
    total_records = (scenario_config["num_vehicles"] * 
                    scenario_config["records_per_vehicle_per_hour"] * 
                    scenario_config["simulation_hours"])
    
    print(f"🚛 מדמה {scenario_config['num_vehicles']} רכבים למשך {scenario_config['simulation_hours']} שעות")
    print(f"📊 סה״כ {total_records:,} רשומות צפויות")
    
    schema_file = "production_vehicle_schema.json"
    with open(schema_file, 'w') as f:
        json.dump(schemas["vehicle_telemetry"], f)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/production_simulation",
            enable_gpu=True
        )
        
        # הערכת דרישות
        estimation = generator.estimate_storage_requirements(
            total_records,
            OutputFormat.BINARY,
            compression_ratio=0.5  # דחיסה חזקה לפרודקציה
        )
        
        print(f"💾 צפוי: {estimation['compressed_gb']:.1f} GB אחרי דחיסה")
        
        # יצירת הנתונים בשני פורמטים
        all_paths = []
        
        for output_format in scenario_config["output_formats"]:
            print(f"\n📝 יוצר בפורמט {output_format.value}...")
            
            records_per_file = total_records // scenario_config["files_per_hour"] // scenario_config["simulation_hours"]
            total_files = scenario_config["files_per_hour"] * scenario_config["simulation_hours"]
            
            start_time = time.time()
            
            paths = generator.generate_multiple_files_enhanced(
                num_files=total_files,
                records_per_file=records_per_file,
                file_prefix=f"logistics_{output_format.value}",
                output_format=output_format,
                max_workers=8,
                use_gpu_batches=True,
                batch_size=10000,
                record_type_ratio={
                    RecordType.UPDATE: 0.85,  # מיקום/מהירות רגילים
                    RecordType.EVENT: 0.15    # עצירות, תדלוק, אירועים
                }
            )
            
            elapsed = time.time() - start_time
            total_size = sum(os.path.getsize(path) for path in paths)
            
            print(f"   ✅ {len(paths)} קבצים, {total_size/(1024**3):.2f} GB")
            print(f"   ⏱️ {elapsed:.1f} שניות")
            print(f"   ⚡ {total_records/elapsed:,.0f} records/sec")
            
            all_paths.extend(paths)
        
        print(f"\n🎯 סימולציית פרודקציה הושלמה!")
        print(f"   📁 סה״כ {len(all_paths)} קבצים")
        print(f"   📊 {total_records:,} רשומות")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_custom_schema_creation():
    """דוגמא ליצירת schema מותאם אישית"""
    print("\n🛠️ דוגמא 8: יצירת Schema מותאם")
    
    # פונקציה ליצירת schema דינאמית
    def create_custom_schema(device_types: List[str], metrics_per_device: int) -> Dict[str, Any]:
        schema = {}
        
        # שדות בסיסיים
        schema["device_id"] = {"type": "string", "length": 12, "compressed": True}
        schema["timestamp"] = {"type": "time", "bits": 32, "range": 86400}
        schema["device_type"] = {"type": "enum", "values": list(range(len(device_types)))}
        
        # מטריקות דינאמיות
        for i in range(metrics_per_device):
            schema[f"metric_{i}"] = {
                "type": "float" if i % 2 == 0 else "int",
                "bits": 32 if i % 2 == 0 else random.choice([8, 16, 24]),
                "min": 0.0 if i % 2 == 0 else 0,
                "max": 100.0 if i % 2 == 0 else (1 << 16) - 1
            }
        
        # מטא-דטה
        schema["quality_score"] = {"type": "int", "bits": 4}  # 0-15
        schema["is_validated"] = {"type": "bool"}
        schema["error_flags"] = {"type": "int", "bits": 8}  # bitmask
        
        return schema
    
    # יצירת schema מותאמת
    device_types = ["sensor", "gateway", "controller", "actuator"]
    custom_schema = create_custom_schema(device_types, 8)
    
    schema_file = "custom_schema.json"
    with open(schema_file, 'w') as f:
        json.dump(custom_schema, f, indent=2)
    
    print(f"🔧 יצר schema עם {len(custom_schema)} שדות")
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/custom"
        )
        
        # ניתוח הsoma שנוצרה
        info = generator.get_enhanced_schema_info()
        print(f"📊 Schema analysis:")
        print(f"   🔢 {info['data_fields_count']} data fields")
        print(f"   💾 {info['bytes_per_record']} bytes per record")
        print(f"   🔧 {info['overhead_bits']} overhead bits")
        
        # יצירת דוגמאות בכל הפורמטים
        for format_type in OutputFormat:
            format_name = format_type.value
            extension = {"binary": ".bin", "json": ".json", "influx_line": ".txt"}[format_name]
            
            print(f"\n📝 יוצר דוגמא ב{format_name}...")
            
            paths = generator.generate_multiple_files_enhanced(
                num_files=2,
                records_per_file=1000,
                file_prefix=f"custom_{format_name}",
                output_format=format_type
            )
            
            total_size = sum(os.path.getsize(path) for path in paths)
            print(f"   ✅ {len(paths)} קבצים, {total_size/1024:.1f} KB")
            
            # הצגת דוגמא מהקובץ הראשון (לא בינארי)
            if format_type != OutputFormat.BINARY:
                with open(paths[0], 'r') as f:
                    first_line = f.readline().strip()
                    print(f"   📄 דוגמא: {first_line[:100]}...")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def example_monitoring_and_validation():
    """דוגמא לניטור ווולידציה"""
    print("\n🔍 דוגמא 9: ניטור ווולידציה")
    
    # יצירת schema עם וולידציות
    validation_schema = {
        "cpu_temp": {"type": "float", "bits": 32, "min": 20.0, "max": 90.0},
        "fan_speed": {"type": "int", "bits": 12, "min": 0, "max": 4000},  # RPM
        "voltage": {"type": "float", "bits": 32, "min": 11.5, "max": 12.5},
        "current": {"type": "float", "bits": 32, "min": 0.0, "max": 20.0},
        "power_state": {"type": "enum", "values": [0, 1, 2, 3, 4]},
        "alert_level": {"type": "enum", "values": [0, 1, 2, 3]},  # none, info, warning, critical
        "node_id": {"type": "string", "length": 6, "compressed": True}
    }
    
    schema_file = "validation_schema.json"
    with open(schema_file, 'w') as f:
        json.dump(validation_schema, f, indent=2)
    
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file,
            output_dir="./output/monitoring"
        )
        
        print("🔍 יוצר נתונים עם ניטור...")
        
        def progress_monitor(current, total):
            """Callback לניטור התקדמות"""
            percent = (current / total) * 100
            if percent % 20 == 0:  # כל 20%
                print(f"   📈 התקדמות: {percent:.0f}% ({current:,}/{total:,} records)")
        
        # יצירה עם ניטור התקדמות
        start_time = time.time()
        
        paths = generator.generate_multiple_files_enhanced(
            num_files=4,
            records_per_file=25000,  # 100K total
            file_prefix="monitored_data",
            output_format=OutputFormat.BINARY,
            max_workers=4
        )
        
        elapsed = time.time() - start_time
        
        print(f"✅ ניטור הושלם!")
        print(f"   📁 {len(paths)} קבצים")
        print(f"   ⏱️ {elapsed:.1f} שניות")
        
        # בדיקת שלמות הנתונים
        print("\n🔍 בודק שלמות נתונים...")
        
        total_size = 0
        for i, path in enumerate(paths):
            file_size = os.path.getsize(path)
            total_size += file_size
            print(f"   📄 קובץ {i}: {file_size:,} bytes")
        
        expected_size = 100000 * generator.get_enhanced_schema_info()["bytes_per_record"]
        size_ratio = total_size / expected_size
        
        print(f"   📊 סה״כ: {total_size:,} bytes")
        print(f"   🎯 יחס לצפוי: {size_ratio:.2f} ({'✅ תקין' if 0.95 <= size_ratio <= 1.05 else '⚠️ חריג'})")
        
    finally:
        if os.path.exists(schema_file):
            os.remove(schema_file)

def main():
    os.makedirs('./output', exist_ok=True)
    os.makedirs('./output/monitoring', exist_ok=True)
    os.makedirs('./output/influxdb_import', exist_ok=True)
    os.makedirs('./output/trading', exist_ok=True)
    os.makedirs('./output/vehicles', exist_ok=True)
    os.makedirs('./output/performance', exist_ok=True)
    os.makedirs('./output/custom', exist_ok=True)

    """מריץ את כל הדוגמאות"""
    print("🎬 מתחיל דוגמאות שימוש מעשיות...")
    print("=" * 70)
    
    # יצירת תיקיית output
    os.makedirs("./output", exist_ok=True)
    
    try:
        # דוגמאות sync
        example_basic_usage()
        example_multiple_formats() 
        example_high_performance()
        example_influxdb_integration()
        example_performance_analysis()
        example_custom_schema_creation()
        example_monitoring_and_validation()
        
        # דוגמא async
        print("\n🔄 מריץ דוגמא אסינכרונית...")
        asyncio.run(example_async_pipeline())
        
        print("\n" + "=" * 70)
        print("🎉 כל הדוגמאות הושלמו בהצלחה!")
        print("💡 הקוד מוכן לשימוש בפרודקציה!")
        
        # הצגת סיכום קבצים שנוצרו
        if os.path.exists("./output"):
            total_files = sum(len(files) for _, _, files in os.walk("./output"))
            total_size = sum(
                os.path.getsize(os.path.join(root, file))
                for root, _, files in os.walk("./output")
                for file in files
            )
            
            print(f"\n📁 נוצרו {total_files} קבצים")
            print(f"💾 סה״כ גודל: {total_size/(1024**2):.1f} MB")
    
    except Exception as e:
        print(f"\n❌ שגיאה: {e}")
        raise


if __name__ == "__main__":
    main()
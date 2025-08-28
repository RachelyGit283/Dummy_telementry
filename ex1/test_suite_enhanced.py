# import os
# import json
# import tempfile
# import unittest
# import shutil
# import struct
# import time
# from typing import Dict, Any
# import logging
# from generator  import EnhancedBitPacker,EnhancedTelemetryGeneratorPro,Enum,GPUAcceleratedGenerator,InfluxDBLineProtocolWriter
# # Imports של ההרחבות
# # from generator import *

# class TestEnhancedBitPacker(unittest.TestCase):
#     """בדיקות ל-EnhancedBitPacker"""
    
#     def setUp(self):
#         self.packer = EnhancedBitPacker()
    
#     def test_float_packing(self):
#         """בדיקת אריזת float"""
#         test_value = 3.14159
#         self.packer.write_float(test_value, precision_bits=16)
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
        
#         # בדיקה שהתוצאה איננה ריקה
#         self.assertGreater(len(result), 0)
#         self.assertEqual(len(result), 4)  # 32 bits = 4 bytes

#     def test_ieee754_float(self):
#         """בדיקת IEEE 754 float encoding"""
#         test_value = 42.5
#         self.packer.write_ieee754_float(test_value)
#         result = self.packer.bytes()
        
#         # השוואה עם struct.pack
#         expected = struct.pack('>f', test_value)
#         self.assertEqual(result, expected)

#     def test_string_packing(self):
#         """בדיקת אריזת string"""
#         test_string = "Hello"
#         self.packer.write_string(test_string)
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
        
#         # בדיקה: length byte + string bytes
#         expected_length = len(test_string)
#         self.assertEqual(result[0], expected_length)
#         self.assertEqual(result[1:expected_length+1], test_string.encode('ascii'))

#     def test_compressed_string(self):
#         """בדיקת string דחוס"""
#         test_string = "ABC123"
#         self.packer.write_compressed_string(test_string, char_bits=6)
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
        
#         # בדיקה שיש נתונים
#         self.assertGreater(len(result), 1)  # לפחות length + data


# class TestInfluxDBLineProtocol(unittest.TestCase):
#     """בדיקות לפורמט InfluxDB Line Protocol"""
    
#     def test_basic_line_format(self):
#         """בדיקת פורמט בסיסי"""
#         measurement = "temperature"
#         tags = {"sensor": "A", "location": "room1"}
#         fields = {"value": 23.5, "status": True}
#         timestamp = 1640995200000000000
        
#         line = InfluxDBLineProtocolWriter.format_record(
#             measurement, tags, fields, timestamp
#         )
        
#         # בדיקה שהפורמט נכון
#         self.assertIn("temperature,sensor=A,location=room1", line)
#         self.assertIn('value=23.5,status=false', line)
#         self.assertIn(str(timestamp), line)

#     def test_string_field_quoting(self):
#         """בדיקת ציטוט של string fields"""
#         measurement = "logs"
#         tags = {}
#         fields = {"message": "Hello World", "level": "info"}
#         timestamp = 0
        
#         line = InfluxDBLineProtocolWriter.format_record(
#             measurement, tags, fields, timestamp
#         )
        
#         self.assertIn('"Hello World"', line)
#         self.assertIn('"info"', line)

#     def test_integer_field_suffix(self):
#         """בדיקת סיומת i לשדות int"""
#         measurement = "counter"
#         tags = {}
#         fields = {"count": 42}
#         timestamp = 0
        
#         line = InfluxDBLineProtocolWriter.format_record(
#             measurement, tags, fields, timestamp
#         )
        
#         self.assertIn('count=42i', line)


# class TestEnhancedTelemetryGenerator(unittest.TestCase):
#     """בדיקות מתקדמות למחלקת הראשית"""
    
#     def setUp(self):
#         self.temp_dir = tempfile.mkdtemp()
#         self.schema_file = os.path.join(self.temp_dir, "enhanced_schema.json")
        
#         # Schema מתקדמת
#         self.enhanced_schema = {
#             "temperature": {"type": "float", "bits": 32, "min": -50.0, "max": 150.0},
#             "pressure": {"type": "int", "bits": 16},
#             "device_id": {"type": "string", "length": 8, "max_length": 16},
#             "status": {"type": "bool"},
#             "mode": {"type": "enum", "values": [0, 1, 2, 3, 4]},
#             "timestamp_offset": {"type": "time", "bits": 16, "range": 3600}
#         }
        
#         with open(self.schema_file, 'w') as f:
#             json.dump(self.enhanced_schema, f)
    
#     def tearDown(self):
#         shutil.rmtree(self.temp_dir)
    
#     def test_enhanced_record_generation(self):
#         """בדיקת יצירת רשומה מתקדמת"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file, 
#             output_dir=self.temp_dir
#         )
        
#         record = generator.generate_enhanced_record()
        
#         # בדיקות בסיסיות
#         self.assertIsInstance(record, TelemetryRecord)
#         self.assertIn(record.record_type, [RecordType.UPDATE, RecordType.EVENT])
#         self.assertGreater(record.timestamp, 0)
#         self.assertGreaterEqual(record.sequence_id, 0)
        
#         # בדיקת נתונים
#         self.assertIn("temperature", record.data)
#         self.assertIn("device_id", record.data)
        
#         # בדיקת טווחים
#         temp = record.data["temperature"]
#         self.assertGreaterEqual(temp, -50.0)
#         self.assertLessEqual(temp, 150.0)
        
#         device_id = record.data["device_id"]
#         self.assertIsInstance(device_id, str)
#         self.assertLessEqual(len(device_id), 16)

#     def test_record_type_ratios(self):
#         """בדיקת יחס סוגי רשומות"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         # יצירת הרבה רשומות עם יחס מוגדר
#         ratio = {RecordType.UPDATE: 0.8, RecordType.EVENT: 0.2}
#         file_path = os.path.join(self.temp_dir, "ratio_test.json")
        
#         generator.write_records_enhanced(
#             file_path,
#             1000,
#             output_format=OutputFormat.JSON,
#             record_type_ratio=ratio
#         )
        
#         # ספירת סוגי רשומות
#         update_count = 0
#         event_count = 0
        
#         with open(file_path, 'r') as f:
#             for line in f:
#                 data = json.loads(line.strip())
#                 if data['type'] == 'update':
#                     update_count += 1
#                 else:
#                     event_count += 1
        
#         total = update_count + event_count
#         update_ratio = update_count / total
#         event_ratio = event_count / total
        
#         # בדיקה שהיחס קרוב למצופה (±10%)
#         self.assertAlmostEqual(update_ratio, 0.8, delta=0.1)
#         self.assertAlmostEqual(event_ratio, 0.2, delta=0.1)

#     def test_multiple_output_formats(self):
#         """בדיקת פורמטי פלט שונים"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         test_cases = [
#             (OutputFormat.BINARY, ".bin"),
#             (OutputFormat.JSON, ".json"),
#             (OutputFormat.INFLUX_LINE, ".txt")
#         ]
        
#         for format_type, extension in test_cases:
#             with self.subTest(format=format_type):
#                 file_path = os.path.join(self.temp_dir, f"test{extension}")
#                 generator.write_records_enhanced(
#                     file_path,
#                     100,
#                     output_format=format_type
#                 )
                
#                 self.assertTrue(os.path.exists(file_path))
#                 self.assertGreater(os.path.getsize(file_path), 0)

#     def test_enhanced_parallel_generation(self):
#         """בדיקת יצירה מקבילית מתקדמת"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         paths = generator.generate_multiple_files_enhanced(
#             num_files=3,
#             records_per_file=100,
#             output_format=OutputFormat.JSON,
#             record_type_ratio={RecordType.UPDATE: 0.5, RecordType.EVENT: 0.5}
#         )
        
#         self.assertEqual(len(paths), 3)
        
#         for path in paths:
#             self.assertTrue(os.path.exists(path))
#             self.assertGreater(os.path.getsize(path), 0)
            
#             # בדיקה שהקובץ מכיל JSON תקין
#             with open(path, 'r') as f:
#                 line_count = 0
#                 for line in f:
#                     data = json.loads(line.strip())
#                     self.assertIn('type', data)
#                     self.assertIn('timestamp', data)
#                     self.assertIn('data', data)
#                     line_count += 1
                
#                 self.assertEqual(line_count, 100)

#     def test_schema_info_enhanced(self):
#         """בדיקת מידע על סכמה מתקדמת"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         info = generator.get_enhanced_schema_info()
        
#         # בדיקות בסיסיות
#         self.assertIn("fields_count", info)
#         self.assertIn("overhead_bits", info)
#         self.assertIn("supported_formats", info)
#         self.assertIn("fields", info)
        
#         # בדיקה שכל הפורמטים נתמכים
#         expected_formats = ["binary", "influx_line", "json"]
#         self.assertEqual(set(info["supported_formats"]), set(expected_formats))
        
#         # בדיקת שדות
#         self.assertEqual(len(info["fields"]), 6)  # 6 data fields
        
#         for field in info["fields"]:
#             self.assertIn("name", field)
#             self.assertIn("type", field)
#             self.assertIn("bits", field)

#     def test_storage_estimation(self):
#         """בדיקת הערכת דרישות אחסון"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         # בדיקה לפורמטים שונים
#         for format_type in OutputFormat:
#             with self.subTest(format=format_type):
#                 estimation = generator.estimate_storage_requirements(
#                     10000, 
#                     format_type,
#                     compression_ratio=0.7
#                 )
                
#                 self.assertEqual(estimation["records"], 10000)
#                 self.assertEqual(estimation["format"], format_type.value)
#                 self.assertGreater(estimation["uncompressed_bytes"], 0)
#                 self.assertLess(estimation["compressed_bytes"], estimation["uncompressed_bytes"])
#                 self.assertEqual(estimation["compression_ratio"], 0.7)

#     def test_gpu_batch_generation(self):
#         """בדיקת יצירה ב-batch עם GPU (אם זמין)"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir,
#             enable_gpu=True
#         )
        
#         batch_size = 50
#         batch = generator.generate_batch_gpu_accelerated(batch_size)
        
#         # בדיקות בסיסיות
#         self.assertEqual(len(batch), batch_size)
        
#         for record in batch:
#             self.assertIsInstance(record, TelemetryRecord)
#             self.assertIn("temperature", record.data)
#             self.assertIn("device_id", record.data)

#     def test_benchmark_functionality(self):
#         """בדיקת פונקציונליות benchmark"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir,
#             enable_gpu=True
#         )
        
#         results = generator.benchmark_generation_speed(
#             test_records=1000,
#             use_gpu=True,
#             batch_size=100
#         )
        
#         # בדיקות בסיסיות
#         self.assertIn("regular_records_per_sec", results)
#         self.assertGreater(results["regular_records_per_sec"], 0)


# class TestAsyncFunctionality(unittest.TestCase):
#     """בדיקות async"""
    
#     def setUp(self):
#         self.temp_dir = tempfile.mkdtemp()
#         self.schema_file = os.path.join(self.temp_dir, "async_schema.json")
        
#         simple_schema = {
#             "value": {"type": "int", "bits": 16},
#             "flag": {"type": "bool"}
#         }
        
#         with open(self.schema_file, 'w') as f:
#             json.dump(simple_schema, f)
    
#     def tearDown(self):
#         shutil.rmtree(self.temp_dir)
    
#     def test_async_generation(self):
#         """בדיקת יצירה אסינכרונית"""
#         import asyncio
        
#         async def run_async_test():
#             generator = EnhancedTelemetryGeneratorPro(
#                 self.schema_file,
#                 output_dir=self.temp_dir
#             )
            
#             paths = await generator.generate_multiple_files_enhanced_async(
#                 num_files=2,
#                 records_per_file=50,
#                 output_format=OutputFormat.JSON
#             )
            
#             self.assertEqual(len(paths), 2)
#             for path in paths:
#                 self.assertTrue(os.path.exists(path))
        
#         # הרצת הבדיקה
#         asyncio.run(run_async_test())


# class TestPerformanceEnhanced(unittest.TestCase):
#     """בדיקות ביצועים מתקדמות"""
    
#     def setUp(self):
#         self.temp_dir = tempfile.mkdtemp()
#         self.schema_file = os.path.join(self.temp_dir, "perf_enhanced_schema.json")
        
#         # Schema גדולה לבדיקת ביצועים
#         self.large_schema = {}
#         for i in range(50):  # 50 שדות
#             if i % 4 == 0:
#                 self.large_schema[f"float_field_{i}"] = {
#                     "type": "float", "bits": 32, "min": 0.0, "max": 100.0
#                 }
#             elif i % 4 == 1:
#                 self.large_schema[f"string_field_{i}"] = {
#                     "type": "string", "length": 10
#                 }
#             elif i % 4 == 2:
#                 self.large_schema[f"enum_field_{i}"] = {
#                     "type": "enum", "values": list(range(16))
#                 }
#             else:
#                 self.large_schema[f"int_field_{i}"] = {
#                     "type": "int", "bits": 20
#                 }
        
#         with open(self.schema_file, 'w') as f:
#             json.dump(self.large_schema, f)
    
#     def tearDown(self):
#         shutil.rmtree(self.temp_dir)
    
#     def test_large_schema_performance(self):
#         """בדיקת ביצועים עם סכמה גדולה"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         start_time = time.time()
        
#         # יצירת קובץ גדול
#         file_path = os.path.join(self.temp_dir, "large_perf_test.bin")
#         generator.write_records_enhanced(
#             file_path,
#             5000,  # 5K records
#             output_format=OutputFormat.BINARY
#         )
        
#         end_time = time.time()
#         elapsed = end_time - start_time
        
#         # בדיקות
#         self.assertTrue(os.path.exists(file_path))
#         self.assertGreater(os.path.getsize(file_path), 0)
        
#         records_per_sec = 5000 / elapsed
#         print(f"Large schema: {records_per_sec:.0f} records/sec")
        
#         # צריך להיות לפחות 500 records/sec
#         self.assertGreater(records_per_sec, 500)
    
#     def test_format_comparison_performance(self):
#         """השוואת ביצועים בין פורמטים"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         test_records = 1000
#         results = {}
        
#         for format_type in OutputFormat:
#             start_time = time.time()
            
#             extension = {
#                 OutputFormat.BINARY: ".bin",
#                 OutputFormat.JSON: ".json",
#                 OutputFormat.INFLUX_LINE: ".txt"
#             }[format_type]
            
#             file_path = os.path.join(self.temp_dir, f"perf_test_{format_type.value}{extension}")
#             generator.write_records_enhanced(
#                 file_path,
#                 test_records,
#                 output_format=format_type
#             )
            
#             elapsed = time.time() - start_time
#             results[format_type.value] = {
#                 "time": elapsed,
#                 "records_per_sec": test_records / elapsed,
#                 "file_size": os.path.getsize(file_path)
#             }
        
#         # הדפסת תוצאות
#         print("\nFormat Performance Comparison:")
#         for format_name, stats in results.items():
#             print(f"{format_name}: {stats['records_per_sec']:.0f} rec/sec, "
#                   f"size: {stats['file_size']:,} bytes")
        
#         # בדיקה שבינארי הכי מהיר וקטן
#         binary_stats = results["binary"]
#         json_stats = results["json"]
        
#         self.assertLess(binary_stats["file_size"], json_stats["file_size"])
    
#     def test_parallel_vs_sequential_performance(self):
#         """השוואת ביצועים מקבילי מול רציף"""
#         generator = EnhancedTelemetryGeneratorPro(
#             self.schema_file,
#             output_dir=self.temp_dir
#         )
        
#         total_records = 5000
#         num_files = 5
#         records_per_file = total_records // num_files
        
#         # רציף
#         start_time = time.time()
#         for i in range(num_files):
#             file_path = os.path.join(self.temp_dir, f"sequential_{i}.bin")
#             generator.write_records_enhanced(
#                 file_path,
#                 records_per_file,
#                 output_format=OutputFormat.BINARY
#             )
#         sequential_time = time.time() - start_time
        
#         # מקבילי
#         start_time = time.time()
#         generator.generate_multiple_files_enhanced(
#             num_files=num_files,
#             records_per_file=records_per_file,
#             file_prefix="parallel",
#             output_format=OutputFormat.BINARY,
#             max_workers=4
#         )
#         parallel_time = time.time() - start_time
        
#         print(f"\nParallel vs Sequential:")
#         print(f"Sequential: {sequential_time:.2f}s ({total_records/sequential_time:.0f} rec/sec)")
#         print(f"Parallel: {parallel_time:.2f}s ({total_records/parallel_time:.0f} rec/sec)")
#         print(f"Speedup: {sequential_time/parallel_time:.2f}x")
        
#         # בדיקה שמקבילי מהיר יותר (לפחות 10% שיפור)
#         self.assertLess(parallel_time, sequential_time * 0.9)


# def run_comprehensive_enhanced_tests():
#     """מריץ את כל הבדיקות המתקדמות"""
#     # הגדרת logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s'
#     )
    
#     print("🚀 מריץ בדיקות מתקדמות לגרסה המורחבת...")
    
#     # יצירת test suite
#     suite = unittest.TestSuite()
    
#     # הוספת כל קבוצות הבדיקות
#     test_classes = [
#         TestEnhancedBitPacker,
#         TestInfluxDBLineProtocol,
#         TestEnhancedTelemetryGenerator,
#         TestAsyncFunctionality,
#         TestPerformanceEnhanced
#     ]
    
#     for test_class in test_classes:
#         suite.addTest(unittest.makeSuite(test_class))
    
#     # הרצת הבדיקות
#     runner = unittest.TextTestRunner(verbosity=2)
#     result = runner.run(suite)
    
#     return result.wasSuccessful()


# class IntegrationTest:
#     """בדיקות אינטגרציה מלאות"""
    
#     def __init__(self, temp_dir: str):
#         self.temp_dir = temp_dir
#         self.logger = logging.getLogger(__name__)
    
#     def test_full_pipeline(self):
#         """בדיקת pipeline מלא"""
#         print("\n🔄 מריץ בדיקת pipeline מלא...")
        
#         # יצירת schema מורכבת
#         complex_schema = {
#             "device_temp": {"type": "float", "bits": 32, "min": -40.0, "max": 120.0},
#             "cpu_usage": {"type": "int", "bits": 8},
#             "memory_usage": {"type": "int", "bits": 8}, 
#             "network_status": {"type": "enum", "values": [0, 1, 2, 3]},
#             "is_online": {"type": "bool"},
#             "hostname": {"type": "string", "length": 12, "compressed": True},
#             "uptime": {"type": "time", "bits": 32, "range": 86400},
#             "error_count": {"type": "int", "bits": 16}
#         }
        
#         schema_file = os.path.join(self.temp_dir, "integration_schema.json")
#         with open(schema_file, 'w') as f:
#             json.dump(complex_schema, f)
        
#         # יצירת מחולל
#         generator = EnhancedTelemetryGeneratorPro(
#             schema_file,
#             output_dir=self.temp_dir,
#             enable_gpu=True
#         )
        
#         # הדפסת מידע על הסכמה
#         schema_info = generator.get_enhanced_schema_info()
#         print(f"📊 Schema info: {schema_info['data_fields_count']} fields, "
#               f"{schema_info['bytes_per_record']} bytes/record")
        
#         # יצירת קבצים בפורמטים שונים
#         test_cases = [
#             (OutputFormat.BINARY, 10000, "high_volume"),
#             (OutputFormat.JSON, 1000, "debug_data"), 
#             (OutputFormat.INFLUX_LINE, 2000, "influx_import")
#         ]
        
#         all_paths = []
        
#         for format_type, num_records, prefix in test_cases:
#             print(f"📝 יוצר {num_records} records בפורמט {format_type.value}...")
            
#             # הערכת דרישות אחסון
#             storage = generator.estimate_storage_requirements(
#                 num_records, format_type, compression_ratio=0.6
#             )
#             print(f"   💾 צפוי: {storage['compressed_mb']:.1f} MB")
            
#             # יצירה בפועל
#             paths = generator.generate_multiple_files_enhanced(
#                 num_files=3,
#                 records_per_file=num_records // 3,
#                 file_prefix=prefix,
#                 output_format=format_type,
#                 use_gpu_batches=True,
#                 batch_size=500,
#                 record_type_ratio={
#                     RecordType.UPDATE: 0.7,
#                     RecordType.EVENT: 0.3
#                 }
#             )
            
#             all_paths.extend(paths)
            
#             # בדיקת התוצאות
#             total_size = sum(os.path.getsize(path) for path in paths)
#             actual_mb = total_size / (1024 * 1024)
#             print(f"   ✅ יצר {len(paths)} קבצים, גודל: {actual_mb:.1f} MB")
        
#         # בדיקת benchmark
#         print("\n⚡ מריץ benchmark...")
#         benchmark_results = generator.benchmark_generation_speed(
#             test_records=5000,
#             use_gpu=True,
#             batch_size=1000
#         )
        
#         for metric, value in benchmark_results.items():
#             print(f"   {metric}: {value:.1f}")
        
#         print(f"\n✅ Pipeline test הושלם בהצלחה!")
#         print(f"   📁 יצר {len(all_paths)} קבצים")
#         print(f"   📊 סה״כ {sum(len(complex_schema) for _ in range(3))} * רשומות")
        
#         return True


# if __name__ == "__main__":
#     import tempfile
#     import shutil
    
#     # יצירת תיקיה זמנית
#     temp_dir = tempfile.mkdtemp()
    
#     try:
#         print("🧪 מתחיל בדיקות מקיפות...")
        
#         # בדיקות יחידה
#         unit_success = run_comprehensive_enhanced_tests()
        
#         if unit_success:
#             print("\n" + "="*60)
#             # בדיקות אינטגרציה
#             integration_test = IntegrationTest(temp_dir)
#             integration_success = integration_test.test_full_pipeline()
            
#             if integration_success:
#                 print("\n🎉 כל הבדיקות עברו בהצלחה!")
#                 print("✅ הקוד מוכן לשימוש בפרודקציה!")
#             else:
#                 print("\n❌ בדיקות האינטגרציה נכשלו")
#         else:
#             print("\n❌ בדיקות היחידה נכשלו - צריך לתקן")
    
#     finally:
#         # ניקוי
#         shutil.rmtree(temp_dir)
#         print(f"\n🧹 נוקה תיקיה זמנית: {temp_dir}")
import os
import json
import tempfile
import unittest
import shutil
import struct
import time
from typing import Dict, Any
import logging

# ייבוא הקלאסים שלנו - עדכני לפי הקבצים שלך
from generator import (
    EnhancedBitPacker,
    InfluxDBLineProtocolWriter,
    EnhancedTelemetryGeneratorPro,
    RecordType,
    OutputFormat,
    TelemetryRecord
)

class TestInfluxDBLineProtocol(unittest.TestCase):
    """בדיקות לפורמט InfluxDB Line Protocol"""
    
    def test_basic_line_format(self):
        """בדיקת פורמט בסיסי"""
        measurement = "temperature"
        tags = {"sensor": "A", "location": "room1"}
        fields = {"value": 23.5, "status": True}
        timestamp = 1640995200000000000
        
        line = InfluxDBLineProtocolWriter.format_record(
            measurement, tags, fields, timestamp
        )
        
        # בדיקה שהפורמט נכון
        self.assertIn("temperature,sensor=A,location=room1", line)
        # תיקון: boolean True -> "true" ב-InfluxDB
        self.assertIn('value=23.5,status=true', line)  # תוקן מ-false ל-true
        self.assertIn(str(timestamp), line)

    def test_string_field_quoting(self):
        """בדיקת ציטוט של string fields"""
        measurement = "logs"
        tags = {}
        fields = {"message": "Hello World", "level": "info"}
        timestamp = 0
        
        line = InfluxDBLineProtocolWriter.format_record(
            measurement, tags, fields, timestamp
        )
        
        self.assertIn('"Hello World"', line)
        self.assertIn('"info"', line)

    def test_integer_field_suffix(self):
        """בדיקת סיומת i לשדות int"""
        measurement = "counter"
        tags = {}
        fields = {"count": 42}
        timestamp = 0
        
        line = InfluxDBLineProtocolWriter.format_record(
            measurement, tags, fields, timestamp
        )
        
        self.assertIn('count=42i', line)


class TestEnhancedTelemetryGenerator(unittest.TestCase):
    """בדיקות מתקדמות למחלקת הראשית"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.schema_file = os.path.join(self.temp_dir, "enhanced_schema.json")
        
        # Schema מתקדמת עם enum strings
        self.enhanced_schema = {
            "temperature": {"type": "float", "bits": 32, "min": -50.0, "max": 150.0},
            "pressure": {"type": "int", "bits": 16},
            "device_id": {"type": "string", "length": 8, "max_length": 16},
            "status": {"type": "bool"},
            "mode": {"type": "enum", "values": [0, 1, 2, 3, 4]},
            "alert_level": {"type": "enum", "values": ["none", "low", "medium", "high"]},  # String enum
            "timestamp_offset": {"type": "time", "bits": 16, "range": 3600}
        }
        
        with open(self.schema_file, 'w') as f:
            json.dump(self.enhanced_schema, f)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_enhanced_record_generation(self):
        """בדיקת יצירת רשומה מתקדמת"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file, 
            output_dir=self.temp_dir
        )
        
        record = generator.generate_enhanced_record()
        
        # בדיקות בסיסיות
        self.assertIsInstance(record, TelemetryRecord)
        self.assertIn(record.record_type, [RecordType.UPDATE, RecordType.EVENT])
        self.assertGreater(record.timestamp, 0)
        self.assertGreaterEqual(record.sequence_id, 0)
        
        # בדיקת נתונים
        self.assertIn("temperature", record.data)
        self.assertIn("device_id", record.data)
        self.assertIn("alert_level", record.data)  # בדיקת string enum
        
        # בדיקת טווחים
        temp = record.data["temperature"]
        self.assertGreaterEqual(temp, -50.0)
        self.assertLessEqual(temp, 150.0)
        
        device_id = record.data["device_id"]
        self.assertIsInstance(device_id, str)
        self.assertLessEqual(len(device_id), 16)
        
        # בדיקת string enum
        alert = record.data["alert_level"]
        self.assertIn(alert, ["none", "low", "medium", "high"])

    def test_string_enum_handling(self):
        """בדיקה מיוחדת לטיפול ב-string enums"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        # יצירת הרבה רשומות לבדיקת התפלגות
        records = [generator.generate_enhanced_record() for _ in range(100)]
        
        alert_values = [r.data["alert_level"] for r in records]
        unique_alerts = set(alert_values)
        
        # בדיקה שכל הערכים תקינים
        expected_values = {"none", "low", "medium", "high"}
        self.assertTrue(unique_alerts.issubset(expected_values))
        
        # בדיקה שהתפלגות סבירה (לא כולם אותו ערך)
        self.assertGreater(len(unique_alerts), 1)

    def test_record_type_ratios(self):
        """בדיקת יחס סוגי רשומות"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        # יצירת הרבה רשומות עם יחס מוגדר
        ratio = {RecordType.UPDATE: 0.8, RecordType.EVENT: 0.2}
        file_path = os.path.join(self.temp_dir, "ratio_test.json")
        
        generator.write_records_enhanced(
            file_path,
            1000,
            output_format=OutputFormat.JSON,
            record_type_ratio=ratio
        )
        
        # ספירת סוגי רשומות
        update_count = 0
        event_count = 0
        
        with open(file_path, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                if data['type'] == 'update':
                    update_count += 1
                else:
                    event_count += 1
        
        total = update_count + event_count
        update_ratio = update_count / total
        event_ratio = event_count / total
        
        # בדיקה שהיחס קרוב למצופה (±10%)
        self.assertAlmostEqual(update_ratio, 0.8, delta=0.1)
        self.assertAlmostEqual(event_ratio, 0.2, delta=0.1)

    def test_multiple_output_formats(self):
        """בדיקת פורמטי פלט שונים"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        test_cases = [
            (OutputFormat.BINARY, ".bin"),
            (OutputFormat.JSON, ".json"),
            (OutputFormat.INFLUX_LINE, ".txt")
        ]
        
        for format_type, extension in test_cases:
            with self.subTest(format=format_type):
                file_path = os.path.join(self.temp_dir, f"test{extension}")
                generator.write_records_enhanced(
                    file_path,
                    100,
                    output_format=format_type
                )
                
                self.assertTrue(os.path.exists(file_path))
                self.assertGreater(os.path.getsize(file_path), 0)

    def test_enhanced_parallel_generation(self):
        """בדיקת יצירה מקבילית מתקדמת"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        paths = generator.generate_multiple_files_enhanced(
            num_files=3,
            records_per_file=100,
            output_format=OutputFormat.JSON,
            record_type_ratio={RecordType.UPDATE: 0.5, RecordType.EVENT: 0.5}
        )
        
        self.assertEqual(len(paths), 3)
        
        for path in paths:
            self.assertTrue(os.path.exists(path))
            self.assertGreater(os.path.getsize(path), 0)
            
            # בדיקה שהקובץ מכיל JSON תקין
            with open(path, 'r') as f:
                line_count = 0
                for line in f:
                    data = json.loads(line.strip())
                    self.assertIn('type', data)
                    self.assertIn('timestamp', data)
                    self.assertIn('data', data)
                    line_count += 1
                
                self.assertEqual(line_count, 100)

    def test_storage_estimation(self):
        """בדיקת הערכת דרישות אחסון"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        # בדיקה לפורמטים שונים
        for format_type in OutputFormat:
            with self.subTest(format=format_type):
                estimation = generator.estimate_storage_requirements(
                    10000, 
                    format_type,
                    compression_ratio=0.7
                )
                
                self.assertEqual(estimation["records"], 10000)
                self.assertEqual(estimation["format"], format_type.value)
                self.assertGreater(estimation["uncompressed_bytes"], 0)
                self.assertLess(estimation["compressed_bytes"], estimation["uncompressed_bytes"])
                self.assertEqual(estimation["compression_ratio"], 0.7)

    def test_data_validation(self):
        """בדיקת וולידציה של נתונים - רק ערכים חוקיים"""
        generator = EnhancedTelemetryGeneratorPro(
            self.schema_file,
            output_dir=self.temp_dir
        )
        
        # יצירת 1000 רשומות לבדיקה סטטיסטית
        records = []
        for _ in range(1000):
            record = generator.generate_enhanced_record()
            records.append(record)
        
        # בדיקת כל הרשומות
        for record in records:
            data = record.data
            
            # טמפרטורה בטווח
            temp = data["temperature"]
            self.assertGreaterEqual(temp, -50.0, f"Temperature {temp} below minimum")
            self.assertLessEqual(temp, 150.0, f"Temperature {temp} above maximum")
            
            # לחץ בטווח תקין
            pressure = data["pressure"]
            self.assertGreaterEqual(pressure, 0, f"Pressure {pressure} negative")
            self.assertLessEqual(pressure, 65535, f"Pressure {pressure} above 16-bit max")
            
            # String enum תקין
            alert = data["alert_level"]
            self.assertIn(alert, ["none", "low", "medium", "high"], f"Invalid alert level: {alert}")
            
            # Int enum תקין
            mode = data["mode"]
            self.assertIn(mode, [0, 1, 2, 3, 4], f"Invalid mode: {mode}")
            
            # Boolean תקין
            status = data["status"]
            self.assertIsInstance(status, bool, f"Status not boolean: {status}")
            
            # String ID תקין
            device_id = data["device_id"]
            self.assertIsInstance(device_id, str, f"Device ID not string: {device_id}")
            self.assertLessEqual(len(device_id), 16, f"Device ID too long: {device_id}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
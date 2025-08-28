# import os
# import json
# import tempfile
# import unittest
# import shutil
# from typing import Dict, Any
# import logging

# from my_generator import BitPacker,TelemetryGeneratorPro
# # Import the main classes (assuming they're in telemetry_generator.py)
# # from telemetry_generator import TelemetryGeneratorPro, BitPacker

# class TestBitPacker(unittest.TestCase):
#     """בדיקות למחלקת BitPacker"""
    
#     def setUp(self):
#         self.packer = BitPacker()
    
#     def test_simple_write(self):
#         """בדיקה בסיסית של כתיבת ביטים"""
#         self.packer.write(5, 3)  # 101 in binary
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
#         # 101 padded to 10100000 = 0xA0 = 160
#         self.assertEqual(result, bytes([0xA0]))
    
#     def test_multiple_values(self):
#         """בדיקת כתיבת מספר ערכים"""
#         self.packer.write(7, 3)   # 111
#         self.packer.write(2, 2)   # 10
#         self.packer.write(1, 1)   # 1
#         # Total: 111101 padded to 11110100 = 0xF4 = 244
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
#         self.assertEqual(result, bytes([0xF4]))
    
#     def test_cross_byte_boundary(self):
#         """בדיקת מעבר גבולות בייט"""
#         self.packer.write(255, 8)  # Full byte
#         self.packer.write(3, 2)    # Two more bits
#         self.packer.flush_to_byte_boundary()
#         result = self.packer.bytes()
#         # First byte: 11111111, Second byte: 11000000
#         self.assertEqual(result, bytes([0xFF, 0xC0]))
    
#     def test_value_validation(self):
#         """בדיקת וולידציה של ערכים"""
#         with self.assertRaises(ValueError):
#             self.packer.write(8, 3)  # 8 doesn't fit in 3 bits
        
#         with self.assertRaises(ValueError):
#             self.packer.write(-1, 4)  # Negative values not allowed


# class TestTelemetryGeneratorBasic(unittest.TestCase):
#     """בדיקות בסיסיות למחלקת TelemetryGeneratorPro"""
    
#     def setUp(self):
#         self.temp_dir = tempfile.mkdtemp()
#         self.schema_file = os.path.join(self.temp_dir, "test_schema.json")
        
#         # Schema לבדיקה
#         self.test_schema = {
#             "temperature": {"type": "int", "bits": 12},
#             "pressure": {"type": "int", "bits": 16},
#             "status": {"type": "bool", "bits": 1},
#             "mode": {"type": "enum", "values": [0, 1, 2, 3]},
#             "time": {"type": "time", "bits": 8, "range": 3600}
#         }
        
#         # כתיבת הסכמה לקובץ
#         with open(self.schema_file, 'w') as f:
#             json.dump(self.test_schema, f)
    
#     def tearDown(self):
#         shutil.rmtree(self.temp_dir)
    
#     def test_schema_loading(self):
#         """בדיקת טעינת סכמה"""
#         generator = TelemetryGeneratorPro(self.schema_file)
#         self.assertEqual(generator.schema, self.test_schema)
        
#         # בדיקה שיש 6 שדות (5 + ID)
#         self.assertEqual(len(generator._compiled_fields), 6)
    
#     def test_invalid_schema_file(self):
#         """בדיקת טעינת קובץ סכמה לא קיים"""
#         with self.assertRaises(FileNotFoundError):
#             TelemetryGeneratorPro("non_existent.json")
    
#     def test_invalid_json(self):
#         """בדיקת JSON פגום"""
#         bad_file = os.path.join(self.temp_dir, "bad.json")
#         with open(bad_file, 'w') as f:
#             f.write("{ invalid json }")
        
#         with self.assertRaises(ValueError):
#             TelemetryGeneratorPro(bad_file)
    
#     def test_record_generation(self):
#         """בדיקת יצירת רשומות"""
#         generator = TelemetryGeneratorPro(self.schema_file)
        
#         # יצירת רשומה אחת
#         values = generator.generate_record_values()
        
#         # בדיקה שיש 6 ערכים (5 שדות + ID)
#         self.assertEqual(len(values), 6)
        
#         # בדיקה שכל הערכים הם מספרים חיוביים
#         for val in values:
#             self.assertIsInstance(val, int)
#             self.assertGreaterEqual(val, 0)
    
#     def test_sequential_ids(self):
#         """בדיקת ID עוקב"""
#         generator = TelemetryGeneratorPro(self.schema_file)
        
#         # יצירת 3 רשומות
#         record1 = generator.generate_record_values()
#         record2 = generator.generate_record_values()
#         record3 = generator.generate_record_values()
        
#         # בדיקה שה-ID (שדה ראשון) עולה
#         self.assertEqual(record1[0], 0)
#         self.assertEqual(record2[0], 1)
#         self.assertEqual(record3[0], 2)
    
#     def test_record_packing(self):
#         """בדיקת אריזת רשומות"""
#         generator = TelemetryGeneratorPro(self.schema_file)
        
#         values = generator.generate_record_values()
#         packed = generator.pack_record(values)
        
#         # בדיקה שהתוצאה היא bytes
#         self.assertIsInstance(packed, bytes)
#         self.assertGreater(len(packed), 0)
    
#     def test_single_file_creation(self):
#         """בדיקת יצירת קובץ יחיד"""
#         generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        
#         file_path = os.path.join(self.temp_dir, "test.bin")
#         generator.write_records_to_file(file_path, 100)
        
#         # בדיקה שהקובץ נוצר
#         self.assertTrue(os.path.exists(file_path))
        
#         # בדיקה שהקובץ לא ריק
#         self.assertGreater(os.path.getsize(file_path), 0)
    
#     def test_multiple_files_creation(self):
#         """בדיקת יצירת קבצים מרובים"""
#         generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        
#         paths = generator.generate_multiple_files_parallel(
#             num_files=3,
#             records_per_file=50,
#             file_prefix="test_multi"
#         )
        
#         # בדיקה שנוצרו 3 קבצים
#         self.assertEqual(len(paths), 3)
        
#         for path in paths:
#             self.assertTrue(os.path.exists(path))
#             self.assertGreater(os.path.getsize(path), 0)
    
#     def test_schema_info(self):
#         """בדיקת מידע על סכמה"""
#         generator = TelemetryGeneratorPro(self.schema_file)
#         info = generator.get_schema_info()
        
#         # בדיקות בסיסיות
#         self.assertIn("fields_count", info)
#         self.assertIn("total_bits_per_record", info)
#         self.assertIn("bytes_per_record", info)
#         self.assertIn("fields", info)
        
#         self.assertEqual(info["fields_count"], 6)  # 5 + ID
#         self.assertGreater(info["total_bits_per_record"], 0)
#         self.assertGreater(info["bytes_per_record"], 0)


# class TestPerformance(unittest.TestCase):
#     """בדיקות ביצועים"""
    
#     def setUp(self):
#         self.temp_dir = tempfile.mkdtemp()
#         self.schema_file = os.path.join(self.temp_dir, "perf_schema.json")
        
#         # Schema גדולה יותר לבדיקת ביצועים
#         self.perf_schema = {
#             f"field_{i}": {"type": "int", "bits": 16} 
#             for i in range(20)  # 20 שדות
#         }
        
#         with open(self.schema_file, 'w') as f:
#             json.dump(self.perf_schema, f)
    
#     def tearDown(self):
#         shutil.rmtree(self.temp_dir)
    
#     def test_large_file_generation(self):
#         """בדיקת יצירת קובץ גדול"""
#         import time
        
#         generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        
#         start_time = time.time()
#         file_path = os.path.join(self.temp_dir, "large_test.bin")
#         generator.write_records_to_file(file_path, 10000)  # 10K records
#         end_time = time.time()
        
#         # בדיקות בסיסיות
#         self.assertTrue(os.path.exists(file_path))
#         self.assertGreater(os.path.getsize(file_path), 0)
        
#         # בדיקת זמן (צריך להיות מתחת ל-5 שניות)
#         elapsed = end_time - start_time
#         print(f"Generated 10K records in {elapsed:.2f} seconds")
#         self.assertLess(elapsed, 5.0)
    
#     def test_parallel_performance(self):
#         """בדיקת ביצועי יצירה במקביל"""
#         import time
        
#         generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        
#         start_time = time.time()
#         paths = generator.generate_multiple_files_parallel(
#             num_files=5,
#             records_per_file=2000,  # 10K total records
#             max_workers=4
#         )
#         end_time = time.time()
        
#         # בדיקות
#         self.assertEqual(len(paths), 5)
#         for path in paths:
#             self.assertTrue(os.path.exists(path))
        
#         elapsed = end_time - start_time
#         print(f"Generated 5 files (10K records total) in {elapsed:.2f} seconds")
#         self.assertLess(elapsed, 3.0)  # במקביל צריך להיות מהיר יותר


# def run_comprehensive_test():
#     """מריץ את כל הבדיקות"""
#     # הגדרת logging
#     logging.basicConfig(level=logging.INFO)
    
#     # יצירת test suite
#     suite = unittest.TestSuite()
    
#     # הוספת כל הבדיקות
#     suite.addTest(unittest.makeSuite(TestBitPacker))
#     suite.addTest(unittest.makeSuite(TestTelemetryGeneratorBasic))
#     suite.addTest(unittest.makeSuite(TestPerformance))
    
#     # הרצת הבדיקות
#     runner = unittest.TextTestRunner(verbosity=2)
#     result = runner.run(suite)
    
#     return result.wasSuccessful()


# if __name__ == "__main__":
#     print("🧪 מריץ בדיקות מקיפות לקוד הבסיסי...")
#     success = run_comprehensive_test()
    
#     if success:
#         print("✅ כל הבדיקות עברו בהצלחה!")
#     else:
#         print("❌ יש בדיקות שנכשלו - צריך לתקן")

import os
import json
import tempfile
import unittest
import shutil
from typing import Dict, Any
import logging

# נתיב לקובץ הסכמה החיצוני

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "perf_schema.json")
from my_generator import BitPacker, TelemetryGeneratorPro


class TestBitPacker(unittest.TestCase):
    """בדיקות למחלקת BitPacker"""
    
    def setUp(self):
        self.packer = BitPacker()
    
    def test_simple_write(self):
        self.packer.write(5, 3)
        self.packer.flush_to_byte_boundary()
        result = self.packer.bytes()
        self.assertEqual(result, bytes([0xA0]))
    
    def test_multiple_values(self):
        self.packer.write(7, 3)
        self.packer.write(2, 2)
        self.packer.write(1, 1)
        self.packer.flush_to_byte_boundary()
        result = self.packer.bytes()
        self.assertEqual(result, bytes([0xF4]))
    
    def test_cross_byte_boundary(self):
        self.packer.write(255, 8)
        self.packer.write(3, 2)
        self.packer.flush_to_byte_boundary()
        result = self.packer.bytes()
        self.assertEqual(result, bytes([0xFF, 0xC0]))
    
    def test_value_validation(self):
        with self.assertRaises(ValueError):
            self.packer.write(8, 3)
        with self.assertRaises(ValueError):
            self.packer.write(-1, 4)


class TestTelemetryGeneratorBasic(unittest.TestCase):
    """בדיקות בסיסיות למחלקת TelemetryGeneratorPro"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.schema_file = SCHEMA_FILE

        # אם הקובץ לא קיים, יוצרים אותו זמנית
        if not os.path.exists(self.schema_file):
            test_schema = {
                "temperature": {"type": "int", "bits": 12},
                "pressure": {"type": "int", "bits": 16},
                "status": {"type": "bool", "bits": 1},
                "mode": {"type": "enum", "values": [0, 1, 2, 3]},
                "time": {"type": "time", "bits": 8, "range": 3600}
            }
            with open(self.schema_file, 'w') as f:
                json.dump(test_schema, f)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_schema_loading(self):
        generator = TelemetryGeneratorPro(self.schema_file)
        with open(self.schema_file, 'r') as f:
            loaded_schema = json.load(f)
        self.assertEqual(generator.schema, loaded_schema)
        self.assertEqual(len(generator._compiled_fields), len(loaded_schema) + 1)  # +1 ל-ID
    
    def test_record_generation(self):
        generator = TelemetryGeneratorPro(self.schema_file)
        values = generator.generate_record_values()
        self.assertEqual(len(values), len(generator._compiled_fields))
        for val in values:
            self.assertIsInstance(val, int)
            self.assertGreaterEqual(val, 0)
    
    def test_sequential_ids(self):
        generator = TelemetryGeneratorPro(self.schema_file)
        record1 = generator.generate_record_values()
        record2 = generator.generate_record_values()
        record3 = generator.generate_record_values()
        self.assertEqual(record1[0], 0)
        self.assertEqual(record2[0], 1)
        self.assertEqual(record3[0], 2)
    
    def test_record_packing(self):
        generator = TelemetryGeneratorPro(self.schema_file)
        values = generator.generate_record_values()
        packed = generator.pack_record(values)
        self.assertIsInstance(packed, bytes)
        self.assertGreater(len(packed), 0)
    
    def test_single_file_creation(self):
        generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        file_path = os.path.join(self.temp_dir, "test.bin")
        generator.write_records_to_file(file_path, 100)
        self.assertTrue(os.path.exists(file_path))
        self.assertGreater(os.path.getsize(file_path), 0)
    
    def test_multiple_files_creation(self):
        generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        paths = generator.generate_multiple_files_parallel(num_files=3, records_per_file=50, file_prefix="test_multi")
        self.assertEqual(len(paths), 3)
        for path in paths:
            self.assertTrue(os.path.exists(path))
            self.assertGreater(os.path.getsize(path), 0)
    
    def test_schema_info(self):
        generator = TelemetryGeneratorPro(self.schema_file)
        info = generator.get_schema_info()
        self.assertIn("fields_count", info)
        self.assertIn("total_bits_per_record", info)
        self.assertIn("bytes_per_record", info)
        self.assertIn("fields", info)
        self.assertEqual(info["fields_count"], len(generator._compiled_fields))
        self.assertGreater(info["total_bits_per_record"], 0)
        self.assertGreater(info["bytes_per_record"], 0)


class TestPerformance(unittest.TestCase):
    """בדיקות ביצועים"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.schema_file = SCHEMA_FILE

        # אם הקובץ לא קיים, יוצרים schema גדול לביצועים
        if not os.path.exists(self.schema_file):
            perf_schema = {f"field_{i}": {"type": "int", "bits": 16} for i in range(20)}
            with open(self.schema_file, 'w') as f:
                json.dump(perf_schema, f)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_large_file_generation(self):
        import time
        generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        start_time = time.time()
        file_path = os.path.join(self.temp_dir, "large_test.bin")
        generator.write_records_to_file(file_path, 10000)
        elapsed = time.time() - start_time
        self.assertTrue(os.path.exists(file_path))
        self.assertGreater(os.path.getsize(file_path), 0)
        print(f"Generated 10K records in {elapsed:.2f} seconds")
        self.assertLess(elapsed, 5.0)
    
    def test_parallel_performance(self):
        import time
        generator = TelemetryGeneratorPro(self.schema_file, output_dir=self.temp_dir)
        start_time = time.time()
        paths = generator.generate_multiple_files_parallel(num_files=5, records_per_file=2000, max_workers=4)
        elapsed = time.time() - start_time
        self.assertEqual(len(paths), 5)
        for path in paths:
            self.assertTrue(os.path.exists(path))
        print(f"Generated 5 files (10K records total) in {elapsed:.2f} seconds")
        self.assertLess(elapsed, 3.0)


def run_comprehensive_test():
    logging.basicConfig(level=logging.INFO)
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBitPacker))
    suite.addTest(unittest.makeSuite(TestTelemetryGeneratorBasic))
    suite.addTest(unittest.makeSuite(TestPerformance))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    print("🧪 מריץ בדיקות מקיפות לקוד הבסיסי...")
    success = run_comprehensive_test()
    if success:
        print("✅ כל הבדיקות עברו בהצלחה!")
    else:
        print("❌ יש בדיקות שנכשלו - צריך לתקן")

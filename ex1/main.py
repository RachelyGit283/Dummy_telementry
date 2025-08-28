import os
import logging
from my_generator import TelemetryGeneratorPro  # נניח שהמחלקה שלך שמורה בקובץ הזה

def main():
    # הגדרות בסיסיות
    schema_file = "perf_schema.json"          # כאן תכניס את הנתיב לקובץ הסכמה שלך
    output_file = "telemetry_output.bin" # הקובץ הבינארי שיווצר
    num_records = 1000                   # מספר רשומות ליצירה

    # הגדרת לוגר בסיסי
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("TelemetryMain")

    # יצירת הגנרטור
    generator = TelemetryGeneratorPro(
        schema_file=schema_file,
        output_dir=".",          # אפשר לשנות את התיקייה אם רוצים
        logger=logger
    )

    # כתיבת רשומות לקובץ
    generator.write_records_to_file(output_file, num_records)

    logger.info(f"Finished writing {num_records} records to {output_file}")

if __name__ == "__main__":
    main()
# from my_generator import TelemetryGeneratorPro, BitPacker

# def unpack_record(data: bytes, nbits_list):
#     """פענוח רשומה לביטים לפי רשימת nbits"""
#     values = []
#     bitbuf = 0
#     bitcount = 0
#     idx = 0

#     for nbits in nbits_list:
#         val = 0
#         while nbits > 0:
#             if bitcount == 0:
#                 if idx >= len(data):
#                     raise ValueError("Not enough data to unpack")
#                 bitbuf = data[idx]
#                 idx += 1
#                 bitcount = 8

#             take = min(bitcount, nbits)
#             val = (val << take) | ((bitbuf >> (bitcount - take)) & ((1 << take) - 1))
#             bitcount -= take
#             nbits -= take
#         values.append(val)
#     return values

# def main():
#     file_path = "telemetry_output.bin"
#     generator = TelemetryGeneratorPro(schema_file="perf_schema.json")

#     info = generator.get_schema_info()
#     nbits_list = [f["bits"] for f in info["fields"]]

#     with open(file_path, "rb") as f:
#         raw = f.read()

#     record_size_bytes = info["bytes_per_record"]
#     num_records_to_show = 5

#     print(f"Showing first {num_records_to_show} records from {file_path}:")

#     for i in range(num_records_to_show):
#         start = i * record_size_bytes
#         end = start + record_size_bytes
#         record_bytes = raw[start:end]
#         values = unpack_record(record_bytes, nbits_list)
#         info = generator.get_schema_info()
#         field_names = [f["name"] for f in info["fields"]]

#         values = unpack_record(record_bytes, [f["bits"] for f in info["fields"]])
#         record_dict = dict(zip(field_names, values))
#         print(record_dict)
#         print(f"Record {i}: {values}")

# if __name__ == "__main__":
#     main()
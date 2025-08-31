# Telemetry Generator

High-performance telemetry data generator with schema support, rate control, and multiple output formats.

## Features

- **Schema-driven generation** - Define your telemetry structure in JSON
- **Multiple output formats** - NDJSON, Binary, LEB128, InfluxDB Line Protocol
- **Rolling file support** - Automatic file rotation based on size
- **Rate control** - Precise control over generation rate (records/sec)
- **Load profiles** - Predefined profiles for common testing scenarios
- **GPU acceleration** - Optional CUDA support for high-throughput generation
- **Parallel generation** - Multi-threaded file writing
- **Compression support** - Built-in gzip compression
- **Flexible data types** - int, float, string, bool, enum, time

## Installation

### Basic Installation
```bash
pip install telemetry-generator
```

### With GPU Support
```bash
pip install telemetry-generator[gpu]
```

### From Source
```bash
git clone https://github.com/yourusername/telemetry-generator.git
cd telemetry-generator
pip install -e .
```

## Quick Start

### 1. Create a Schema File

Create `schema.json`:
```json
{
    "device_id": {
        "type": "string",
        "length": 8
    },
    "temperature": {
        "type": "float",
        "min": -40.0,
        "max": 85.0
    },
    "status": {
        "type": "enum",
        "values": ["active", "idle", "error"]
    },
    "timestamp": {
        "type": "time",
        "bits": 64
    }
}
```

### 2. Generate Data

Basic generation:
```bash
telegen generate --schema schema.json --rate 1000 --duration 60
```

With specific output format and rotation:
```bash
telegen generate \
    --schema schema.json \
    --rate 5000 \
    --duration 300 \
    --format ndjson \
    --rotate-size 100MB \
    --out-dir /data/telemetry/
```

## Command Line Interface

### Basic Commands

```bash
# Generate telemetry data
telegen generate --schema schema.json --rate 1000 --duration 60

# Validate schema
telegen validate --schema schema.json --records 100

# List available load profiles
telegen profiles
```

### Generation Options

| Option | Description | Default |
|--------|-------------|---------|
| `--schema, -s` | Path to JSON schema file | Required |
| `--rate, -r` | Records per second | 1000 |
| `--duration, -d` | Duration in seconds | 60 |
| `--out-dir, -o` | Output directory | data/ |
| `--rotate-size` | Max file size before rotation | 512MB |
| `--format, -f` | Output format (ndjson/json/binary/influx/leb128) | ndjson |
| `--compress` | Enable gzip compression | False |
| `--batch-size, -b` | Records per batch | 100 |
| `--prefix, -p` | Filename prefix | telemetry |
| `--workers, -w` | Number of worker threads | 4 |
| `--seed` | Random seed for reproducibility | None |
| `--gpu` | Enable GPU acceleration | False |

## Load Profiles

Use predefined load profiles for common scenarios:

```bash
# Low load for development
telegen generate --schema schema.json --load-profile low

# High load for performance testing
telegen generate --schema schema.json --load-profile high

# Stress test
telegen generate --schema schema.json --load-profile stress
```

### Available Profiles

| Profile | Rate (rec/s) | Duration | Description |
|---------|-------------|----------|-------------|
| `low` | 100 | 60s | Development and testing |
| `medium` | 1,000 | 5m | Integration testing |
| `high` | 10,000 | 10m | Performance testing |
| `stress` | 50,000 | 30m | Maximum load testing |
| `burst` | Variable | 10m | Alternating high/low load |
| `realistic` | Variable | 1h | Sine wave pattern |
| `endurance` | 500 | 24h | Long-running stability test |
| `spike` | Variable | 15m | Sudden load increases |
| `ramp` | 100→10,000 | 20m | Gradually increasing load |
| `chaos` | Random | 30m | Random variations |

## Output Formats

### NDJSON (Newline-Delimited JSON)
```json
{"type":"update","timestamp":1699123456789,"seq_id":1,"data":{"temperature":23.5,"status":"active"}}
{"type":"update","timestamp":1699123456790,"seq_id":2,"data":{"temperature":23.6,"status":"active"}}
```

### Binary Format
Compact binary encoding with BitPacking for efficient storage.

### LEB128 Format
Variable-length encoding for integers, very efficient for small values.

### InfluxDB Line Protocol
```
telemetry,type=update,seq_id=1 temperature=23.5,status="active" 1699123456789
```

## Schema Definition

### Supported Data Types

#### Integer
```json
{
    "counter": {
        "type": "int",
        "bits": 32
    }
}
```

#### Float
```json
{
    "temperature": {
        "type": "float",
        "min": -50.0,
        "max": 100.0,
        "precision_bits": 16
    }
}
```

#### String
```json
{
    "device_name": {
        "type": "string",
        "length": 16,
        "max_length": 32
    }
}
```

#### Boolean
```json
{
    "is_active": {
        "type": "bool"
    }
}
```

#### Enum
```json
{
    "status": {
        "type": "enum",
        "values": ["active", "idle", "error", "maintenance"]
    }
}
```

#### Time
```json
{
    "event_time": {
        "type": "time",
        "bits": 64
    }
}
```

## Python API Usage

```python
from telemetry_generator import EnhancedTelemetryGeneratorPro
from telemetry_generator import RollingFileWriter, RateLimiter

# Initialize generator
generator = EnhancedTelemetryGeneratorPro(
    schema_file="schema.json",
    enable_gpu=False
)

# Create rolling file writer
writer = RollingFileWriter(
    base_path="data/telemetry",
    max_size_bytes=100 * 1024 * 1024,  # 100MB
    format='ndjson',
    compress=True
)

# Set up rate control
rate_limiter = RateLimiter(
    records_per_second=1000,
    batch_size=100
)

# Generate records
rate_limiter.start()
for i in range(10000):
    record = generator.generate_enhanced_record()
    writer.write_record(record, generator)
    rate_limiter.wait_if_needed()

writer.close()
```

## Testing Scenarios

### 1. Development Testing
```bash
# Quick test with small dataset
telegen generate --schema schema.json --rate 100 --duration 10
```

### 2. Performance Testing
```bash
# High throughput test
telegen generate \
    --schema schema.json \
    --load-profile high \
    --format binary \
    --workers 8 \
    --gpu
```

### 3. Endurance Testing
```bash
# 24-hour stability test
telegen generate \
    --schema schema.json \
    --load-profile endurance \
    --rotate-size 1GB \
    --compress
```

### 4. Burst Pattern Testing
```bash
# Alternating load pattern
telegen generate \
    --schema schema.json \
    --load-profile burst \
    --duration 1800
```

### 5. Reproducible Testing
```bash
# Use seed for reproducible data
telegen generate \
    --schema schema.json \
    --seed 42 \
    --rate 1000 \
    --duration 60
```

## Verifying Data

### Check Generated Files
```bash
# List generated files
ls -lh data/

# Count records in NDJSON file
wc -l data/telemetry_*.ndjson

# Inspect first few records
head -n 5 data/telemetry_0001.ndjson | jq '.'

# Check file sizes
du -sh data/*
```

### Validate Schema Compliance
```bash
# Validate schema and generate test records
telegen validate --schema schema.json --records 1000
```

## Performance Tips

1. **Use appropriate batch sizes** - Larger batches (100-1000) are more efficient
2. **Enable compression** for text formats when disk I/O is a bottleneck
3. **Use binary formats** for maximum throughput
4. **Enable GPU acceleration** for very high rates (>10k rec/s)
5. **Adjust worker threads** based on CPU cores
6. **Use SSDs** for output directory when possible
7. **Monitor system resources** during generation

## Troubleshooting

### Low Generation Rate
- Increase batch size: `--batch-size 500`
- Add more workers: `--workers 8`
- Use binary format: `--format binary`
- Enable GPU: `--gpu`

### High Memory Usage
- Reduce batch size
- Decrease number of workers
- Enable compression to reduce buffer sizes

### File Rotation Issues
- Ensure sufficient disk space
- Check write permissions
- Adjust rotation size based on throughput

## Resource Requirements

| Load Profile | CPU | Memory | Disk I/O | GPU |
|-------------|-----|--------|----------|-----|
| Low | 1 core | 512MB | 10 MB/s | No |
| Medium | 2 cores | 1GB | 50 MB/s | No |
| High | 4 cores | 2GB | 200 MB/s | Optional |
| Stress | 8+ cores | 4GB | 500+ MB/s | Recommended |

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please read CONTRIBUTING.md for guidelines.

## Support

For issues and questions:
- GitHub Issues: https://github.com/yourusername/telemetry-generator/issues
- Documentation: https://telemetry-generator.readthedocs.io

## how to get more fake telemetry
מעולה. הבעיה היא שה־global_fault_rate שלך מוגדר ל־0.20 (20%), אבל סיכוי ההחלה של כל fault type בתוך FaultInjector הוא נמוך מאוד (למשל 0.01–0.05 ברירת מחדל). לכן בפועל, גם אם רשומה נבחרה להכניס שגיאה, רוב הסיכויים שה־fault type שבחרו לא "קופץ", ולכן רשומות פגומות נמוכות מאוד.

כדי להגיע קרוב ל־20% בפועל אפשר:

להגדיל את ההסתברות בכל FaultConfig – לדוגמה, במקום probability=0.02, לשים 0.2–0.5 בהתאם לרצוי.

להקטין את מספר ה־fault types – כל שגיאה תתרחש בתדירות גבוהה יותר.

לאפשר יותר שדות להיפגע בכל רשומה – כרגע max_fields=1 כברירת מחדל, אפשר להגדיל ל־2–3.

להגדיר force_fault_count או retry – בכמה מערכות מגדירים שהמכונה תנסה להכניס fault עד שמגיעים למספר רשומות פגומות מסוים.

לדוגמה, שינוי פשוט ב־_create_default_configs שלך:

FaultConfig(
    fault_type=FaultType.OUT_OF_RANGE,
    probability=0.25,  # במקום 0.02
    ...
)
FaultConfig(
    fault_type=FaultType.WRONG_TYPE,
    probability=0.25,  # במקום 0.015
    ...
)


וכן להגדיל ב־parameters:

parameters={"max_fields": 2}


כך כל רשומה שנבחרה להיכנס ל־fault יכולה לקבל יותר משגיאה אחת, וזה מעלה את אחוז הרשומות הפגומות קרוב יותר ל־20%.
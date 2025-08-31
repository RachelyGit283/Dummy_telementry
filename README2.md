# Advanced Telemetry System - User Guide

High-performance telemetry data generator with binary schema support and fault injection capabilities.

## Quick Installation

```bash
# Install the package
pip install -e .

# Verify system works
telegen --help
```

## Load Testing Scenarios - Load Profiles

### Available Profiles

Display all available profiles:
```bash
telegen profiles
```

#### Basic Load Profiles:
- **low**: 100 rec/s, suitable for development
- **medium**: 1,000 rec/s, suitable for integration testing
- **high**: 10,000 rec/s, performance testing
- **stress**: 50,000 rec/s, maximum load testing

#### Advanced Profiles:
- **burst**: Burst load patterns (100→5,000 rec/s in cycles)
- **realistic**: Variable patterns mimicking real-world usage
- **endurance**: Long-term stability testing (24 hours)

### Running Load Scenarios

```bash
# Basic development load
telegen generate --schema gpu_schema.json --load-profile low --duration 60

# Medium load for testing
telegen generate --schema gpu_schema.json --load-profile medium --duration 300

# High load with GPU acceleration
telegen generate --schema gpu_schema.json --load-profile high --gpu --duration 600

# Extreme stress testing
telegen generate --schema gpu_schema.json --load-profile stress --workers 8 --duration 1800
```

### Custom Configuration

```bash
telegen generate \
  --schema gpu_schema.json \
  --rate 5000 \
  --batch-size 500 \
  --workers 6 \
  --duration 1200 \
  --rotate-size 1GB
```

## Schema Validation and Compliance

### Basic Validation

```bash
# Quick schema validation
telegen validate --schema gpu_schema.json

# Validation with sample records
telegen validate --schema gpu_schema.json --records 1000 --show-sample

# Size consistency check
telegen validate --schema gpu_schema.json --records 5000 --check-size
```

### Detailed Schema Information

```bash
# Display schema details
telegen info gpu_schema.json

# Expected output:
# Schema: gpu_telemetry_flat_v1
# Record size: 48 bytes
# Fields: 14
# Bit utilization: 352/384 (91.7%)
```

### Data Quality Verification

```bash
# Generate with detailed statistics
telegen generate \
  --schema gpu_schema.json \
  --rate 1000 \
  --duration 10 \
  --verbose

# Test different output formats
telegen generate --schema gpu_schema.json --format json --records 100
telegen generate --schema gpu_schema.json --format binary --records 100
telegen generate --schema gpu_schema.json --format influx --records 100
```

## Fault Injection Mechanism

### Fault Profiles

```bash
# Light faults for development (2% fault rate)
telegen generate --schema gpu_schema.json --fault-profile development --duration 60

# Diverse faults for testing (5% fault rate)
telegen generate --schema gpu_schema.json --fault-profile testing --duration 300

# Heavy faults for stress testing (15% fault rate)
telegen generate --schema gpu_schema.json --fault-profile stress --duration 600
```

### Custom Fault Configuration

```bash
# Specific fault rate
telegen generate \
  --schema gpu_schema.json \
  --enable-faults \
  --fault-rate 0.08 \
  --duration 300

# Specific fault types
telegen generate \
  --schema gpu_schema.json \
  --enable-faults \
  --fault-types out_of_range,wrong_type,missing_field \
  --fault-rate 0.05

# Save fault report
telegen generate \
  --schema gpu_schema.json \
  --fault-profile testing \
  --save-fault-report faults_report.json \
  --duration 600
```

### Creating Fault Configurations

```bash
# List available fault types
telegen list-fault-types

# Create configuration file
telegen create-fault-config my_faults.json --profile testing

# Custom configuration
telegen create-fault-config custom_faults.json \
  --fault-rate 0.1 \
  --include-types out_of_range,enum_invalid \
  --profile custom
```

### Testing Fault Injection

```bash
# Validate with fault injection
telegen validate \
  --schema gpu_schema.json \
  --test-faults \
  --fault-rate 0.1 \
  --records 1000
```

## Output Formats

```bash
# Binary format (default - fastest and most compact)
telegen generate --schema gpu_schema.json --format binary

# JSON array
telegen generate --schema gpu_schema.json --format json

# NDJSON (one record per line)
telegen generate --schema gpu_schema.json --format ndjson

# InfluxDB Line Protocol
telegen generate --schema gpu_schema.json --format influx --measurement-name gpu_metrics

# Compression
telegen generate --schema gpu_schema.json --compress
```

## Performance Testing

### Basic Benchmark
```bash
# Compare generation speeds
python -c "
from telemetry_system import create_generator
gen = create_generator(schema_file='gpu_schema.json')
results = gen.benchmark_generation_speed(test_records=10000)
print(f'Rate: {results[\"regular_records_per_sec\"]:.0f} records/sec')
"
```

### Throughput Measurement
```bash
# Test throughput with different profiles
telegen generate --schema gpu_schema.json --load-profile low --duration 30 --verbose
telegen generate --schema gpu_schema.json --load-profile medium --duration 30 --verbose
telegen generate --schema gpu_schema.json --load-profile high --duration 30 --verbose
```

## Common Troubleshooting

### Schema Validation
```bash
# Ensure schema is valid
telegen info gpu_schema.json

# Expected output:
# Format: binary
# Total bits: 384
# Record size: 48 bytes
# Bit utilization: >90%
```

### Data Verification
```bash
# Check generated files
ls -la data/

# Check file size (should be records × 48 bytes + small overhead)
du -h data/telemetry_*.bin
```

### Performance Issues
```bash
# Use GPU if available
telegen generate --schema gpu_schema.json --gpu --rate 10000

# Increase batch size for better performance
telegen generate --schema gpu_schema.json --batch-size 1000

# Increase worker threads
telegen generate --schema gpu_schema.json --workers 8
```

### Memory Issues
```bash
# Reduce rotation size
telegen generate --schema gpu_schema.json --rotate-size 128MB

# Use compression
telegen generate --schema gpu_schema.json --compress

# Reduce batch size
telegen generate --schema gpu_schema.json --batch-size 50
```

## Quick Examples

### Daily Testing
```bash
# 10 minutes of medium-rate data
telegen generate --schema gpu_schema.json --load-profile medium --duration 600
```

### Overnight Testing
```bash
# Stability test for one hour
telegen generate --schema gpu_schema.json --load-profile realistic --duration 3600
```

### Extreme Load Testing
```bash
# Push system limits
telegen generate \
  --schema gpu_schema.json \
  --rate 100000 \
  --workers 16 \
  --gpu \
  --duration 1800 \
  --rotate-size 2GB \
  --compress
```

## Configuration Files

Example optional `types.json` file:

```json
{
  "uint8": "np.uint8",
  "uint16": "np.uint16", 
  "uint32": "np.uint32",
  "uint64": "np.uint64",
  "int8": "np.int8",
  "float32": "np.float32",
  "bytes": "np.bytes_",
  "enum": "np.uint8"
}
```

The system will automatically search for a `types.json` file in the same directory as the schema file.

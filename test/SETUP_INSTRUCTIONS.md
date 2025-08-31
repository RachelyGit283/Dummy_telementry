# SETUP_INSTRUCTIONS.md

# Setting Up and Running Telemetry Generator Tests

## 1. Install Test Dependencies

```bash
# Install test requirements
pip install -r requirements-test.txt

# Or install individually
pip install pytest pytest-cov pytest-html pytest-mock pytest-xdist
```

## 2. Project Structure

Make sure your project structure looks like this:

```
telemetry_generator/
├── cli.py
├── telemetry_generator.py
├── binary_schema.py
├── binary_packer.py
├── data_generators.py
├── formatters.py
├── gpu_accelerator.py
├── types_and_enums.py
├── utilities.py
├── file_writer.py
├── gpu_batch_generator.py
├── test_telemetry_generator.py  # Main test file
├── conftest.py                  # Test configuration
├── pytest.ini                  # Pytest settings
└── run_tests.py                # Test runner script
```

## 3. Running Tests

### Basic Test Execution

```bash
# Run all tests
python run_tests.py

# Or use pytest directly
python -m pytest test_telemetry_generator.py -v
```

### Specific Test Categories

```bash
# Unit tests only
python run_tests.py unit

# Integration tests
python run_tests.py integration

# With coverage report
python run_tests.py coverage

# GPU tests (if you have GPU setup)
python run_tests.py gpu

# Performance tests
python run_tests.py slow
```

### Advanced Pytest Options

```bash
# Run specific test class
python -m pytest test_telemetry_generator.py::TestBinarySchemaProcessor -v

# Run specific test method
python -m pytest test_telemetry_generator.py::TestBinarySchemaProcessor::test_schema_processor_initialization -v

# Run tests in parallel (if you have pytest-xdist)
python -m pytest test_telemetry_generator.py -n auto

# Generate HTML report
python -m pytest test_telemetry_generator.py --html=report.html --self-contained-html

# Run tests with detailed output
python -m pytest test_telemetry_generator.py -vvv --tb=long

# Run only failed tests from last run
python -m pytest test_telemetry_generator.py --lf

# Stop on first failure
python -m pytest test_telemetry_generator.py -x
```

## 4. Test Categories Explained

### Unit Tests (Default)
- Test individual components in isolation
- Fast execution
- Mock external dependencies
- Examples: schema processing, data generation, binary packing

### Integration Tests
- Test multiple components working together
- File I/O operations
- End-to-end workflows
- Run with: `pytest -m integration`

### GPU Tests
- Test GPU acceleration features
- Requires CuPy/CUDA setup
- Gracefully degrades if GPU not available
- Run with: `pytest -m gpu`

### Slow Tests
- Performance benchmarks
- Large data generation
- Multi-file operations
- Run with: `pytest -m slow`

## 5. Coverage Reports

```bash
# Generate coverage report
python -m pytest test_telemetry_generator.py --cov=. --cov-report=html

# View coverage in terminal
python -m pytest test_telemetry_generator.py --cov=. --cov-report=term-missing

# Coverage with specific threshold
python -m pytest test_telemetry_generator.py --cov=. --cov-fail-under=80
```

## 6. Debugging Tests

```bash
# Run with pdb on failures
python -m pytest test_telemetry_generator.py --pdb

# Capture print statements
python -m pytest test_telemetry_generator.py -s

# Show local variables in tracebacks
python -m pytest test_telemetry_generator.py --tb=long

# Run single test with maximum verbosity
python -m pytest test_telemetry_generator.py::test_specific_function -vvv -s --tb=long
```

## 7. Creating Test Data

The tests automatically create temporary schema files and test data. You can also create permanent test schemas:

```python
# Create a test schema file
import json

schema = {
    "schema_name": "MyTestSchema",
    "endianness": "little",
    "total_bits": 64,
    "field1": {
        "type": "np.uint32",
        "bits": 32,
        "pos": "0-31",
        "desc": "Test field"
    }
}

with open("test_schema.json", "w") as f:
    json.dump(schema, f, indent=2)
```

## 8. Expected Test Results

When tests pass, you should see output like:

```
================================= test session starts =================================
collected 45 items

test_telemetry_generator.py::TestBinarySchemaProcessor::test_schema_processor_initialization PASSED
test_telemetry_generator.py::TestBinarySchemaProcessor::test_schema_validation_overlap_detection PASSED
test_telemetry_generator.py::TestBinaryRecordPacker::test_record_packing_size PASSED
...

========================== 45 passed in 2.34s ==========================
```

## 9. Troubleshooting

### Common Issues:

1. **ImportError**: Make sure all your modules are in the Python path
   ```bash
   export PYTHONPATH="${PYTHONPATH}:."
   ```

2. **Missing dependencies**: Install test requirements
   ```bash
   pip install -r requirements-test.txt
   ```

3. **Schema validation errors**: Check your test schema format matches the expected binary schema structure

4. **GPU tests failing**: GPU tests will skip gracefully if CUDA/CuPy not available

### Running Individual Test Components:

```bash
# Test only schema processing
python -c "
from binary_schema import BinarySchemaProcessor
schema = {'schema_name': 'test', 'endianness': 'little', 'total_bits': 64}
processor = BinarySchemaProcessor(schema)
print('Schema processor works!')
"

# Test basic record generation
python -c "
from telemetry_generator import EnhancedTelemetryGeneratorPro
import json
schema = {'schema_name': 'test', 'endianness': 'little', 'total_bits': 64, 'field1': {'type': 'np.uint32', 'bits': 32, 'pos': '0-31'}}
gen = EnhancedTelemetryGeneratorPro(schema_dict=schema)
record = gen.generate_enhanced_record()
print(f'Generated record: {record}')
"
```

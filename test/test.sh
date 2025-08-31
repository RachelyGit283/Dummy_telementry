#!/bin/bash
# test.sh - Simple bash script to run telemetry generator tests

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Telemetry Generator Test Suite${NC}"
echo "========================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is required but not found${NC}"
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "cli.py" ]]; then
    echo -e "${RED}❌ Please run this script from the project root directory${NC}"
    echo "   (where cli.py is located)"
    exit 1
fi

# Function to run a command with nice output
run_test() {
    local description="$1"
    shift
    echo -e "\n${YELLOW}📋 $description${NC}"
    echo "----------------------------------------"
    
    if "$@"; then
        echo -e "${GREEN}✅ $description - PASSED${NC}"
        return 0
    else
        echo -e "${RED}❌ $description - FAILED${NC}"
        return 1
    fi
}

# Check dependencies
echo -e "${YELLOW}🔍 Checking dependencies...${NC}"
python3 -c "import pytest; print('pytest: OK')" 2>/dev/null || {
    echo -e "${RED}❌ pytest not found. Installing...${NC}"
    pip3 install pytest pytest-cov pytest-html pytest-mock
}

# Parse command line arguments
case "${1:-all}" in
    "quick"|"q")
        echo -e "${YELLOW}⚡ Running quick tests only${NC}"
        run_test "Quick Unit Tests" python3 -m pytest test_telemetry_generator.py -x -v --tb=short -k "not slow and not integration"
        ;;
    
    "unit"|"u")
        echo -e "${YELLOW}🧪 Running unit tests${NC}"
        run_test "Unit Tests" python3 -m pytest test_telemetry_generator.py -v --tb=short -m "not integration"
        ;;
    
    "integration"|"i")
        echo -e "${YELLOW}🔗 Running integration tests${NC}"
        run_test "Integration Tests" python3 -m pytest test_telemetry_generator.py -v -m integration
        ;;
    
    "coverage"|"c")
        echo -e "${YELLOW}📊 Running tests with coverage${NC}"
        run_test "Coverage Tests" python3 -m pytest test_telemetry_generator.py --cov=. --cov-report=html --cov-report=term-missing -v
        echo -e "${GREEN}📄 Coverage report generated in htmlcov/index.html${NC}"
        ;;
    
    "gpu"|"g")
        echo -e "${YELLOW}🎮 Running GPU tests${NC}"
        run_test "GPU Tests" python3 -m pytest test_telemetry_generator.py -v -m gpu
        ;;
    
    "slow"|"s")
        echo -e "${YELLOW}🐌 Running performance tests${NC}"
        run_test "Performance Tests" python3 -m pytest test_telemetry_generator.py -v -m slow --tb=long
        ;;
    
    "validate"|"v")
        echo -e "${YELLOW}✅ Running validation tests${NC}"
        
        # Test basic imports
        run_test "Import Test" python3 -c "
from binary_schema import BinarySchemaProcessor
from telemetry_generator import EnhancedTelemetryGeneratorPro
from types_and_enums import RecordType, OutputFormat
print('All imports successful!')
"
        
        # Test schema validation with sample
        run_test "Schema Validation" python3 -c "
import json
from binary_schema import BinarySchemaProcessor

# Load sample schema if it exists
try:
    with open('test_schema.json', 'r') as f:
        schema = json.load(f)
    processor = BinarySchemaProcessor(schema)
    print(f'Schema {processor.schema_name} validated successfully!')
    print(f'Fields: {len(processor.fields)}, Total bits: {processor.total_bits}')
except FileNotFoundError:
    print('No test_schema.json found - skipping')
except Exception as e:
    print(f'Validation failed: {e}')
    exit(1)
"
        
        # Test basic record generation
        run_test "Record Generation Test" python3 -c "
from telemetry_generator import EnhancedTelemetryGeneratorPro

schema = {
    'schema_name': 'ValidateTest',
    'endianness': 'little',
    'total_bits': 64,
    'test_field': {
        'type': 'np.uint64',
        'bits': 64,
        'pos': '0-63',
        'desc': 'Test field'
    }
}

gen = EnhancedTelemetryGeneratorPro(schema_dict=schema)
record = gen.generate_enhanced_record()
packed = gen.pack_record_enhanced(record)
print(f'Generated record with {len(packed)} bytes')
print('Basic functionality working!')
"
        ;;
    
    "all"|*)
        echo -e "${YELLOW}🎯 Running complete test suite${NC}"
        
        success_count=0
        total_tests=4
        
        if run_test "Unit Tests" python3 -m pytest test_telemetry_generator.py -v --tb=short -m "not slow"; then
            ((success_count++))
        fi
        
        if run_test "Integration Tests" python3 -m pytest test_telemetry_generator.py -v -m integration --tb=short; then
            ((success_count++))
        fi
        
        if run_test "Coverage Analysis" python3 -m pytest test_telemetry_generator.py --cov=. --cov-report=term-missing --cov-fail-under=70 -q; then
            ((success_count++))
        fi
        
        if run_test "Basic Validation" python3 -c "
from binary_schema import BinarySchemaProcessor
from telemetry_generator import EnhancedTelemetryGeneratorPro
print('All core components validated!')
"; then
            ((success_count++))
        fi
        
        echo -e "\n${GREEN}📊 FINAL RESULTS${NC}"
        echo "========================================"
        echo -e "Tests passed: ${GREEN}$success_count${NC}/$total_tests"
        
        if [[ $success_count -eq $total_tests ]]; then
            echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
            exit 0
        else
            echo -e "${RED}❌ Some tests failed${NC}"
            exit 1
        fi
        ;;
esac

echo -e "\n${GREEN}✨ Test execution completed${NC}"

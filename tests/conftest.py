# tests/conftest.py
"""
Pytest configuration and shared fixtures
"""

import pytest
import tempfile
import shutil
from pathlib import Path


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data"""
    temp_dir = Path(tempfile.mkdtemp(prefix="telegen_test_"))
    yield temp_dir
    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def clean_test_dir(test_data_dir):
    """Provide a clean test directory for each test"""
    test_dir = test_data_dir / "test_run"
    test_dir.mkdir(exist_ok=True)
    yield test_dir
    # Cleanup after test
    if test_dir.exists():
        shutil.rmtree(test_dir)
        


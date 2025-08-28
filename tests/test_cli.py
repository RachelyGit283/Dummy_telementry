# tests/test_cli.py
"""
Tests for CLI functionality
"""

import pytest
from click.testing import CliRunner
import json
from pathlib import Path

from telemetry_generator.cli import cli


@pytest.fixture
def runner():
    """Create CLI runner"""
    return CliRunner()


@pytest.fixture
def schema_file(tmp_path):
    """Create temporary schema file"""
    schema = {
        "value": {"type": "int", "bits": 32},
        "status": {"type": "enum", "values": ["on", "off"]}
    }
    schema_path = tmp_path / "test_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(schema, f)
    return str(schema_path)


class TestCLI:
    """Test CLI commands"""
    
    def test_cli_help(self, runner):
        """Test help command"""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'Telemetry Generator' in result.output
    
    def test_generate_basic(self, runner, schema_file, tmp_path):
        """Test basic generation"""
        result = runner.invoke(cli, [
            'generate',
            '--schema', schema_file,
            '--rate', '100',
            '--duration', '1',
            '--out-dir', str(tmp_path)
        ])
        
        assert result.exit_code == 0
        assert 'GENERATION COMPLETE' in result.output
        
        # Check files were created
        files = list(tmp_path.glob("telemetry_*.ndjson"))
        assert len(files) > 0
    
    def test_validate_command(self, runner, schema_file):
        """Test validate command"""
        result = runner.invoke(cli, [
            'validate',
            '--schema', schema_file,
            '--records', '10'
        ])
        
        assert result.exit_code == 0
        assert 'Schema loaded successfully' in result.output
        assert 'Test generation successful' in result.output
    
    def test_profiles_command(self, runner):
        """Test profiles command"""
        result = runner.invoke(cli, ['profiles'])
        
        assert result.exit_code == 0
        assert 'Available Load Profiles' in result.output
        assert 'LOW' in result.output
        assert 'HIGH' in result.output
    
    def test_generate_with_profile(self, runner, schema_file, tmp_path):
        """Test generation with load profile"""
        result = runner.invoke(cli, [
            'generate',
            '--schema', schema_file,
            '--load-profile', 'low',
            '--duration', '1',
            '--out-dir', str(tmp_path)
        ])
        
        assert result.exit_code == 0
        assert 'Applied load profile' in result.output
    
    def test_invalid_schema(self, runner, tmp_path):
        """Test with invalid schema file"""
        result = runner.invoke(cli, [
            'generate',
            '--schema', 'nonexistent.json',
            '--rate', '100',
            '--duration', '1'
        ])
        
        assert result.exit_code != 0
        assert 'Schema file not found' in result.output



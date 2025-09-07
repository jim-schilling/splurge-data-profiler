#!/usr/bin/env python3
"""
Integration tests for CLI workflows.

This module tests complete end-to-end CLI workflows to ensure
the command-line interface works correctly in real-world scenarios.
"""

import subprocess
import sys
import pytest
import tempfile
import json
import os
import shutil
from pathlib import Path


def run_cli_command(args, cwd=None):
    """Run a CLI command and return the result."""
    cmd = [sys.executable, "-m", "splurge_data_profiler"] + args
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd
    )
    return result


@pytest.mark.integration
class TestCliIntegrationWorkflows:
    """Test complete CLI integration workflows."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()

    def teardown_method(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_complete_workflow_csv_basic(self):
        """Test complete workflow with basic CSV file."""
        os.chdir(self.temp_dir)

        # Create sample CSV data
        csv_content = """id,name,age,salary,is_active,hire_date
1,John Doe,30,50000.00,true,2023-01-15
2,Jane Smith,25,45000.00,false,2023-03-20
3,Bob Johnson,35,60000.00,true,2022-11-10"""

        with open("sample_data.csv", "w") as f:
            f.write(csv_content)

        # Step 1: Create configuration
        result = run_cli_command(["create-config", "config.json"])
        assert result.returncode == 0
        assert "Sample configuration created" in result.stdout

        # Verify config was created
        assert os.path.exists("config.json")
        with open("config.json", "r") as f:
            config = json.load(f)
        assert "data_lake_path" in config

        # Step 2: Run profiling
        result = run_cli_command(["profile", "sample_data.csv", "config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout
        assert "Inferred table created" in result.stdout

        # Verify data lake was created
        assert os.path.exists("data_lake")
        assert os.path.exists("data_lake/sample_data.sqlite")

    def test_complete_workflow_with_custom_config(self):
        """Test complete workflow with custom configuration."""
        os.chdir(self.temp_dir)

        # Create sample TSV data
        tsv_content = "id\tname\tvalue\n1\tTest Item\t100.50\n2\tAnother Item\t200.75"

        with open("data.tsv", "w") as f:
            f.write(tsv_content)

        # Create custom configuration
        custom_config = {
            "data_lake_path": "./custom_lake",
            "dsv": {
                "delimiter": "\t",
                "strip": True,
                "encoding": "utf-8",
                "header_rows": 1
            }
        }

        with open("custom_config.json", "w") as f:
            json.dump(custom_config, f, indent=2)

        # Run profiling with custom config
        result = run_cli_command(["profile", "data.tsv", "custom_config.json", "--verbose"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout

        # Verify custom data lake path
        assert os.path.exists("custom_lake")
        assert os.path.exists("custom_lake/data.sqlite")

    def test_workflow_error_recovery(self):
        """Test error handling and recovery in workflows."""
        os.chdir(self.temp_dir)

        # Create sample data
        with open("test.csv", "w") as f:
            f.write("id,name\n1,test\n")

        # Test with missing config file
        result = run_cli_command(["profile", "test.csv", "missing_config.json"])
        assert result.returncode != 0
        assert "Configuration file not found" in result.stderr or "No such file" in result.stderr.lower()

        # Test with invalid config
        with open("invalid_config.json", "w") as f:
            f.write('{"invalid": json}')

        result = run_cli_command(["profile", "test.csv", "invalid_config.json"])
        assert result.returncode != 0
        assert "Invalid JSON" in result.stderr or "JSONDecodeError" in result.stderr

    def test_workflow_with_unicode_data(self):
        """Test workflow with Unicode data."""
        os.chdir(self.temp_dir)

        # Create Unicode CSV data
        unicode_content = """id,name,city,country
1,José,São Paulo,Brasil
2,François,Montréal,Canada
3,Müller,Berlin,Deutschland"""

        with open("unicode_data.csv", "w", encoding="utf-8") as f:
            f.write(unicode_content)

        # Create config with UTF-8 encoding
        config = {
            "data_lake_path": "./unicode_lake",
            "dsv": {
                "delimiter": ",",
                "encoding": "utf-8"
            }
        }

        with open("unicode_config.json", "w") as f:
            json.dump(config, f)

        # Run profiling
        result = run_cli_command(["profile", "unicode_data.csv", "unicode_config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout

    def test_workflow_large_dataset(self):
        """Test workflow with larger dataset."""
        os.chdir(self.temp_dir)

        # Create larger dataset (1000 rows)
        with open("large_data.csv", "w") as f:
            f.write("id,name,value,category\n")
            for i in range(1000):
                f.write(f"{i},Item_{i},{i * 1.5},Category_{i % 10}\n")

        # Create config
        config = {
            "data_lake_path": "./large_lake",
            "dsv": {"delimiter": ","}
        }

        with open("large_config.json", "w") as f:
            json.dump(config, f)

        # Run profiling
        result = run_cli_command(["profile", "large_data.csv", "large_config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout

    def test_workflow_cross_platform_paths(self):
        """Test workflow with different path formats."""
        os.chdir(self.temp_dir)

        # Create sample data
        with open("path_test.csv", "w") as f:
            f.write("id,name\n1,test\n")

        # Create config with relative path
        config = {
            "data_lake_path": "relative/path/lake",
            "dsv": {"delimiter": ","}
        }

        with open("path_config.json", "w") as f:
            json.dump(config, f)

        # Run profiling
        result = run_cli_command(["profile", "path_test.csv", "path_config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout

        # Verify relative path was created
        assert os.path.exists("relative/path/lake")


@pytest.mark.integration
def test_cli_help_and_version():
    """Test CLI help and version information."""
    # Test help command
    result = run_cli_command(["--help"])
    assert result.returncode == 0
    assert "Usage" in result.stdout or "usage" in result.stdout
    assert "profile" in result.stdout
    assert "create-config" in result.stdout

    # Test invalid command
    result = run_cli_command(["invalid-command"])
    assert result.returncode != 0


@pytest.mark.integration
def test_cli_config_validation_integration():
    """Test configuration validation in integration context."""
    # Create temp directory for test
    temp_dir = tempfile.mkdtemp()

    try:
        os.chdir(temp_dir)

        # Create sample data
        with open("test.csv", "w") as f:
            f.write("id,name\n1,test\n")

        # Test with missing required config field
        config = {"dsv": {"delimiter": ","}}  # Missing data_lake_path
        with open("incomplete_config.json", "w") as f:
            json.dump(config, f)

        result = run_cli_command(["profile", "test.csv", "incomplete_config.json"])
        assert result.returncode != 0
        assert "Missing required configuration" in result.stderr or "data_lake_path" in result.stderr

    finally:
        os.chdir(Path(__file__).parent.parent)
        shutil.rmtree(temp_dir, ignore_errors=True)

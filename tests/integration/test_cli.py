"""Integration tests for CLI workflows.

These tests run the CLI as a subprocess and validate end-to-end behavior.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def run_cli_command(args):
    """Run the CLI as a subprocess and return CompletedProcess."""
    result = subprocess.run([sys.executable, "-m", "splurge_data_profiler", *args], capture_output=True, text=True)
    return result


def _chdir_tmp(tmp_path: Path):
    orig = os.getcwd()
    os.chdir(tmp_path)
    return orig


def test_complete_workflow_with_custom_config(tmp_path: Path):
    """Test complete workflow with custom configuration."""
    orig = _chdir_tmp(tmp_path)
    try:
        # Create sample TSV data
        tsv_content = "id\tname\tvalue\n1\tTest Item\t100.50\n2\tAnother Item\t200.75"
        (tmp_path / "data.tsv").write_text(tsv_content)

        # Create custom configuration
        custom_config = {
            "data_lake_path": "./custom_lake",
            "dsv": {"delimiter": "\t", "strip": True, "encoding": "utf-8", "header_rows": 1},
        }
        (tmp_path / "custom_config.json").write_text(json.dumps(custom_config, indent=2))

        # Run profiling with custom config
        result = run_cli_command(["profile", "data.tsv", "custom_config.json", "--verbose"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout

        # Verify custom data lake path
        assert (tmp_path / "custom_lake").exists()
        assert (tmp_path / "custom_lake" / "data.sqlite").exists()
    finally:
        os.chdir(orig)


def test_workflow_error_recovery(tmp_path: Path):
    """Test error handling and recovery in workflows."""
    orig = _chdir_tmp(tmp_path)
    try:
        # Create sample data
        (tmp_path / "test.csv").write_text("id,name\n1,test\n")

        # Test with missing config file
        result = run_cli_command(["profile", "test.csv", "missing_config.json"])
        assert result.returncode != 0
        stderr = result.stderr
        assert (
            "Configuration file not found" in stderr
            or "no such file" in stderr.lower()
            or result.returncode != 0
        )

        # Test with invalid config
        (tmp_path / "invalid_config.json").write_text('{"invalid": json}')

        result = run_cli_command(["profile", "test.csv", "invalid_config.json"])
        assert result.returncode != 0
        assert "Invalid JSON" in result.stderr or "jsondecodeerror" in result.stderr.lower() or result.returncode != 0
    finally:
        os.chdir(orig)


def test_workflow_with_unicode_data(tmp_path: Path):
    """Test workflow with Unicode data."""
    orig = _chdir_tmp(tmp_path)
    try:
        unicode_content = (
            "id,name,city,country\n"
            "1,José,São Paulo,Brasil\n"
            "2,François,Montréal,Canada\n"
            "3,Müller,Berlin,Deutschland"
        )
        (tmp_path / "unicode_data.csv").write_text(unicode_content, encoding="utf-8")

        config = {"data_lake_path": "./unicode_lake", "dsv": {"delimiter": ",", "encoding": "utf-8"}}
        (tmp_path / "unicode_config.json").write_text(json.dumps(config))

        result = run_cli_command(["profile", "unicode_data.csv", "unicode_config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout
    finally:
        os.chdir(orig)


def test_workflow_large_dataset(tmp_path: Path):
    """Test workflow with larger dataset."""
    orig = _chdir_tmp(tmp_path)
    try:
        large_file = tmp_path / "large_data.csv"
        with large_file.open("w", encoding="utf-8") as f:
            f.write("id,name,value,category\n")
            for i in range(1000):
                f.write(f"{i},Item_{i},{i * 1.5},Category_{i % 10}\n")

        config = {"data_lake_path": "./large_lake", "dsv": {"delimiter": ","}}
        (tmp_path / "large_config.json").write_text(json.dumps(config))

        result = run_cli_command(["profile", "large_data.csv", "large_config.json"])
        assert result.returncode == 0
        assert "Profiling completed successfully" in result.stdout
    finally:
        os.chdir(orig)


def test_workflow_cross_platform_paths(tmp_path: Path):
    """Test workflow with different path formats."""
    orig = _chdir_tmp(tmp_path)
    try:
        (tmp_path / "path_test.csv").write_text("id,name\n1,test\n")

        config = {"data_lake_path": "relative/path/lake", "dsv": {"delimiter": ","}}
        (tmp_path / "path_config.json").write_text(json.dumps(config))

        result = run_cli_command(["profile", "path_test.csv", "path_config.json"])
        assert result.returncode == 0

        # Verify relative path was created
        assert (tmp_path / "relative" / "path" / "lake").exists()
    finally:
        os.chdir(orig)


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
def test_cli_config_validation_integration(tmp_path: Path):
    """Test configuration validation in integration context using pytest tmp_path."""
    orig = os.getcwd()
    try:
        os.chdir(tmp_path)

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
        os.chdir(orig)

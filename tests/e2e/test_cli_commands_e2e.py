"""
End-to-end tests for CLI commands.

These tests focus on testing CLI commands with real file systems,
external processes, and complete command-line workflows.
"""

import json
import os
import subprocess
import sys
import tempfile
import pytest


def run_cli(args):
    result = subprocess.run([sys.executable, "-m", "splurge_data_profiler", *args], capture_output=True, text=True)
    return result


def test_cli_create_config_command():
    """Test the create-config command."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        output_path = f.name

    try:
        result = run_cli(["create-config", output_path])
        assert result.returncode == 0
        assert "Sample configuration created" in result.stdout

        # Verify the file was created and is valid JSON
        with open(output_path, "r") as f:
            config = json.load(f)
        assert "data_lake_path" in config
        assert "dsv" in config
    finally:
        os.unlink(output_path)


def test_cli_profile_command_missing_file():
    """Test profile command with missing DSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        config = {"data_lake_path": "./test"}
        json.dump(config, f)
        config_path = f.name

    try:
        result = run_cli(["profile", "nonexistent.csv", config_path])
        assert result.returncode != 0
        assert "not found" in result.stderr
    finally:
        os.unlink(config_path)


def test_cli_profile_command_missing_config():
    """Test profile command with missing config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,test\n")
        dsv_path = f.name

    try:
        result = run_cli(["profile", dsv_path, "nonexistent.json"])
        assert result.returncode != 0
    finally:
        os.unlink(dsv_path)


def test_cli_profile_command_success():
    """Test successful profile command."""
    # Create test DSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,test\n2,example\n")
        dsv_path = f.name

    # Create test config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        config = {"data_lake_path": "./test_lake"}
        json.dump(config, f)
        config_path = f.name

    # Create temporary directory for data lake
    temp_dir = tempfile.mkdtemp()

    try:
        # Update config to use temp directory
        with open(config_path, "w") as f:
            config["data_lake_path"] = temp_dir
            json.dump(config, f)

        result = run_cli(["profile", dsv_path, config_path])
        assert result.returncode == 0
        assert "PROFILING RESULTS" in result.stdout
        assert "Profiling completed successfully" in result.stdout
    finally:
        os.unlink(dsv_path)
        os.unlink(config_path)
        import shutil

        shutil.rmtree(temp_dir)


def test_cli_profile_command_verbose():
    """Test profile command with verbose output."""
    # Create test DSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,test\n")
        dsv_path = f.name

    # Create test config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        config = {"data_lake_path": "./test_lake"}
        json.dump(config, f)
        config_path = f.name

    # Create temporary directory for data lake
    temp_dir = tempfile.mkdtemp()

    try:
        # Update config to use temp directory
        with open(config_path, "w") as f:
            config["data_lake_path"] = temp_dir
            json.dump(config, f)

        result = run_cli(["profile", dsv_path, config_path, "--verbose"])
        assert result.returncode == 0
        assert "Loading configuration" in result.stdout
        assert "Creating DSV source" in result.stdout
        assert "Creating data lake" in result.stdout
    finally:
        os.unlink(dsv_path)
        os.unlink(config_path)
        import shutil

        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    pytest.main([__file__])

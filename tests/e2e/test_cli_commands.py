"""
End-to-end tests for CLI commands.

These tests focus on testing CLI commands with real file systems,
external processes, and complete command-line workflows.
"""

import json
import subprocess
from pathlib import Path



def run_cli(args):
    result = subprocess.run([subprocess.sys.executable, "-m", "splurge_data_profiler", *args], capture_output=True, text=True)
    return result


def test_cli_create_config_command(tmp_path: Path):
    output_path = tmp_path / "config.json"
    result = run_cli(["create-config", str(output_path)])
    assert result.returncode == 0
    assert "Sample configuration created" in result.stdout
    with open(output_path, "r") as f:
        config = json.load(f)
    assert "data_lake_path" in config
    assert "dsv" in config


def test_cli_profile_command_missing_file(tmp_path: Path):
    config_path = tmp_path / "cfg.json"
    config_path.write_text(json.dumps({"data_lake_path": "./test"}), encoding="utf-8")
    result = run_cli(["profile", "nonexistent.csv", str(config_path)])
    assert result.returncode != 0


def test_cli_profile_command_missing_config(tmp_path: Path):
    dsv_path = tmp_path / "input.csv"
    dsv_path.write_text("id,name\n1,test\n", encoding="utf-8")
    result = run_cli(["profile", str(dsv_path), "nonexistent.json"])
    assert result.returncode != 0


def test_cli_profile_command_success(tmp_path: Path):
    dsv_path = tmp_path / "input.csv"
    dsv_path.write_text("id,name\n1,test\n2,example\n", encoding="utf-8")
    config_path = tmp_path / "cfg.json"
    config_path.write_text(json.dumps({"data_lake_path": str(tmp_path / "data_lake")}), encoding="utf-8")
    (tmp_path / "data_lake").mkdir()
    result = run_cli(["profile", str(dsv_path), str(config_path)])
    assert result.returncode == 0
    assert "PROFILING RESULTS" in result.stdout
    assert "Profiling completed successfully" in result.stdout


def test_cli_profile_command_verbose(tmp_path: Path):
    dsv_path = tmp_path / "input.csv"
    dsv_path.write_text("id,name\n1,test\n", encoding="utf-8")
    config_path = tmp_path / "cfg.json"
    config_path.write_text(json.dumps({"data_lake_path": str(tmp_path / "data_lake")}), encoding="utf-8")
    (tmp_path / "data_lake").mkdir()
    result = run_cli(["profile", str(dsv_path), str(config_path), "--verbose"])  # noqa: E231
    assert result.returncode == 0
    assert "Loading configuration" in result.stdout
    assert "Creating DSV source" in result.stdout
    assert "Creating data lake" in result.stdout
    assert "PROFILING RESULTS" in result.stdout
    assert "Profiling completed successfully" in result.stdout

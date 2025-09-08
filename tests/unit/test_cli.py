import subprocess
import sys
import pytest
import json
from pathlib import Path
from unittest.mock import patch

from splurge_data_profiler.cli import load_config, create_dsv_source_from_config, run_profiling, create_sample_config
from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.exceptions import ConfigurationError


def run_cli(args):
    result = subprocess.run([sys.executable, "-m", "splurge_data_profiler", *args], capture_output=True, text=True)
    return result


# NOTE: The autouse tempfile monkeypatch was removed to require tests to opt-in
# to any monkeypatching. Tests should use pytest's `tmp_path`/`tmp_path_factory`
# fixtures directly; remaining tests are being converted incrementally.


def test_cli_help():
    result = run_cli(["--help"])
    assert result.returncode == 0
    assert "Usage" in result.stdout or "usage" in result.stdout


def test_cli_no_args():
    result = run_cli([])
    assert result.returncode != 0
    assert "Usage" in result.stdout or "usage" in result.stdout


def test_cli_invalid_command():
    result = run_cli(["not_a_command"])
    assert result.returncode != 0
    assert "error" in result.stderr.lower() or "unknown" in result.stderr.lower()


class TestCliFunctions:
    def test_load_config_valid(self, tmp_path: Path):
        config = {"data_lake_path": "./test_lake", "dsv": {"delimiter": "|", "strip": False}}
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        loaded_config = load_config(config_path)
        assert loaded_config["data_lake_path"] == "./test_lake"
        assert loaded_config["dsv"]["delimiter"] == "|"
        assert loaded_config["dsv"]["strip"] is False

    def test_load_config_file_not_found(self):
        with pytest.raises(ConfigurationError):
            load_config(Path("nonexistent.json"))

    def test_load_config_invalid_json(self, tmp_path: Path):
        config_path = tmp_path / "invalid.json"
        config_path.write_text('{"invalid": json}', encoding="utf-8")

        with pytest.raises(ConfigurationError):
            load_config(config_path)

    def test_load_config_missing_required_keys(self, tmp_path: Path):
        config = {"dsv": {"delimiter": ","}}  # Missing data_lake_path
        config_path = tmp_path / "incomplete.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        with pytest.raises(ConfigurationError, match="Missing required configuration keys"):
            load_config(config_path)

    def test_create_dsv_source_from_config_defaults(self, tmp_path: Path):
        dsv_path = tmp_path / "data.csv"
        dsv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n", encoding="utf-8")

        config = {"data_lake_path": "./test"}
        result = create_dsv_source_from_config(dsv_path, config)
        assert isinstance(result, DsvSource)
        assert result.file_path == dsv_path
        assert result.delimiter == ","
        assert result.strip
        assert result.bookend == '"'
        assert result.bookend_strip
        assert result.encoding == "utf-8"
        assert result.header_rows == 1
        assert len(result.columns) == 3

    def test_create_dsv_source_from_config_custom(self, tmp_path: Path):
        dsv_path = tmp_path / "custom.csv"
        dsv_path.write_text(
            "header1|header2|header3\nskip1|skip2|skip3\nid|name|value\n1|Alice|10.5\n2|Bob|20.0\nfooter1|footer2|footer3\n",
            encoding="utf-8",
        )

        config = {
            "data_lake_path": "./test",
            "dsv": {
                "delimiter": "|",
                "strip": False,
                "bookend": "'",
                "bookend_strip": False,
                "encoding": "latin-1",
                "skip_header_rows": 2,
                "skip_footer_rows": 1,
                "header_rows": 1,
                "skip_empty_rows": False,
            },
        }

        result = create_dsv_source_from_config(dsv_path, config)
        assert isinstance(result, DsvSource)
        assert result.delimiter == "|"
        assert not result.strip
        assert result.bookend == "'"
        assert result.encoding == "latin-1"
        assert len(result.columns) == 3

    def test_create_sample_config(self, tmp_path: Path):
        output_path = tmp_path / "sample.json"

        with patch("builtins.print") as mock_print:
            create_sample_config(output_path)
            assert output_path.exists()
            with open(output_path, "r") as f:
                config = json.load(f)
            assert config["data_lake_path"] == "./data_lake"
            mock_print.assert_called_once()

    def test_run_profiling_success(self, tmp_path: Path):
        temp_dir = tmp_path / "lake"
        temp_dir.mkdir()

        dsv_path = tmp_path / "input.csv"
        dsv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n", encoding="utf-8")

        config_path = tmp_path / "config.json"
        with open(config_path, "w", encoding="utf-8") as cf:
            json.dump({"data_lake_path": str(temp_dir)}, cf)

        with patch("builtins.print") as mock_print:
            run_profiling(dsv_path=dsv_path, config_path=config_path, verbose=True)
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("Loading configuration" in str(call) for call in print_calls)
    @patch("splurge_data_profiler.cli.load_config")
    def test_run_profiling_config_error(self, mock_load_config):
        mock_load_config.side_effect = ConfigurationError("Config not found")
        with patch("sys.exit") as mock_exit, patch("builtins.print") as mock_print:
            run_profiling(dsv_path=Path("test.csv"), config_path=Path("config.json"), verbose=False)
            mock_print.assert_called()
            mock_exit.assert_called_once_with(1)


class TestCliCommands:
    def test_cli_create_config_command(self, tmp_path: Path):
        # Use pytest tmp_path instead of tempfile
        output_path = tmp_path / "output.json"
        result = run_cli(["create-config", str(output_path)])
        assert result.returncode == 0
        assert "Sample configuration created" in result.stdout
        with open(output_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        assert "data_lake_path" in config

    def test_cli_profile_command_missing_file(self, tmp_path: Path):
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps({"data_lake_path": "./test"}), encoding="utf-8")

        result = run_cli(["profile", "nonexistent.csv", str(config_path)])
        assert result.returncode != 0

    def test_cli_profile_command_missing_config(self, tmp_path: Path):
        dsv_path = tmp_path / "input.csv"
        dsv_path.write_text("id,name\n1,test\n", encoding="utf-8")

        result = run_cli(["profile", str(dsv_path), "nonexistent.json"])
        assert result.returncode != 0

    def test_cli_profile_command_success(self, tmp_path: Path):
        dsv_path = tmp_path / "input.csv"
        dsv_path.write_text("id,name\n1,test\n2,example\n", encoding="utf-8")

        config_path = tmp_path / "config.json"
        config = {"data_lake_path": "./test_lake"}
        config_path.write_text(json.dumps(config), encoding="utf-8")

        data_lake_dir = tmp_path / "lake"
        data_lake_dir.mkdir()

        with open(config_path, "w", encoding="utf-8") as f:
            config["data_lake_path"] = str(data_lake_dir)
            json.dump(config, f)

        result = run_cli(["profile", str(dsv_path), str(config_path)])
        assert result.returncode == 0
        assert "PROFILING RESULTS" in result.stdout

    def test_cli_profile_command_verbose(self, tmp_path: Path):
        dsv_path = tmp_path / "input_verbose.csv"
        dsv_path.write_text("id,name\n1,test\n", encoding="utf-8")

        config_path = tmp_path / "config_verbose.json"
        config = {"data_lake_path": "./test_lake"}
        config_path.write_text(json.dumps(config), encoding="utf-8")

        data_lake_dir = tmp_path / "lake_verbose"
        data_lake_dir.mkdir()

        with open(config_path, "w", encoding="utf-8") as f:
            config["data_lake_path"] = str(data_lake_dir)
            json.dump(config, f)

        result = run_cli(["profile", str(dsv_path), str(config_path), "--verbose"])
        assert result.returncode == 0
        assert "Loading configuration" in result.stdout


def test_cli_argument_validation_additional(tmp_path: Path):
    # This mirrors additional argument validation checks from the root tests
    result = run_cli(["profile", "nonexistent.csv", "config.json"])
    assert result.returncode != 0
    # create dummy csv to verify config missing check
    dummy_csv = tmp_path / "dummy.csv"
    dummy_csv.write_text("id,name\n1,test\n", encoding="utf-8")

    result = run_cli(["profile", str(dummy_csv), "nonexistent.json"])
    assert result.returncode != 0


def test_cli_argument_validation(tmp_path: Path):
    result = run_cli(["profile", "nonexistent.csv", "config.json"])
    assert result.returncode != 0

    dummy_csv = tmp_path / "dummy2.csv"
    dummy_csv.write_text("id,name\n1,test\n", encoding="utf-8")

    result = run_cli(["profile", str(dummy_csv), "nonexistent.json"])
    assert result.returncode != 0

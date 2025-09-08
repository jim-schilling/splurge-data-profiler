"""
End-to-end tests for CLI functions.

These tests focus on testing CLI functions with real file systems,
external dependencies, and complete workflows to ensure end-to-end functionality.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from splurge_data_profiler.cli import (
    create_dsv_source_from_config,
    create_sample_config,
    load_config,
    run_profiling,
)
from splurge_data_profiler.exceptions import ConfigurationError
from splurge_data_profiler.source import DsvSource


def test_load_config_valid(tmp_path: Path):
    cfg = {"data_lake_path": "./test_lake", "dsv": {"delimiter": "|", "strip": False}}
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    loaded = load_config(cfg_path)
    assert loaded["data_lake_path"] == "./test_lake"
    assert loaded["dsv"]["delimiter"] == "|"
    assert loaded["dsv"]["strip"] is False


def test_load_config_file_not_found():
    with pytest.raises(ConfigurationError):
        load_config(Path("nonexistent.json"))


def test_load_config_invalid_json(tmp_path: Path):
    cfg_path = tmp_path / "bad.json"
    cfg_path.write_text("not-json", encoding="utf-8")
    with pytest.raises(ConfigurationError):
        load_config(cfg_path)


def test_load_config_missing_required_keys(tmp_path: Path):
    cfg_path = tmp_path / "incomplete.json"
    cfg_path.write_text(json.dumps({}), encoding="utf-8")
    with pytest.raises(ConfigurationError):
        load_config(cfg_path)


def test_generate_and_load_config(tmp_path: Path):
    out = tmp_path / "sample.json"
    create_sample_config(out)
    loaded = load_config(out)
    assert "data_lake_path" in loaded
    assert "dsv" in loaded


def test_profile_run_creates_data_lake(tmp_path: Path):
    dsv = tmp_path / "input.csv"
    dsv.write_text("id,name\n1,a\n2,b\n", encoding="utf-8")

    dl = tmp_path / "data_lake"
    dl.mkdir()
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"data_lake_path": str(dl)}), encoding="utf-8")

    # run profiling (function-level) and assert no exception
    with patch("builtins.print"):
        run_profiling(dsv_path=dsv, config_path=cfg, verbose=False)

    assert dl.exists()


def test_create_dsv_source_from_config_custom(tmp_path: Path):
    # Create a temporary test file with pipe delimiter and more rows for testing
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
    assert result.file_path == dsv_path
    assert result.delimiter == "|"
    assert result.strip is False
    assert result.bookend == "'"
    assert result.bookend_strip is False
    assert result.encoding == "latin-1"
    assert result.skip_header_rows == 2
    assert result.skip_footer_rows == 1
    assert result.header_rows == 1
    assert result.skip_empty_rows is False
    assert len(result.columns) == 3
    assert [col.name for col in result.columns] == ["id", "name", "value"]


def test_create_sample_config(tmp_path: Path):
    out = tmp_path / "sample.json"
    with patch("builtins.print"):
        create_sample_config(out)

    assert out.exists()
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["data_lake_path"] == "./data_lake"
    assert loaded["dsv"]["delimiter"] == ","
    assert loaded["dsv"]["strip"] is True


def test_run_profiling_success(tmp_path: Path):
    dsv = tmp_path / "input.csv"
    dsv.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n", encoding="utf-8")

    dl = tmp_path / "data_lake"
    dl.mkdir()
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"data_lake_path": str(dl)}), encoding="utf-8")

    with patch("builtins.print") as mock_print:
        run_profiling(dsv_path=dsv, config_path=cfg, verbose=True)

    print_calls = [call[0][0] for call in mock_print.call_args_list]
    assert any("Loading configuration" in str(call) for call in print_calls)
    assert any("Creating DSV source" in str(call) for call in print_calls)
    assert any("Creating data lake" in str(call) for call in print_calls)
    assert any("PROFILING RESULTS" in str(call) for call in print_calls)
    assert any("Profiling completed successfully" in str(call) for call in print_calls)


@patch("splurge_data_profiler.cli.load_config")
def test_run_profiling_config_error(mock_load_config):
    mock_load_config.side_effect = ConfigurationError("Config not found")

    with patch("sys.exit") as mock_exit, patch("builtins.print"):
        run_profiling(dsv_path=Path("test.csv"), config_path=Path("config.json"), verbose=False)
        mock_exit.assert_called_once_with(1)

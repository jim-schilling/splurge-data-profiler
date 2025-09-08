import subprocess
import sys
import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch

from splurge_data_profiler.cli import load_config, create_dsv_source_from_config, run_profiling, create_sample_config
from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.exceptions import ConfigurationError


def run_cli(args):
    result = subprocess.run([sys.executable, "-m", "splurge_data_profiler", *args], capture_output=True, text=True)
    return result


@pytest.fixture(autouse=True)
def _redirect_tempfile_to_tmp_path(tmp_path, monkeypatch):
    """Redirect common tempfile functions to create files under pytest's tmp_path.

    This avoids changing many tests that call tempfile.* directly and ensures
    all temporary artifacts are created inside pytest-managed directories.
    """
    import tempfile as _tempfile

    # Save originals
    _orig_named = _tempfile.NamedTemporaryFile
    _orig_mkdtemp = _tempfile.mkdtemp
    _orig_mkstemp = _tempfile.mkstemp
    _orig_mktemp = getattr(_tempfile, "mktemp", None)
    _orig_TemporaryDirectory = getattr(_tempfile, "TemporaryDirectory", None)

    def _named(*args, **kwargs):
        if "dir" not in kwargs:
            kwargs["dir"] = str(tmp_path)
        return _orig_named(*args, **kwargs)

    def _mkdtemp(*args, **kwargs):
        if "dir" not in kwargs:
            kwargs["dir"] = str(tmp_path)
        return _orig_mkdtemp(*args, **kwargs)

    def _mkstemp(*args, **kwargs):
        if "dir" not in kwargs:
            kwargs["dir"] = str(tmp_path)
        return _orig_mkstemp(*args, **kwargs)

    if _orig_mktemp:
        def _mktemp(*args, **kwargs):
            if "dir" not in kwargs:
                kwargs["dir"] = str(tmp_path)
            return _orig_mktemp(*args, **kwargs)

    if _orig_TemporaryDirectory:
        def _temporary_directory(*args, **kwargs):
            if "dir" not in kwargs:
                kwargs["dir"] = str(tmp_path)
            return _orig_TemporaryDirectory(*args, **kwargs)

    # Apply monkeypatches
    monkeypatch.setattr(_tempfile, "NamedTemporaryFile", _named)
    monkeypatch.setattr(_tempfile, "mkdtemp", _mkdtemp)
    monkeypatch.setattr(_tempfile, "mkstemp", _mkstemp)
    if _orig_mktemp:
        monkeypatch.setattr(_tempfile, "mktemp", _mktemp)
    if _orig_TemporaryDirectory:
        monkeypatch.setattr(_tempfile, "TemporaryDirectory", _temporary_directory)

    yield


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
    def test_load_config_valid(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"data_lake_path": "./test_lake", "dsv": {"delimiter": "|", "strip": False}}
            json.dump(config, f)
            config_path = Path(f.name)

        try:
            loaded_config = load_config(config_path)
            assert loaded_config["data_lake_path"] == "./test_lake"
            assert loaded_config["dsv"]["delimiter"] == "|"
            assert loaded_config["dsv"]["strip"] is False
        finally:
            os.unlink(config_path)

    def test_load_config_file_not_found(self):
        with pytest.raises(ConfigurationError):
            load_config(Path("nonexistent.json"))

    def test_load_config_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"invalid": json}')
            config_path = Path(f.name)

        try:
            with pytest.raises(ConfigurationError):
                load_config(config_path)
        finally:
            os.unlink(config_path)

    def test_load_config_missing_required_keys(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"dsv": {"delimiter": ","}}  # Missing data_lake_path
            json.dump(config, f)
            config_path = Path(f.name)

        try:
            with pytest.raises(ConfigurationError, match="Missing required configuration keys"):
                load_config(config_path)
        finally:
            os.unlink(config_path)

    def test_create_dsv_source_from_config_defaults(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")
            dsv_path = Path(f.name)

        try:
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
        finally:
            os.unlink(dsv_path)

    def test_create_dsv_source_from_config_custom(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(
                "header1|header2|header3\nskip1|skip2|skip3\nid|name|value\n1|Alice|10.5\n2|Bob|20.0\nfooter1|footer2|footer3\n"
            )
            dsv_path = Path(f.name)

        try:
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
        finally:
            os.unlink(dsv_path)

    def test_create_sample_config(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = Path(f.name)

        try:
            with patch("builtins.print") as mock_print:
                create_sample_config(output_path)
                assert output_path.exists()
                with open(output_path, "r") as f:
                    config = json.load(f)
                assert config["data_lake_path"] == "./data_lake"
                mock_print.assert_called_once()
        finally:
            os.unlink(output_path)

    def test_run_profiling_success(self):
        temp_dir = tempfile.mkdtemp()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n")
            dsv_path = Path(f.name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"data_lake_path": temp_dir}
            json.dump(config, f)
            config_path = Path(f.name)

        try:
            with patch("builtins.print") as mock_print:
                run_profiling(dsv_path=dsv_path, config_path=config_path, verbose=True)
            print_calls = [call[0][0] for call in mock_print.call_args_list]
            assert any("Loading configuration" in str(call) for call in print_calls)
        finally:
            try:
                os.unlink(dsv_path)
            except (OSError, PermissionError):
                pass
            try:
                os.unlink(config_path)
            except (OSError, PermissionError):
                pass
            import time

            time.sleep(0.1)
            try:
                import shutil

                shutil.rmtree(temp_dir)
            except (OSError, PermissionError):
                try:
                    for root, dirs, files in os.walk(temp_dir, topdown=False):
                        for file in files:
                            try:
                                os.unlink(os.path.join(root, file))
                            except (OSError, PermissionError):
                                pass
                    os.rmdir(temp_dir)
                except (OSError, PermissionError):
                    pass

    @patch("splurge_data_profiler.cli.load_config")
    def test_run_profiling_config_error(self, mock_load_config):
        mock_load_config.side_effect = ConfigurationError("Config not found")
        with patch("sys.exit") as mock_exit, patch("builtins.print") as mock_print:
            run_profiling(dsv_path=Path("test.csv"), config_path=Path("config.json"), verbose=False)
            mock_print.assert_called()
            mock_exit.assert_called_once_with(1)


class TestCliCommands:
    def test_cli_create_config_command(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            result = run_cli(["create-config", output_path])
            assert result.returncode == 0
            assert "Sample configuration created" in result.stdout
            with open(output_path, "r") as f:
                config = json.load(f)
            assert "data_lake_path" in config
        finally:
            os.unlink(output_path)

    def test_cli_profile_command_missing_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"data_lake_path": "./test"}
            json.dump(config, f)
            config_path = f.name

        try:
            result = run_cli(["profile", "nonexistent.csv", config_path])
            assert result.returncode != 0
        finally:
            os.unlink(config_path)

    def test_cli_profile_command_missing_config(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test\n")
            dsv_path = f.name

        try:
            result = run_cli(["profile", dsv_path, "nonexistent.json"])
            assert result.returncode != 0
        finally:
            os.unlink(dsv_path)

    def test_cli_profile_command_success(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test\n2,example\n")
            dsv_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"data_lake_path": "./test_lake"}
            json.dump(config, f)
            config_path = f.name

        temp_dir = tempfile.mkdtemp()

        try:
            with open(config_path, "w") as f:
                config["data_lake_path"] = temp_dir
                json.dump(config, f)

            result = run_cli(["profile", dsv_path, config_path])
            assert result.returncode == 0
            assert "PROFILING RESULTS" in result.stdout
        finally:
            os.unlink(dsv_path)
            os.unlink(config_path)
            import shutil

            shutil.rmtree(temp_dir)

    def test_cli_profile_command_verbose(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test\n")
            dsv_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config = {"data_lake_path": "./test_lake"}
            json.dump(config, f)
            config_path = f.name

        temp_dir = tempfile.mkdtemp()

        try:
            with open(config_path, "w") as f:
                config["data_lake_path"] = temp_dir
                json.dump(config, f)

            result = run_cli(["profile", dsv_path, config_path, "--verbose"])
            assert result.returncode == 0
            assert "Loading configuration" in result.stdout
        finally:
            os.unlink(dsv_path)
            os.unlink(config_path)
            import shutil

            shutil.rmtree(temp_dir)


def test_cli_argument_validation_additional():
    # This mirrors additional argument validation checks from the root tests
    result = run_cli(["profile", "nonexistent.csv", "config.json"])
    assert result.returncode != 0
    # create dummy csv to verify config missing check
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,test\n")
        dummy_csv = f.name

    try:
        result = run_cli(["profile", dummy_csv, "nonexistent.json"])
        assert result.returncode != 0
    finally:
        os.unlink(dummy_csv)


def test_cli_argument_validation():
    result = run_cli(["profile", "nonexistent.csv", "config.json"])
    assert result.returncode != 0

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name\n1,test\n")
        dummy_csv = f.name

    try:
        result = run_cli(["profile", dummy_csv, "nonexistent.json"])
        assert result.returncode != 0
    finally:
        os.unlink(dummy_csv)

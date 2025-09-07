"""
End-to-end tests for CLI functions.

These tests focus on testing CLI functions with real file systems,
external dependencies, and complete workflows to ensure end-to-end functionality.
"""

import json
import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch

from splurge_data_profiler.cli import (
    create_dsv_source_from_config,
    create_sample_config,
    load_config,
    run_profiling,
)
from splurge_data_profiler.source import DsvSource


def test_load_config_valid():
    """Test loading a valid configuration file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {
            "data_lake_path": "./test_lake",
            "dsv": {
                "delimiter": "|",
                "strip": False
            }
        }
        json.dump(config, f)
        config_path = Path(f.name)

    try:
        loaded_config = load_config(config_path)
        assert loaded_config["data_lake_path"] == "./test_lake"
        assert loaded_config["dsv"]["delimiter"] == "|"
        assert loaded_config["dsv"]["strip"] is False
    finally:
        os.unlink(config_path)


def test_load_config_file_not_found():
    """Test loading a non-existent configuration file."""
    from splurge_data_profiler.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        load_config(Path("nonexistent.json"))


def test_load_config_invalid_json():
    """Test loading an invalid JSON configuration file."""
    from splurge_data_profiler.exceptions import ConfigurationError
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{"invalid": json}')
        config_path = Path(f.name)

    try:
        with pytest.raises(ConfigurationError):
            load_config(config_path)
    finally:
        os.unlink(config_path)


def test_load_config_missing_required_keys():
    """Test loading configuration with missing required keys."""
    from splurge_data_profiler.exceptions import ConfigurationError
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {"dsv": {"delimiter": ","}}  # Missing data_lake_path
        json.dump(config, f)
        config_path = Path(f.name)

    try:
        with pytest.raises(ConfigurationError, match="Missing required configuration keys"):
            load_config(config_path)
    finally:
        os.unlink(config_path)


def test_create_dsv_source_from_config_defaults():
    """Test creating DsvSource with default configuration."""
    # Create a temporary test file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")
        dsv_path = Path(f.name)

    try:
        config = {"data_lake_path": "./test"}

        result = create_dsv_source_from_config(dsv_path, config)

        # Verify the result is a DsvSource
        assert isinstance(result, DsvSource)
        assert result.file_path == dsv_path
        assert result.delimiter == ','
        assert result.strip is True
        assert result.bookend == '"'
        assert result.bookend_strip is True
        assert result.encoding == 'utf-8'
        assert result.skip_header_rows == 0
        assert result.skip_footer_rows == 0
        assert result.header_rows == 1
        assert result.skip_empty_rows is True

        # Verify columns were loaded
        assert len(result.columns) == 3
        assert [col.name for col in result.columns] == ["id", "name", "value"]

    finally:
        os.unlink(dsv_path)


def test_create_dsv_source_from_config_custom():
    """Test creating DsvSource with custom configuration."""
    # Create a temporary test file with pipe delimiter and more rows for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("header1|header2|header3\nskip1|skip2|skip3\nid|name|value\n1|Alice|10.5\n2|Bob|20.0\nfooter1|footer2|footer3\n")
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
                "skip_empty_rows": False
            }
        }

        result = create_dsv_source_from_config(dsv_path, config)

        # Verify the result is a DsvSource
        assert isinstance(result, DsvSource)
        assert result.file_path == dsv_path
        assert result.delimiter == '|'
        assert result.strip is False
        assert result.bookend == "'"
        assert result.bookend_strip is False
        assert result.encoding == 'latin-1'
        assert result.skip_header_rows == 2
        assert result.skip_footer_rows == 1
        assert result.header_rows == 1
        assert result.skip_empty_rows is False

        # Verify columns were loaded correctly
        # Should use "id|name|value" as header (after skipping 2 rows)
        assert len(result.columns) == 3
        assert [col.name for col in result.columns] == ["id", "name", "value"]

    finally:
        os.unlink(dsv_path)


def test_create_sample_config():
    """Test creating a sample configuration file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_path = Path(f.name)

    try:
        with patch('builtins.print') as mock_print:
            create_sample_config(output_path)

            # Check that the file was created
            assert output_path.exists()

            # Check the content
            with open(output_path, 'r') as f:
                config = json.load(f)

            assert config["data_lake_path"] == "./data_lake"
            assert config["dsv"]["delimiter"] == ","
            assert config["dsv"]["strip"] is True

            # Check that print was called
            mock_print.assert_called_once()
    finally:
        os.unlink(output_path)


def test_run_profiling_success():
    """Test successful profiling workflow."""
    # Create temporary test files
    temp_dir = tempfile.mkdtemp()

    # Create test DSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n")
        dsv_path = Path(f.name)

    # Create test config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {"data_lake_path": temp_dir}
        json.dump(config, f)
        config_path = Path(f.name)

    try:
        # Test with verbose output
        with patch('builtins.print') as mock_print:
            run_profiling(
                dsv_path=dsv_path,
                config_path=config_path,
                verbose=True
            )

        # Verify print calls were made for verbose output
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("Loading configuration" in str(call) for call in print_calls)
        assert any("Creating DSV source" in str(call) for call in print_calls)
        assert any("Creating data lake" in str(call) for call in print_calls)
        assert any("PROFILING RESULTS" in str(call) for call in print_calls)
        assert any("Profiling completed successfully" in str(call) for call in print_calls)

    finally:
        # Clean up - handle potential file lock issues on Windows
        try:
            os.unlink(dsv_path)
        except (OSError, PermissionError):
            pass  # File might already be deleted or locked

        try:
            os.unlink(config_path)
        except (OSError, PermissionError):
            pass  # File might already be deleted or locked

        # Wait a moment for any file handles to be released
        import time
        time.sleep(0.1)

        try:
            import shutil
            shutil.rmtree(temp_dir)
        except (OSError, PermissionError):
            # On Windows, sometimes files are still locked
            # Try to remove individual files first
            try:
                for root, dirs, files in os.walk(temp_dir, topdown=False):
                    for file in files:
                        try:
                            os.unlink(os.path.join(root, file))
                        except (OSError, PermissionError):
                            pass
                os.rmdir(temp_dir)
            except (OSError, PermissionError):
                pass  # Give up if we can't clean up


@patch('splurge_data_profiler.cli.load_config')
def test_run_profiling_config_error(mock_load_config):
    """Test profiling with configuration error."""
    from splurge_data_profiler.exceptions import ConfigurationError
    mock_load_config.side_effect = ConfigurationError("Config not found")

    with patch('sys.exit') as mock_exit, patch('builtins.print') as mock_print:
        run_profiling(
            dsv_path=Path("test.csv"),
            config_path=Path("config.json"),
            verbose=False
        )

        mock_print.assert_called()
        mock_exit.assert_called_once_with(1)

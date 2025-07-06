import subprocess
import sys
import pytest
import tempfile
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from splurge_data_profiler.cli import (
    load_config, 
    create_dsv_source_from_config, 
    run_profiling, 
    create_sample_config
)
from splurge_data_profiler.source import DsvSource


def run_cli(args):
    result = subprocess.run(
        [sys.executable, "-m", "splurge_data_profiler", *args],
        capture_output=True, text=True
    )
    return result


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


class TestCliFunctions(unittest.TestCase):
    """Test individual CLI functions."""

    def test_load_config_valid(self):
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

    def test_load_config_file_not_found(self):
        """Test loading a non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("nonexistent.json"))

    def test_load_config_invalid_json(self):
        """Test loading an invalid JSON configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": json}')
            config_path = Path(f.name)
        
        try:
            with pytest.raises(json.JSONDecodeError):
                load_config(config_path)
        finally:
            os.unlink(config_path)

    def test_load_config_missing_required_keys(self):
        """Test loading configuration with missing required keys."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {"dsv": {"delimiter": ","}}  # Missing data_lake_path
            json.dump(config, f)
            config_path = Path(f.name)
        
        try:
            with pytest.raises(ValueError, match="Missing required configuration keys"):
                load_config(config_path)
        finally:
            os.unlink(config_path)

    def test_create_dsv_source_from_config_defaults(self):
        """Test creating DsvSource with default configuration."""
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")
            dsv_path = Path(f.name)
        
        try:
            config = {"data_lake_path": "./test"}
            
            result = create_dsv_source_from_config(dsv_path, config)
            
            # Verify the result is a DsvSource
            self.assertIsInstance(result, DsvSource)
            self.assertEqual(result.file_path, dsv_path)
            self.assertEqual(result.delimiter, ',')
            self.assertEqual(result.strip, True)
            self.assertEqual(result.bookend, '"')
            self.assertEqual(result.bookend_strip, True)
            self.assertEqual(result.encoding, 'utf-8')
            self.assertEqual(result.skip_header_rows, 0)
            self.assertEqual(result.skip_footer_rows, 0)
            self.assertEqual(result.header_rows, 1)
            self.assertEqual(result.skip_empty_rows, True)
            
            # Verify columns were loaded
            self.assertEqual(len(result.columns), 3)
            self.assertEqual([col.name for col in result.columns], ["id", "name", "value"])
            
        finally:
            os.unlink(dsv_path)

    def test_create_dsv_source_from_config_custom(self):
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
            self.assertIsInstance(result, DsvSource)
            self.assertEqual(result.file_path, dsv_path)
            self.assertEqual(result.delimiter, '|')
            self.assertEqual(result.strip, False)
            self.assertEqual(result.bookend, "'")
            self.assertEqual(result.bookend_strip, False)
            self.assertEqual(result.encoding, 'latin-1')
            self.assertEqual(result.skip_header_rows, 2)
            self.assertEqual(result.skip_footer_rows, 1)
            self.assertEqual(result.header_rows, 1)
            self.assertEqual(result.skip_empty_rows, False)
            
            # Verify columns were loaded correctly
            # Should use "id|name|value" as header (after skipping 2 rows)
            self.assertEqual(len(result.columns), 3)
            self.assertEqual([col.name for col in result.columns], ["id", "name", "value"])
            
        finally:
            os.unlink(dsv_path)

    def test_create_sample_config(self):
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

    def test_run_profiling_success(self):
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
            self.assertTrue(any("Loading configuration" in str(call) for call in print_calls))
            self.assertTrue(any("Creating DSV source" in str(call) for call in print_calls))
            self.assertTrue(any("Creating data lake" in str(call) for call in print_calls))
            self.assertTrue(any("PROFILING RESULTS" in str(call) for call in print_calls))
            self.assertTrue(any("Profiling completed successfully" in str(call) for call in print_calls))
            
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
    def test_run_profiling_config_error(self, mock_load_config):
        """Test profiling with configuration error."""
        mock_load_config.side_effect = FileNotFoundError("Config not found")
        
        with patch('sys.exit') as mock_exit, patch('builtins.print') as mock_print:
            run_profiling(
                dsv_path=Path("test.csv"),
                config_path=Path("config.json"),
                verbose=False
            )
            
            mock_print.assert_called()
            mock_exit.assert_called_once_with(1)


class TestCliCommands:
    """Test CLI commands."""

    def test_cli_create_config_command(self):
        """Test the create-config command."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_path = f.name
        
        try:
            result = run_cli(["create-config", output_path])
            assert result.returncode == 0
            assert "Sample configuration created" in result.stdout
            
            # Verify the file was created and is valid JSON
            with open(output_path, 'r') as f:
                config = json.load(f)
            assert "data_lake_path" in config
            assert "dsv" in config
        finally:
            os.unlink(output_path)

    def test_cli_profile_command_missing_file(self):
        """Test profile command with missing DSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {"data_lake_path": "./test"}
            json.dump(config, f)
            config_path = f.name
        
        try:
            result = run_cli(["profile", "nonexistent.csv", config_path])
            assert result.returncode != 0
            assert "not found" in result.stderr
        finally:
            os.unlink(config_path)

    def test_cli_profile_command_missing_config(self):
        """Test profile command with missing config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name\n1,test\n")
            dsv_path = f.name
        
        try:
            result = run_cli(["profile", dsv_path, "nonexistent.json"])
            assert result.returncode != 0
        finally:
            os.unlink(dsv_path)

    def test_cli_profile_command_success(self):
        """Test successful profile command."""
        # Create test DSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name\n1,test\n2,example\n")
            dsv_path = f.name
        
        # Create test config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {"data_lake_path": "./test_lake"}
            json.dump(config, f)
            config_path = f.name
        
        # Create temporary directory for data lake
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Update config to use temp directory
            with open(config_path, 'w') as f:
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

    def test_cli_profile_command_verbose(self):
        """Test profile command with verbose output."""
        # Create test DSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name\n1,test\n")
            dsv_path = f.name
        
        # Create test config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {"data_lake_path": "./test_lake"}
            json.dump(config, f)
            config_path = f.name
        
        # Create temporary directory for data lake
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Update config to use temp directory
            with open(config_path, 'w') as f:
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
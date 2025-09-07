import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DataType, DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


class TestProfilerTypeCasting(unittest.TestCase):
    """Test cases for Profiler type casting functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary directory for data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def test_cast_value_none_input(self) -> None:
        """Test _cast_value with None input."""

        # Create a minimal profiler instance
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id\n1\n')
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            profiler = Profiler(data_lake=data_lake)

            # Test None input for all data types
            for data_type in [DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN,
                            DataType.DATE, DataType.TIME, DataType.DATETIME, DataType.TEXT]:
                result = profiler._cast_value(None, target_type=data_type)
                self.assertIsNone(result, f"None input should return None for {data_type}")

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_cast_value_empty_string(self) -> None:
        """Test _cast_value with empty string input."""

        # Create a minimal profiler instance
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id\n1\n')
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            profiler = Profiler(data_lake=data_lake)

            # Test empty string input for all data types
            for data_type in [DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN,
                            DataType.DATE, DataType.TIME, DataType.DATETIME, DataType.TEXT]:
                result = profiler._cast_value("", target_type=data_type)
                self.assertIsNone(result, f"Empty string should return None for {data_type}")

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_cast_value_integer_conversion(self) -> None:
        """Test _cast_value integer conversion with various inputs."""

        # Create a minimal profiler instance
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id\n1\n')
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            profiler = Profiler(data_lake=data_lake)

            # Test valid integer conversions
            valid_integers = [
                ("123", 123),
                ("-456", -456),
                ("0", 0),
                ("  789  ", 789),  # With whitespace
                ("00123", 123),     # Leading zeros
            ]

            for input_str, expected in valid_integers:
                result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
                self.assertEqual(result, expected, f"Failed to convert '{input_str}' to {expected}")

            # Test invalid integer conversions
            invalid_integers = [
                "123.45",      # Float
                "abc",         # Text
                "12.3.4",      # Invalid format
                "",            # Empty
                " ",           # Whitespace
            ]

            for input_str in invalid_integers:
                result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
                self.assertIsNone(result, f"Invalid integer '{input_str}' should return None")

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_cast_value_boolean_conversion(self) -> None:
        """Test _cast_value boolean conversion with various inputs."""

        # Create a minimal profiler instance
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id\n1\n')
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            profiler = Profiler(data_lake=data_lake)

            # Test true values (only lowercase versions are accepted)
            true_values = [
                "true", "yes", "y", "1", "on"
            ]

            for input_str in true_values:
                result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
                self.assertTrue(result, f"'{input_str}' should convert to True")

            # Test false values (only lowercase versions are accepted)
            false_values = [
                "false", "no", "n", "0", "off"
            ]

            for input_str in false_values:
                result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
                self.assertFalse(result, f"'{input_str}' should convert to False")

            # Test uppercase/mixed case values (some are accepted by String.to_bool)
            mixed_case_cases = [
                ("True", True),    # Accepted
                ("TRUE", True),    # Accepted
                ("T", None),       # Not accepted (single char, uppercase only)
                ("Yes", True),     # Accepted
                ("YES", True),     # Accepted
                ("Y", True),       # Accepted (lowercases to 'y')
                ("False", False),  # Accepted
                ("FALSE", False),  # Accepted
                ("F", None),       # Not accepted (single char, uppercase only)
                ("No", False),     # Accepted
                ("NO", False),     # Accepted (lowercases to 'no')
                ("N", False),      # Accepted (lowercases to 'n')
            ]

            for input_str, expected in mixed_case_cases:
                result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
                self.assertEqual(result, expected, f"'{input_str}' should convert to {expected}")

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass


if __name__ == '__main__':
    unittest.main()

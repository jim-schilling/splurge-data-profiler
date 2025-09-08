import uuid
import pytest
from pathlib import Path

from splurge_data_profiler.source import DataType, DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


@pytest.fixture
def temp_data_lake_path(tmp_path: Path):
    """Create a temporary directory for data lake testing under pytest's tmp_path."""
    data_lake_path = tmp_path / "data_lake"
    data_lake_path.mkdir()
    yield data_lake_path


def create_test_profiler(data_lake_path: Path) -> Profiler:
    """Helper function to create a test profiler with minimal data.

    The function creates a small CSV file under the same tmp directory as
    the data lake, then removes it after the profiler is created.
    """
    temp_path = data_lake_path.parent / f"{uuid.uuid4().hex}.csv"
    temp_path.write_text("id\n1\n", encoding="utf-8")
    try:
        dsv_source = DsvSource(temp_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
        return Profiler(data_lake=data_lake)
    finally:
        try:
            temp_path.unlink()
        except Exception:
            pass


def test_cast_value_none_input(temp_data_lake_path):
    """Test _cast_value with None input."""
    profiler = create_test_profiler(temp_data_lake_path)

    # Test None input for all data types
    for data_type in [
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.BOOLEAN,
        DataType.DATE,
        DataType.TIME,
        DataType.DATETIME,
        DataType.TEXT,
    ]:
        result = profiler._cast_value(None, target_type=data_type)
        assert result is None, f"None input should return None for {data_type}"


def test_cast_value_empty_string(temp_data_lake_path):
    """Test _cast_value with empty string input."""
    profiler = create_test_profiler(temp_data_lake_path)

    # Test empty string input for all data types
    for data_type in [
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.BOOLEAN,
        DataType.DATE,
        DataType.TIME,
        DataType.DATETIME,
        DataType.TEXT,
    ]:
        result = profiler._cast_value("", target_type=data_type)
        assert result is None, f"Empty string should return None for {data_type}"


def test_cast_value_integer_conversion(temp_data_lake_path):
    """Test _cast_value integer conversion with various inputs."""
    profiler = create_test_profiler(temp_data_lake_path)

    # Test valid integer conversions
    valid_integers = [
        ("123", 123),
        ("-456", -456),
        ("0", 0),
        ("  789  ", 789),  # With whitespace
        ("00123", 123),  # Leading zeros
    ]

    for input_str, expected in valid_integers:
        result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
        assert result == expected, f"Failed to convert '{input_str}' to {expected}"

    # Test invalid integer conversions
    invalid_integers = [
        "123.45",  # Float
        "abc",  # Text
        "12.3.4",  # Invalid format
        "",  # Empty
        " ",  # Whitespace
    ]

    for input_str in invalid_integers:
        result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
        assert result is None, f"Invalid integer '{input_str}' should return None"


def test_cast_value_boolean_conversion(temp_data_lake_path):
    """Test _cast_value boolean conversion with various inputs."""
    profiler = create_test_profiler(temp_data_lake_path)

    # Test true values (only lowercase versions are accepted)
    true_values = ["true", "yes", "y", "1", "on"]

    for input_str in true_values:
        result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
        assert result is True, f"'{input_str}' should convert to True"

    # Test false values (only lowercase versions are accepted)
    false_values = ["false", "no", "n", "0", "off"]

    for input_str in false_values:
        result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
        assert result is False, f"'{input_str}' should convert to False"

    # Test uppercase/mixed case values (some are accepted by String.to_bool)
    mixed_case_cases = [
        ("True", True),  # Accepted
        ("TRUE", True),  # Accepted
        ("T", None),  # Not accepted (single char, uppercase only)
        ("Yes", True),  # Accepted
        ("YES", True),  # Accepted
        ("Y", True),  # Accepted (lowercases to 'y')
        ("False", False),  # Accepted
        ("FALSE", False),  # Accepted
        ("F", None),  # Not accepted (single char, uppercase only)
        ("No", False),  # Accepted
        ("NO", False),  # Accepted (lowercases to 'no')
        ("N", False),  # Accepted (lowercases to 'n')
    ]

    for input_str, expected in mixed_case_cases:
        result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
        assert result == expected, f"'{input_str}' should convert to {expected}"

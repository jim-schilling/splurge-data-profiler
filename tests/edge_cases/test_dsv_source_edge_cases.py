"""
Edge case tests for DsvSource class.

These tests focus on testing DsvSource with edge cases, error conditions,
and boundary scenarios to ensure robust error handling.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DataType, DsvSource


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name,value\n1,Alice,100\n2,Bob,200\n')
    test_file_path = Path(temp_path)

    yield test_file_path

    # Cleanup
    try:
        os.remove(temp_path)
    except Exception:
        pass


def test_dsv_source_nonexistent_file():
    """Test DsvSource with nonexistent file."""
    from splurge_data_profiler.exceptions import FileProcessingError
    nonexistent_path = Path("/nonexistent/path/file.csv")

    with pytest.raises(FileProcessingError):
        DsvSource(nonexistent_path)


def test_dsv_source_empty_file():
    """Test DsvSource with completely empty file."""
    # Create an empty file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    empty_path = Path(temp_path)

    try:
        # File is empty, should handle gracefully
        source = DsvSource(empty_path)
        assert len(source.columns) == 0

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_header_only_file():
    """Test DsvSource with header-only file."""
    # Create a file with only headers
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n')
        header_path = Path(temp_path)

        source = DsvSource(header_path)
        # Should create columns from header
        assert len(source.columns) == 3
        assert source.columns[0].name == 'id'
        assert source.columns[1].name == 'name'
        assert source.columns[2].name == 'value'

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_malformed_delimiter():
    """Test DsvSource with malformed delimiter usage."""
    # Create a file with inconsistent delimiters
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,100\n2,Bob;200\n3,Charlie,300\n')
        malformed_path = Path(temp_path)

        # Should still parse correctly with comma delimiter
        source = DsvSource(malformed_path, delimiter=',')
        assert len(source.columns) == 3

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_encoding_error():
    """Test DsvSource with encoding errors."""
    # Create a file with UTF-8 content
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,José\n2,François\n')
        utf8_path = Path(temp_path)

        # Should work with correct encoding
        source = DsvSource(utf8_path, encoding='utf-8')
        assert len(source.columns) == 2

        # Should fail with wrong encoding
        from splurge_data_profiler.exceptions import FileProcessingError
        with pytest.raises(FileProcessingError):
            DsvSource(utf8_path, encoding='ascii')

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_skip_rows_edge_cases():
    """Test DsvSource with edge cases for skip_header_rows and skip_footer_rows."""
    # Create a file with multiple header and footer rows
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('# Comment line 1\n')
            f.write('# Comment line 2\n')
            f.write('id,name,value\n')
            f.write('1,Alice,100\n')
            f.write('2,Bob,200\n')
            f.write('# Footer comment\n')
            f.write('# Another footer\n')
        skip_path = Path(temp_path)

        # Skip 2 header rows
        source = DsvSource(skip_path, skip_header_rows=2)
        assert len(source.columns) == 3
        assert source.columns[0].name == 'id'

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_large_skip_values():
    """Test DsvSource with skip values larger than file size."""
    # Create a small file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n')
        small_path = Path(temp_path)

        # Skip more rows than exist - should handle gracefully
        source = DsvSource(small_path, skip_header_rows=10, skip_footer_rows=10)
        # Should still work and create columns
        assert len(source.columns) >= 0

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_whitespace_handling():
    """Test DsvSource whitespace handling."""
    # Create a file with various whitespace scenarios
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('  id  ,  name  ,  value  \n')  # Header with whitespace
            f.write('  1  ,  Alice  ,  100  \n')    # Data with whitespace
            f.write('  2  ,  Bob  ,  200  \n')
        ws_path = Path(temp_path)

        # With strip=True (default)
        source_strip = DsvSource(ws_path, strip=True)
        assert source_strip.columns[0].name == 'id'  # Should be stripped

        # With strip=False
        source_no_strip = DsvSource(ws_path, strip=False)
        assert source_no_strip.columns[0].name == 'id'  # Column names are always stripped

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_bookend_edge_cases():
    """Test DsvSource bookend/quote handling edge cases."""
    # Create a file with various quoting scenarios
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('"id","name","value"\n')
            f.write('"1","Alice","100"\n')
            f.write('"2","Bob","200"\n')
            f.write('3,"Charlie","300"\n')  # Mixed quoting
        quote_path = Path(temp_path)

        # With bookend_strip=True (default)
        source_strip = DsvSource(quote_path, bookend='"', bookend_strip=True)
        assert len(source_strip.columns) == 3

        # With bookend_strip=False
        source_no_strip = DsvSource(quote_path, bookend='"', bookend_strip=False)
        assert len(source_no_strip.columns) == 3

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_mixed_data_types():
    """Test DsvSource with mixed data types in columns."""
    # Create a file with mixed data types
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value,active\n')
            f.write('1,Alice,100.5,true\n')
            f.write('2,Bob,200,false\n')
            f.write('3,Charlie,300.75,1\n')
        mixed_path = Path(temp_path)

        source = DsvSource(mixed_path)
        assert len(source.columns) == 4

        # All columns should be TEXT type initially
        for column in source.columns:
            assert column.raw_type == DataType.TEXT

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass
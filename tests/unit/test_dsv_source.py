"""
Unit tests for DsvSource class.

These tests focus on testing the DsvSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource, DataType


class TestDsvSource:
    """Test cases for DsvSource class."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        self.test_file_path = Path(self.temp_path)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass

    def test_dsv_source_initialization_defaults(self) -> None:
        """Test DsvSource initialization with default values."""
        source = DsvSource(self.test_file_path)

        assert source.file_path == self.test_file_path
        assert source.delimiter == ","
        assert source.strip is True
        assert source.bookend == '"'
        assert source.bookend_strip is True
        assert source.encoding == "utf-8"
        assert source.skip_header_rows == 0
        assert source.skip_footer_rows == 0
        assert source.header_rows == 1
        assert source.skip_empty_rows is True

    def test_dsv_source_initialization_custom_values(self) -> None:
        """Test DsvSource initialization with custom values."""
        # Create a file with more data for this test
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("header1\nheader2\nid\tname\n1\tAlice\n2\tBob\nfooter\n")

            source = DsvSource(
                file_path=Path(temp_path),
                delimiter="\t",
                strip=False,
                bookend="'",
                bookend_strip=False,
                encoding="latin-1",
                skip_header_rows=2,
                skip_footer_rows=1,
                header_rows=1,
                skip_empty_rows=False,
            )

            assert source.delimiter == "\t"
            assert source.strip is False
            assert source.bookend == "'"
            assert source.bookend_strip is False
            assert source.encoding == "latin-1"
            assert source.skip_header_rows == 2
            assert source.skip_footer_rows == 1
            assert source.header_rows == 1
            assert source.skip_empty_rows is False
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_equality(self) -> None:
        """Test DsvSource equality comparison."""
        source1 = DsvSource(self.test_file_path, delimiter=",")
        source2 = DsvSource(self.test_file_path, delimiter=",")
        source3 = DsvSource(self.test_file_path, delimiter="\t")

        assert source1 == source2
        assert source1 != source3

    def test_dsv_source_equality_different_type(self) -> None:
        """Test DsvSource equality with different type."""
        source = DsvSource(self.test_file_path)
        other = "not a dsv source"

        assert source != other

    def test_dsv_source_string_representation(self) -> None:
        """Test DsvSource string representation."""
        source = DsvSource(self.test_file_path, delimiter=",")

        # Use flexible pattern matching for key fields
        actual = str(source)
        assert f"file_path={self.test_file_path}" in actual
        assert "delimiter=," in actual
        assert 'bookend="' in actual
        assert "bookend_strip=True" in actual
        assert "encoding=utf-8" in actual
        assert "skip_header_rows=0" in actual
        assert "skip_footer_rows=0" in actual
        assert "header_rows=1" in actual
        assert "skip_empty_rows=True" in actual
        assert "Column(name=id, inferred_type=DataType.TEXT, raw_type=DataType.TEXT, is_nullable=True)" in actual
        assert "Column(name=name, inferred_type=DataType.TEXT, raw_type=DataType.TEXT, is_nullable=True)" in actual


def test_dsv_source_nonexistent_file():
    """Test DsvSource with nonexistent file."""
    from splurge_data_profiler.exceptions import FileProcessingError

    nonexistent_path = Path("/nonexistent/path/file.csv")

    with pytest.raises(FileProcessingError):
        DsvSource(nonexistent_path)


def test_dsv_source_empty_file_handling():
    """Test DsvSource with completely empty file."""
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


def test_dsv_source_malformed_delimiter_and_header_cases():
    """Test DsvSource with malformed delimiter and header-only cases."""
    # Malformed delimiter
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob;200\n3,Charlie,300\n")
        malformed_path = Path(temp_path)

        # Should still parse correctly with comma delimiter
        source = DsvSource(malformed_path, delimiter=",")
        assert len(source.columns) == 3

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_encoding_and_skip_rows_edge_cases():
    """Test DsvSource encoding issues and skip rows behavior."""
    # Encoding error
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,José\n2,François\n")
        utf8_path = Path(temp_path)

        # Should work with correct encoding
        source = DsvSource(utf8_path, encoding="utf-8")
        assert len(source.columns) == 2

        # Should fail with wrong encoding
        from splurge_data_profiler.exceptions import FileProcessingError

        with pytest.raises(FileProcessingError):
            DsvSource(utf8_path, encoding="ascii")

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass

    # Skip rows edge cases
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("# Comment line 1\n")
            f.write("# Comment line 2\n")
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,200\n")
            f.write("# Footer comment\n")
            f.write("# Another footer\n")
        skip_path = Path(temp_path)

        # Skip 2 header rows
        source = DsvSource(skip_path, skip_header_rows=2)
        assert len(source.columns) == 3
        assert source.columns[0].name == "id"

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_whitespace_and_bookend_handling():
    """Test whitespace, bookend, and mixed quoting behaviors."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("  id  ,  name  ,  value  \n")
            f.write("  1  ,  Alice  ,  100  \n")
            f.write("  2  ,  Bob  ,  200  \n")
        ws_path = Path(temp_path)

        # With strip=True (default)
        source_strip = DsvSource(ws_path, strip=True)
        assert source_strip.columns[0].name == "id"

        # With strip=False
        source_no_strip = DsvSource(ws_path, strip=False)
        assert source_no_strip.columns[0].name == "id"  # Column names are always stripped

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_bookend_edge_cases():
    """Test bookend/quote handling edge cases."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
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
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value,active\n")
            f.write("1,Alice,100.5,true\n")
            f.write("2,Bob,200,false\n")
            f.write("3,Charlie,300.75,1\n")
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

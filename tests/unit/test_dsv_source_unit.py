"""
Unit tests for DsvSource class.

These tests focus on testing the DsvSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import os
import tempfile
from pathlib import Path

from splurge_data_profiler.source import DsvSource


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

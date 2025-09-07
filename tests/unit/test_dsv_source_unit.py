"""
Unit tests for DsvSource class.

These tests focus on testing the DsvSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DsvSource


class TestDsvSource(unittest.TestCase):
    """Test cases for DsvSource class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        self.test_file_path = Path(self.temp_path)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass

    def test_dsv_source_initialization_defaults(self) -> None:
        """Test DsvSource initialization with default values."""
        source = DsvSource(self.test_file_path)

        self.assertEqual(source.file_path, self.test_file_path)
        self.assertEqual(source.delimiter, ",")
        self.assertTrue(source.strip)
        self.assertEqual(source.bookend, '"')
        self.assertTrue(source.bookend_strip)
        self.assertEqual(source.encoding, "utf-8")
        self.assertEqual(source.skip_header_rows, 0)
        self.assertEqual(source.skip_footer_rows, 0)
        self.assertEqual(source.header_rows, 1)
        self.assertTrue(source.skip_empty_rows)

    def test_dsv_source_initialization_custom_values(self) -> None:
        """Test DsvSource initialization with custom values."""
        # Create a file with more data for this test
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('header1\nheader2\nid\tname\n1\tAlice\n2\tBob\nfooter\n')

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
                skip_empty_rows=False
            )

            self.assertEqual(source.delimiter, "\t")
            self.assertFalse(source.strip)
            self.assertEqual(source.bookend, "'")
            self.assertFalse(source.bookend_strip)
            self.assertEqual(source.encoding, "latin-1")
            self.assertEqual(source.skip_header_rows, 2)
            self.assertEqual(source.skip_footer_rows, 1)
            self.assertEqual(source.header_rows, 1)
            self.assertFalse(source.skip_empty_rows)
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

        self.assertEqual(source1, source2)
        self.assertNotEqual(source1, source3)

    def test_dsv_source_equality_different_type(self) -> None:
        """Test DsvSource equality with different type."""
        source = DsvSource(self.test_file_path)
        other = "not a dsv source"

        self.assertNotEqual(source, other)

    def test_dsv_source_string_representation(self) -> None:
        """Test DsvSource string representation."""
        source = DsvSource(self.test_file_path, delimiter=",")

        # The string representation will include the actual columns
        expected_str = f"DsvSource(file_path={self.test_file_path}, delimiter=,, bookend=\", bookend_strip=True, encoding=utf-8, skip_header_rows=0, skip_footer_rows=0, header_rows=1, skip_empty_rows=True, columns=[Column(name=id, inferred_type=DataType.TEXT, raw_type=DataType.TEXT, is_nullable=True), Column(name=name, inferred_type=DataType.TEXT, raw_type=DataType.TEXT, is_nullable=True)])"
        self.assertEqual(str(source), expected_str)


if __name__ == "__main__":
    unittest.main()

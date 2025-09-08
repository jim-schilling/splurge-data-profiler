"""
Unit tests for DsvSource class.

These tests focus on testing the DsvSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource, DataType


@pytest.fixture
def dsv_test_file(tmp_path: Path) -> Path:
    """Fixture to create a small DSV file for tests."""
    p = tmp_path / "test.csv"
    p.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    return p


def test_dsv_source_initialization_defaults(dsv_test_file: Path) -> None:
    """Test DsvSource initialization with default values."""
    source = DsvSource(dsv_test_file)

    assert source.file_path == dsv_test_file
    assert source.delimiter == ","
    assert source.strip is True
    assert source.bookend == '"'
    assert source.bookend_strip is True
    assert source.encoding == "utf-8"
    assert source.skip_header_rows == 0
    assert source.skip_footer_rows == 0
    assert source.header_rows == 1
    assert source.skip_empty_rows is True

def test_dsv_source_initialization_custom_values(tmp_path: Path) -> None:
    """Test DsvSource initialization with custom values."""
    p = tmp_path / "custom.csv"
    p.write_text("header1\nheader2\nid\tname\n1\tAlice\n2\tBob\nfooter\n", encoding="utf-8")

    source = DsvSource(
        file_path=p,
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

def test_dsv_source_equality(dsv_test_file: Path) -> None:
    """Test DsvSource equality comparison."""
    source1 = DsvSource(dsv_test_file, delimiter=",")
    source2 = DsvSource(dsv_test_file, delimiter=",")
    source3 = DsvSource(dsv_test_file, delimiter="\t")

    assert source1 == source2
    assert source1 != source3


def test_dsv_source_equality_different_type(dsv_test_file: Path) -> None:
    """Test DsvSource equality with different type."""
    source = DsvSource(dsv_test_file)
    other = "not a dsv source"

    assert source != other


def test_dsv_source_string_representation(dsv_test_file: Path) -> None:
    """Test DsvSource string representation."""
    source = DsvSource(dsv_test_file, delimiter=",")

    # Use flexible pattern matching for key fields
    actual = str(source)
    assert f"file_path={dsv_test_file}" in actual
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


def test_dsv_source_empty_file_handling(tmp_path: Path):
    """Test DsvSource with completely empty file."""
    empty_path = tmp_path / "empty.csv"
    empty_path.write_text("", encoding="utf-8")

    # File is empty, should handle gracefully
    source = DsvSource(empty_path)
    assert len(source.columns) == 0


def test_dsv_source_malformed_delimiter_and_header_cases(tmp_path: Path):
    """Test DsvSource with malformed delimiter and header-only cases."""
    # Malformed delimiter
    p = tmp_path / "malformed.csv"
    p.write_text("id,name,value\n1,Alice,100\n2,Bob;200\n3,Charlie,300\n", encoding="utf-8")

    # Should still parse correctly with comma delimiter
    source = DsvSource(p, delimiter=",")
    assert len(source.columns) == 3


def test_dsv_source_encoding_and_skip_rows_edge_cases(tmp_path: Path):
    """Test DsvSource encoding issues and skip rows behavior."""
    # Encoding error
    utf8_path = tmp_path / "utf8.csv"
    utf8_path.write_text("id,name\n1,José\n2,François\n", encoding="utf-8")

    # Should work with correct encoding
    source = DsvSource(utf8_path, encoding="utf-8")
    assert len(source.columns) == 2

    # Should fail with wrong encoding
    from splurge_data_profiler.exceptions import FileProcessingError

    with pytest.raises(FileProcessingError):
        DsvSource(utf8_path, encoding="ascii")

    # Skip rows edge cases
    skip_path = tmp_path / "skip.csv"
    skip_path.write_text(
        (
            "# Comment line 1\n# Comment line 2\n"
            "id,name,value\n1,Alice,100\n2,Bob,200\n"
            "# Footer comment\n# Another footer\n"
        ),
        encoding="utf-8",
    )

    # Skip 2 header rows
    source = DsvSource(skip_path, skip_header_rows=2)
    assert len(source.columns) == 3
    assert source.columns[0].name == "id"


def test_dsv_source_whitespace_and_bookend_handling(tmp_path: Path):
    """Test whitespace, bookend, and mixed quoting behaviors."""
    ws_path = tmp_path / "ws.csv"
    ws_path.write_text("  id  ,  name  ,  value  \n  1  ,  Alice  ,  100  \n  2  ,  Bob  ,  200  \n", encoding="utf-8")

    # With strip=True (default)
    source_strip = DsvSource(ws_path, strip=True)
    assert source_strip.columns[0].name == "id"

    # With strip=False
    source_no_strip = DsvSource(ws_path, strip=False)
    assert source_no_strip.columns[0].name == "id"  # Column names are always stripped


def test_dsv_source_bookend_edge_cases(tmp_path: Path):
    """Test bookend/quote handling edge cases."""
    quote_path = tmp_path / "quote.csv"
    quote_path.write_text(
        (
            '"id","name","value"\n'
            '"1","Alice","100"\n'
            '"2","Bob","200"\n'
            '3,"Charlie","300"\n'
        ),
        encoding="utf-8",
    )

    # With bookend_strip=True (default)
    source_strip = DsvSource(quote_path, bookend='"', bookend_strip=True)
    assert len(source_strip.columns) == 3

    # With bookend_strip=False
    source_no_strip = DsvSource(quote_path, bookend='"', bookend_strip=False)
    assert len(source_no_strip.columns) == 3


def test_dsv_source_mixed_data_types(tmp_path: Path):
    """Test DsvSource with mixed data types in columns."""
    mixed_path = tmp_path / "mixed.csv"
    mixed_path.write_text(
        (
            "id,name,value,active\n"
            "1,Alice,100.5,true\n"
            "2,Bob,200,false\n"
            "3,Charlie,300.75,1\n"
        ),
        encoding="utf-8",
    )

    source = DsvSource(mixed_path)
    assert len(source.columns) == 4

    # All columns should be TEXT type initially
    for column in source.columns:
        assert column.raw_type == DataType.TEXT

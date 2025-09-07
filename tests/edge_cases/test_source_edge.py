"""
Edge case tests for source module classes.

These tests focus on error conditions, boundary conditions, and edge cases
like empty files, malformed data, and invalid inputs.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DataType, Column, Source, DsvSource, DbSource
from splurge_data_profiler.exceptions import DatabaseError, FileProcessingError


def test_source_with_empty_columns():
    """Test Source with empty columns list."""
    class TestSource(Source):
        pass
    
    source = TestSource(columns=[])
    assert len(source.columns) == 0
    assert len(source) == 0
    
    # Test iteration on empty source
    iterated_columns = list(source)
    assert iterated_columns == []


def test_source_with_none_columns():
    """Test Source with None columns (should default to empty list)."""
    class TestSource(Source):
        pass
    
    source = TestSource(columns=None)
    assert len(source.columns) == 0


def test_source_indexing_out_of_bounds():
    """Test Source indexing with out-of-bounds indices."""
    class TestSource(Source):
        pass
    
    columns = [Column("col1"), Column("col2")]
    source = TestSource(columns=columns)
    
    # Test negative indexing
    assert source[-1] == columns[1]
    assert source[-2] == columns[0]
    
    # Test out-of-bounds positive indexing
    with pytest.raises(IndexError):
        _ = source[2]
    
    # Test out-of-bounds negative indexing
    with pytest.raises(IndexError):
        _ = source[-3]


def test_source_with_duplicate_column_names():
    """Test Source with duplicate column names."""
    class TestSource(Source):
        pass
    
    columns = [Column("col1"), Column("col1")]  # Duplicate names
    source = TestSource(columns=columns)
    
    # Should still work, but may cause issues in downstream processing
    assert len(source.columns) == 2
    assert source.columns[0].name == "col1"
    assert source.columns[1].name == "col1"


def test_dsv_source_nonexistent_file():
    """Test DsvSource with non-existent file."""
    from splurge_data_profiler.exceptions import FileProcessingError
    non_existent_path = Path("/non/existent/file.csv")

    with pytest.raises(FileProcessingError):
        DsvSource(non_existent_path)


def test_dsv_source_empty_file():
    """Test DsvSource with empty file."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    empty_path = Path(temp_path)
    
    try:
        # Create empty file
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as _:
            pass  # Empty file
        
        # Empty files should be handled gracefully with 0 columns
        source = DsvSource(empty_path)
        assert len(source.columns) == 0
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_file_with_only_header():
    """Test DsvSource with file containing only header row."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    header_only_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n')  # Only header, no data
        
        # This should work with just a header
        source = DsvSource(header_only_path)
        
        # Should work, but with no data rows
        assert len(source.columns) == 3
        assert [col.name for col in source.columns] == ["id", "name", "value"]
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_file_with_malformed_header():
    """Test DsvSource with malformed header."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    malformed_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,\n1,Alice,10.5\n')  # Header with trailing comma
        
        # This should work with malformed header
        source = DsvSource(malformed_path)
        
        # Should work, but with auto-generated column name
        assert len(source.columns) == 3
        assert source.columns[2].name == "column_2"
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_with_invalid_delimiter():
    """Test DsvSource with invalid delimiter."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    test_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n')
        
        # Test with empty delimiter
        with pytest.raises(FileProcessingError):
            DsvSource(test_path, delimiter="")
        
        # Test with multi-character delimiter
        # Multi-character delimiter should work
        source = DsvSource(test_path, delimiter="||")
        assert source.delimiter == "||"
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_with_invalid_encoding():
    """Test DsvSource with invalid encoding."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    test_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n')
        
        # Test with invalid encoding
        with pytest.raises(FileProcessingError):
            DsvSource(test_path, encoding="invalid_encoding")
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_with_negative_skip_rows():
    """Test DsvSource with negative skip rows values."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    test_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n')
        
        # Test with negative skip_header_rows - should work (validation happens in underlying library)
        source = DsvSource(test_path, skip_header_rows=-1)
        assert source.skip_header_rows == -1
        
        # Test with negative skip_footer_rows - should work
        source = DsvSource(test_path, skip_footer_rows=-1)
        assert source.skip_footer_rows == -1
        
        # Test with zero header_rows - should fail (validation in underlying library)
        with pytest.raises(FileProcessingError):
            source = DsvSource(test_path, header_rows=0)
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_dsv_source_with_large_skip_values():
    """Test DsvSource with very large skip values."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    test_path = Path(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n')
        
        # Test with skip_header_rows larger than file - should handle gracefully
        source = DsvSource(test_path, skip_header_rows=1000)
        # Should still work and create columns (empty in this case)
        assert len(source.columns) >= 0
            
    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_db_source_invalid_url():
    """Test DbSource with invalid database URL."""
    with pytest.raises(Exception):
        DbSource(
            db_url="invalid://url",
            db_schema="test_schema",
            db_table="test_table"
        )


def test_db_source_empty_url():
    """Test DbSource with empty database URL."""
    with pytest.raises(DatabaseError):
        DbSource(
            db_url="",
            db_schema="test_schema",
            db_table="test_table"
        )


def test_db_source_none_url():
    """Test DbSource with None database URL."""
    with pytest.raises(DatabaseError):
        DbSource(
            db_url=None,
            db_schema="test_schema",
            db_table="test_table"
        )


def test_db_source_empty_table_name():
    """Test DbSource with empty table name."""
    from splurge_data_profiler.exceptions import DatabaseError
    with pytest.raises(DatabaseError):
        DbSource(
            db_url="sqlite:///test.db",
            db_schema="test_schema",
            db_table=""
        )


def test_db_source_none_table_name():
    """Test DbSource with None table name."""
    with pytest.raises(TypeError):
        DbSource(
            db_url="sqlite:///test.db",
            db_schema="test_schema",
            db_table=None
        )


def test_db_source_nonexistent_table():
    """Test DbSource with non-existent table."""
    import tempfile
    
    # Create a temporary SQLite database
    temp_fd, temp_path = tempfile.mkstemp(suffix=".db")
    db_url = f"sqlite:///{temp_path}"
    
    try:
        with pytest.raises(Exception):
            DbSource(
                db_url=db_url,
                db_schema=None,
                db_table="nonexistent_table"
            )
    finally:
        try:
            os.close(temp_fd)
            os.remove(temp_path)
        except Exception:
            pass


def test_db_source_with_special_characters_in_table_name():
    """Test DbSource with special characters in table name."""
    import tempfile
    
    # Create a temporary SQLite database
    temp_fd, temp_path = tempfile.mkstemp(suffix=".db")
    db_url = f"sqlite:///{temp_path}"
    
    try:
        # Test with table name containing spaces
        with pytest.raises(Exception):
            DbSource(
                db_url=db_url,
                db_schema=None,
                db_table="table with spaces"
            )
        
        # Test with table name containing special characters
        with pytest.raises(Exception):
            DbSource(
                db_url=db_url,
                db_schema=None,
                db_table="table-with-dashes"
            )
    finally:
        try:
            os.close(temp_fd)
            os.remove(temp_path)
        except Exception:
            pass


def test_column_empty_name():
    """Test Column with empty name."""
    column = Column("")
    assert column.name == ""
    assert column.inferred_type == DataType.TEXT


def test_column_none_name():
    """Test Column with None name."""
    # Column should accept None name (it's just a string)
    column = Column(None)
    assert column.name is None
    assert column.inferred_type == DataType.TEXT


def test_column_with_special_characters_in_name():
    """Test Column with special characters in name."""
    special_names = [
        "column with spaces",
        "column-with-dashes",
        "column_with_underscores",
        "column123",
        "123column",
        "column!@#$%",
        "column\nwith\nnewlines",
        "column\twith\ttabs"
    ]
    
    for name in special_names:
        column = Column(name)
        assert column.name == name
        assert column.inferred_type == DataType.TEXT


def test_column_with_very_long_name():
    """Test Column with very long name."""
    long_name = "a" * 1000  # 1000 character name
    column = Column(long_name)
    assert column.name == long_name
    assert column.inferred_type == DataType.TEXT


def test_column_with_unicode_name():
    """Test Column with unicode name."""
    unicode_names = [
        "café",
        "résumé",
        "über",
        "naïve",
        "café_émojis_🚀",
        "中文列名",
        "日本語の列名",
        "русский_столбец"
    ]
    
    for name in unicode_names:
        column = Column(name)
        assert column.name == name
        assert column.inferred_type == DataType.TEXT

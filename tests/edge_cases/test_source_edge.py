"""
Edge case tests for source module classes.

These tests focus on error conditions, boundary conditions, and edge cases
like empty files, malformed data, and invalid inputs.
"""

import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DataType, Column, Source, DsvSource, DbSource


class TestSourceEdgeCases(unittest.TestCase):
    """Edge case tests for Source base class."""

    def test_source_with_empty_columns(self) -> None:
        """Test Source with empty columns list."""
        class TestSource(Source):
            pass
        
        source = TestSource(columns=[])
        self.assertEqual(len(source.columns), 0)
        self.assertEqual(len(source), 0)
        
        # Test iteration on empty source
        iterated_columns = list(source)
        self.assertEqual(iterated_columns, [])

    def test_source_with_none_columns(self) -> None:
        """Test Source with None columns (should default to empty list)."""
        class TestSource(Source):
            pass
        
        source = TestSource(columns=None)
        self.assertEqual(len(source.columns), 0)

    def test_source_indexing_out_of_bounds(self) -> None:
        """Test Source indexing with out-of-bounds indices."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        
        # Test negative indexing
        self.assertEqual(source[-1], columns[1])
        self.assertEqual(source[-2], columns[0])
        
        # Test out-of-bounds positive indexing
        with self.assertRaises(IndexError):
            _ = source[2]
        
        # Test out-of-bounds negative indexing
        with self.assertRaises(IndexError):
            _ = source[-3]

    def test_source_with_duplicate_column_names(self) -> None:
        """Test Source with duplicate column names."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col1")]  # Duplicate names
        source = TestSource(columns=columns)
        
        # Should still work, but may cause issues in downstream processing
        self.assertEqual(len(source.columns), 2)
        self.assertEqual(source.columns[0].name, "col1")
        self.assertEqual(source.columns[1].name, "col1")


class TestDsvSourceEdgeCases(unittest.TestCase):
    """Edge case tests for DsvSource class."""

    def test_dsv_source_nonexistent_file(self) -> None:
        """Test DsvSource with non-existent file."""
        non_existent_path = Path("/non/existent/file.csv")
        
        with self.assertRaises(RuntimeError):
            DsvSource(non_existent_path)

    def test_dsv_source_empty_file(self) -> None:
        """Test DsvSource with empty file."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        empty_path = Path(temp_path)
        
        try:
            # Create empty file
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                pass  # Empty file
            
            with self.assertRaises(RuntimeError):
                DsvSource(empty_path)
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_file_with_only_header(self) -> None:
        """Test DsvSource with file containing only header row."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        header_only_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,value\n')  # Only header, no data
            
            # This should work with just a header
            source = DsvSource(header_only_path)
            
            # Should work, but with no data rows
            self.assertEqual(len(source.columns), 3)
            self.assertEqual([col.name for col in source.columns], ["id", "name", "value"])
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_file_with_malformed_header(self) -> None:
        """Test DsvSource with malformed header."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        malformed_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,\n1,Alice,10.5\n')  # Header with trailing comma
            
            # This should work with malformed header
            source = DsvSource(malformed_path)
            
            # Should work, but with auto-generated column name
            self.assertEqual(len(source.columns), 3)
            self.assertEqual(source.columns[2].name, "column_2")
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_with_invalid_delimiter(self) -> None:
        """Test DsvSource with invalid delimiter."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,value\n1,Alice,10.5\n')
            
            # Test with empty delimiter
            with self.assertRaises(RuntimeError):
                DsvSource(test_path, delimiter="")
            
            # Test with multi-character delimiter
            # Multi-character delimiter should work
            source = DsvSource(test_path, delimiter="||")
            self.assertEqual(source.delimiter, "||")
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_with_invalid_encoding(self) -> None:
        """Test DsvSource with invalid encoding."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,value\n1,Alice,10.5\n')
            
            # Test with invalid encoding
            with self.assertRaises(RuntimeError):
                DsvSource(test_path, encoding="invalid_encoding")
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_with_negative_skip_rows(self) -> None:
        """Test DsvSource with negative skip rows values."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,value\n1,Alice,10.5\n')
            
            # Test with negative skip_header_rows - should work (validation happens in underlying library)
            source = DsvSource(test_path, skip_header_rows=-1)
            self.assertEqual(source.skip_header_rows, -1)
            
            # Test with negative skip_footer_rows - should work
            source = DsvSource(test_path, skip_footer_rows=-1)
            self.assertEqual(source.skip_footer_rows, -1)
            
            # Test with zero header_rows - should fail (validation in underlying library)
            with self.assertRaises(RuntimeError):
                source = DsvSource(test_path, header_rows=0)
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_dsv_source_with_large_skip_values(self) -> None:
        """Test DsvSource with very large skip values."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write('id,name,value\n1,Alice,10.5\n')
            
            # Test with skip_header_rows larger than file
            with self.assertRaises(RuntimeError):
                source = DsvSource(test_path, skip_header_rows=1000)
                
        finally:
            try:
                os.remove(temp_path)
            except:
                pass


class TestDbSourceEdgeCases(unittest.TestCase):
    """Edge case tests for DbSource class."""

    def test_db_source_invalid_url(self) -> None:
        """Test DbSource with invalid database URL."""
        with self.assertRaises(Exception):
            DbSource(
                db_url="invalid://url",
                db_schema="test_schema",
                db_table="test_table"
            )

    def test_db_source_empty_url(self) -> None:
        """Test DbSource with empty database URL."""
        with self.assertRaises(RuntimeError):
            DbSource(
                db_url="",
                db_schema="test_schema",
                db_table="test_table"
            )

    def test_db_source_none_url(self) -> None:
        """Test DbSource with None database URL."""
        with self.assertRaises(RuntimeError):
            DbSource(
                db_url=None,
                db_schema="test_schema",
                db_table="test_table"
            )

    def test_db_source_empty_table_name(self) -> None:
        """Test DbSource with empty table name."""
        with self.assertRaises(RuntimeError):
            DbSource(
                db_url="sqlite:///test.db",
                db_schema="test_schema",
                db_table=""
            )

    def test_db_source_none_table_name(self) -> None:
        """Test DbSource with None table name."""
        with self.assertRaises(TypeError):
            DbSource(
                db_url="sqlite:///test.db",
                db_schema="test_schema",
                db_table=None
            )

    def test_db_source_nonexistent_table(self) -> None:
        """Test DbSource with non-existent table."""
        import tempfile
        
        # Create a temporary SQLite database
        temp_fd, temp_path = tempfile.mkstemp(suffix=".db")
        db_url = f"sqlite:///{temp_path}"
        
        try:
            with self.assertRaises(Exception):
                DbSource(
                    db_url=db_url,
                    db_schema=None,
                    db_table="nonexistent_table"
                )
        finally:
            try:
                os.close(temp_fd)
                os.remove(temp_path)
            except:
                pass

    def test_db_source_with_special_characters_in_table_name(self) -> None:
        """Test DbSource with special characters in table name."""
        import tempfile
        
        # Create a temporary SQLite database
        temp_fd, temp_path = tempfile.mkstemp(suffix=".db")
        db_url = f"sqlite:///{temp_path}"
        
        try:
            # Test with table name containing spaces
            with self.assertRaises(Exception):
                DbSource(
                    db_url=db_url,
                    db_schema=None,
                    db_table="table with spaces"
                )
            
            # Test with table name containing special characters
            with self.assertRaises(Exception):
                DbSource(
                    db_url=db_url,
                    db_schema=None,
                    db_table="table-with-dashes"
                )
        finally:
            try:
                os.close(temp_fd)
                os.remove(temp_path)
            except:
                pass


class TestColumnEdgeCases(unittest.TestCase):
    """Edge case tests for Column class."""

    def test_column_empty_name(self) -> None:
        """Test Column with empty name."""
        column = Column("")
        self.assertEqual(column.name, "")
        self.assertEqual(column.inferred_type, DataType.TEXT)

    def test_column_none_name(self) -> None:
        """Test Column with None name."""
        # Column should accept None name (it's just a string)
        column = Column(None)
        self.assertEqual(column.name, None)
        self.assertEqual(column.inferred_type, DataType.TEXT)

    def test_column_with_special_characters_in_name(self) -> None:
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
            self.assertEqual(column.name, name)
            self.assertEqual(column.inferred_type, DataType.TEXT)

    def test_column_with_very_long_name(self) -> None:
        """Test Column with very long name."""
        long_name = "a" * 1000  # 1000 character name
        column = Column(long_name)
        self.assertEqual(column.name, long_name)
        self.assertEqual(column.inferred_type, DataType.TEXT)

    def test_column_with_unicode_name(self) -> None:
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
            self.assertEqual(column.name, name)
            self.assertEqual(column.inferred_type, DataType.TEXT)


if __name__ == '__main__':
    unittest.main() 
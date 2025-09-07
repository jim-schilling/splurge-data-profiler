"""
Unit tests for DbSource class.

These tests focus on testing the DbSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import os
import tempfile
import unittest

from sqlalchemy import create_engine, MetaData, Table as SATable, Column as SAColumn, String

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.exceptions import DatabaseError


class TestDbSource(unittest.TestCase):
    """Test cases for DbSource class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary SQLite database file
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_table = "test_table"
        self.db_schema = None  # SQLite does not use schemas
        # Create table
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        SATable(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        # Create a second table for equality testing
        SATable(
            "different_table", metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("description", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Ensure engine is disposed before removing file
        try:
            self.engine.dispose()
        except Exception:
            pass
        os.close(self.db_fd)
        try:
            os.remove(self.db_path)
        except PermissionError:
            # File might still be in use, that's okay for tests
            pass

    def test_db_source_initialization_connection_error(self) -> None:
        """Test DbSource initialization with invalid database URL."""
        with self.assertRaises(DatabaseError):
            DbSource(
                db_url="sqlite:///nonexistent.db",
                db_schema=None,
                db_table="nonexistent_table"
            )

    def test_db_source_properties(self) -> None:
        """Test DbSource properties."""
        source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )

        self.assertEqual(source.db_url, self.db_url)
        self.assertEqual(source.db_schema, self.db_schema)
        self.assertEqual(source.db_table, self.db_table)
        self.assertEqual(len(source.columns), 2)
        self.assertEqual(source.columns[0].name, "id")
        self.assertEqual(source.columns[1].name, "name")

    def test_db_source_string_representation(self) -> None:
        """Test DbSource string representation."""
        # Create a temporary database for this test
        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        db_url = f"sqlite:///{db_path}"

        try:
            # Create a simple table
            engine = create_engine(db_url)
            metadata = MetaData()
            SATable(
                "test_table", metadata,
                SAColumn("id", String, primary_key=True),
            )
            metadata.create_all(engine)
            engine.dispose()

            source = DbSource(
                db_url=db_url,
                db_schema=None,
                db_table="test_table"
            )

            # Test the new DbSource __str__ method
            expected_str = f"DbSource(db_url={db_url}, schema=None, table=test_table, columns=1)"
            self.assertEqual(str(source), expected_str)

        finally:
            # Ensure engine is disposed before removing file
            try:
                engine.dispose()
            except Exception:
                pass
            os.close(db_fd)
            try:
                os.remove(db_path)
            except PermissionError:
                # File might still be in use, that's okay for tests
                pass

    def test_db_source_equality(self) -> None:
        """Test DbSource equality comparison."""
        source1 = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )
        source2 = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )

        # Create a different source with different table name but same database
        source3 = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table="different_table"
        )

        self.assertEqual(source1, source2)
        self.assertNotEqual(source1, source3)

    def test_db_source_equality_different_type(self) -> None:
        """Test DbSource equality with different type."""
        source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )
        other = "not a db source"

        self.assertNotEqual(source, other)

    def test_db_source_repr_representation(self) -> None:
        """Test DbSource repr representation."""
        source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )

        # Test that repr shows detailed information
        repr_str = repr(source)
        self.assertIn("DbSource", repr_str)
        self.assertIn(self.db_url, repr_str)
        self.assertIn(self.db_table, repr_str)
        self.assertIn("columns=", repr_str)


if __name__ == "__main__":
    unittest.main()

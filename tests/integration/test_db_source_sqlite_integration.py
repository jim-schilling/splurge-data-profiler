"""
Integration tests for DbSource with real SQLite database.

These tests focus on testing DbSource with real database connections and file systems,
validating end-to-end functionality without mocking.
"""

import os
import tempfile
import unittest

from sqlalchemy import create_engine, MetaData, Column as SAColumn, String, Table

from splurge_data_profiler.source import DbSource


class TestDbSourceWithRealSQLite(unittest.TestCase):
    """Integration test for DbSource using a real SQLite database (no mocking)."""

    def setUp(self) -> None:
        # Create a temporary SQLite database file
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_table = "test_table"
        self.db_schema = None  # SQLite does not use schemas
        # Create table
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
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

    def test_dbsource_sqlite_columns(self):
        source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)
        self.assertEqual(len(source.columns), 2)
        self.assertEqual(source.columns[0].name, "id")
        self.assertEqual(source.columns[1].name, "name")
        self.assertTrue(all(col.raw_type.name == "TEXT" for col in source.columns))


if __name__ == "__main__":
    unittest.main()

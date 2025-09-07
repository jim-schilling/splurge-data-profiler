import os
import tempfile
import unittest

from sqlalchemy import create_engine, MetaData, Column as SAColumn, String, Table

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.data_lake import DataLake


class TestDataLake(unittest.TestCase):
    """Test cases for DataLake class."""

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
        Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        # Create a second table for equality testing
        Table(
            "different_table", metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("description", String, nullable=True),
        )
        metadata.create_all(self.engine)

        # Create DbSource and DataLake
        self.db_source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )
        self.data_lake = DataLake(db_source=self.db_source)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            self.engine.dispose()
        except Exception:
            pass
        os.close(self.db_fd)
        try:
            os.remove(self.db_path)
        except PermissionError:
            pass

    def test_data_lake_initialization(self) -> None:
        """Test DataLake initialization."""
        self.assertIsInstance(self.data_lake, DataLake)
        self.assertEqual(self.data_lake.db_source, self.db_source)
        self.assertEqual(self.data_lake.db_url, self.db_url)
        self.assertEqual(self.data_lake.db_schema, self.db_schema)
        self.assertEqual(self.data_lake.db_table, self.db_table)
        self.assertEqual(self.data_lake.column_names, ["id", "name"])

    def test_data_lake_string_representation(self) -> None:
        """Test DataLake string representation."""
        expected_str = f"DataLake(db_url={self.db_url}, schema=None, table={self.db_table}, columns=2)"
        self.assertEqual(str(self.data_lake), expected_str)

    def test_data_lake_repr_representation(self) -> None:
        """Test DataLake repr representation."""
        repr_str = repr(self.data_lake)
        self.assertIn("DataLake", repr_str)
        self.assertIn(self.db_url, repr_str)
        self.assertIn(self.db_table, repr_str)
        self.assertIn("columns=", repr_str)

    def test_data_lake_equality(self) -> None:
        """Test DataLake equality comparison."""
        data_lake1 = DataLake(db_source=self.db_source)
        data_lake2 = DataLake(db_source=self.db_source)

        # They should be equal since they have the same db_source
        self.assertEqual(data_lake1, data_lake2)

        # Create a different db_source using the different table in the same database
        different_db_source = DbSource(
            db_url=self.db_url,
            db_schema=None,
            db_table="different_table"
        )
        data_lake3 = DataLake(db_source=different_db_source)

        # They should not be equal since they have different db_sources
        self.assertNotEqual(data_lake1, data_lake3)

    def test_data_lake_equality_different_type(self) -> None:
        """Test DataLake equality with different type."""
        other = "not a data lake"
        self.assertNotEqual(self.data_lake, other)

    def test_data_lake_properties(self) -> None:
        """Test DataLake properties."""
        # Test db_source property
        self.assertEqual(self.data_lake.db_source, self.db_source)

        # Test column_names property
        self.assertEqual(self.data_lake.column_names, ["id", "name"])

        # Test db_url property
        self.assertEqual(self.data_lake.db_url, self.db_url)

        # Test db_schema property
        self.assertEqual(self.data_lake.db_schema, self.db_schema)

        # Test db_table property
        self.assertEqual(self.data_lake.db_table, self.db_table)


if __name__ == '__main__':
    unittest.main()

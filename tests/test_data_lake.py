import os
import tempfile
import unittest
from pathlib import Path
from typing import List

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String

from splurge_data_profiler.source import DataType, DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.source import DsvSource


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
        self.table = Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        # Create a second table for equality testing
        self.different_table = Table(
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
        except:
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




class TestDataLakeIntegration(unittest.TestCase):
    """Integration tests for DataLake with real data."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n')
        self.test_file_path = Path(self.temp_path)
        
        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except:
            pass
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except:
            pass

    def test_data_lake_from_factory(self) -> None:
        """Test DataLake creation through DataLakeFactory."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake using factory
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Test DataLake properties
        self.assertIsInstance(data_lake, DataLake)
        self.assertIsInstance(data_lake.db_source, DbSource)
        self.assertEqual(data_lake.column_names, ["id", "name", "value"])
        self.assertEqual(data_lake.db_table, self.test_file_path.stem)
        self.assertIsNone(data_lake.db_schema)  # SQLite doesn't use schemas
        
        # Test string representation
        expected_str = f"DataLake(db_url={data_lake.db_url}, schema=None, table={data_lake.db_table}, columns=3)"
        self.assertEqual(str(data_lake), expected_str)

    def test_data_lake_equality_with_factory_created(self) -> None:
        """Test DataLake equality with factory-created instances."""
        # Create two data lakes from the same source
        dsv_source = DsvSource(self.test_file_path)
        
        data_lake1 = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        data_lake2 = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # They should be equal since they have the same configuration
        self.assertEqual(data_lake1, data_lake2)

    def test_data_lake_empty_dsv(self):
        """Test DataLake creation from an empty DSV file."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n')  # Only header, no data
        dsv_source = DsvSource(temp_path)
        # Should not raise, but will create an empty table
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        self.assertIsInstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_missing_columns(self):
        """Test DataLake creation from DSV with missing columns."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Should not raise, but will have None for missing values
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        self.assertIsInstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_extra_columns(self):
        """Test DataLake creation from DSV with extra columns in data rows."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice,Extra\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Should not raise, extra columns are ignored
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        self.assertIsInstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_batch_size_edge_case(self):
        """Test DataLake batch insertion with minimum batch_size (edge case)."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Patch DataLakeFactory to use minimum batch_size
        orig_stream = DataLakeFactory._stream_dsv_to_sqlite
        def patched_stream(*args, **kwargs):
            return orig_stream(*args, **kwargs, batch_size=100)
        DataLakeFactory._stream_dsv_to_sqlite, orig = patched_stream, DataLakeFactory._stream_dsv_to_sqlite
        try:
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            self.assertIsInstance(data_lake, DataLake)
        finally:
            DataLakeFactory._stream_dsv_to_sqlite = orig
        os.remove(temp_path)




if __name__ == '__main__':
    unittest.main() 
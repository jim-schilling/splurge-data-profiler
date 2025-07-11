"""
Integration tests for source module classes.

These tests focus on testing interactions between components and
using real file systems and databases.
"""

import os
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text
from sqlalchemy.exc import SQLAlchemyError

from splurge_data_profiler.source import DataType, Column, Source, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_tools.dsv_helper import DsvHelper
from splurge_tools.tabular_data_model import TabularDataModel


class TestDsvSourceIntegration(unittest.TestCase):
    """Integration tests for DsvSource with real files."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n')
        self.test_file_path = Path(self.temp_path)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except:
            pass

    def test_dsv_source_real_file(self):
        """Test DsvSource with a real CSV file."""
        # This will use the real DsvHelper and TabularDataModel
        source = DsvSource(self.test_file_path)
        
        # Verify the source was created correctly
        self.assertEqual(source.file_path, self.test_file_path)
        self.assertEqual(len(source.columns), 3)
        self.assertEqual([col.name for col in source.columns], ["id", "name", "value"])
        
        # Verify all columns are TEXT type initially
        for col in source.columns:
            self.assertEqual(col.inferred_type, DataType.TEXT)


class TestDbSourceWithRealSQLite(unittest.TestCase):
    """Integration tests for DbSource with real SQLite database."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary SQLite database file
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_schema = None  # SQLite does not use schemas
        self.db_table = "test_table"
        
        # Create table with some data
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        self.table = Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
            SAColumn("value", String, nullable=True),
        )
        metadata.create_all(self.engine)
        
        # Insert some test data
        with self.engine.connect() as conn:
            conn.execute(text(f"INSERT INTO {self.db_table} VALUES ('1', 'Alice', '10.5')"))
            conn.execute(text(f"INSERT INTO {self.db_table} VALUES ('2', 'Bob', '20.0')"))
            conn.commit()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            self.engine.dispose()
        except:
            pass
        try:
            os.close(self.db_fd)
            os.remove(self.db_path)
        except:
            pass

    def test_dbsource_sqlite_columns(self):
        """Test DbSource with real SQLite database."""
        source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )
        
        # Verify the source was created correctly
        self.assertEqual(source.db_url, self.db_url)
        self.assertEqual(source.db_schema, self.db_schema)
        self.assertEqual(source.db_table, self.db_table)
        self.assertEqual(len(source.columns), 3)
        self.assertEqual([col.name for col in source.columns], ["id", "name", "value"])


class TestDataLakeFactoryStreaming(unittest.TestCase):
    """Integration tests for DataLakeFactory with streaming large files."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)
        
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        self.csv_path = Path(self.temp_path)
        
        # Generate a large CSV file for testing
        self._generate_large_csv_file()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            os.close(self.temp_fd)
            os.remove(self.temp_path)
        except:
            pass
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except:
            pass

    def _generate_large_csv_file(self) -> None:
        """Generate a large CSV file for testing."""
        import csv
        import random
        import string
        
        # Generate 10,000 rows of data
        num_rows = 10000
        num_columns = 5
        
        # Generate column names
        column_names = [f"col_{i}" for i in range(num_columns)]
        
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            
            for i in range(num_rows):
                row = []
                for j in range(num_columns):
                    if j == 0:
                        row.append(str(i))  # ID column
                    elif j == 1:
                        row.append(f"name_{i}")  # Name column
                    elif j == 2:
                        row.append(str(random.randint(1, 1000)))  # Integer column
                    elif j == 3:
                        row.append(f"{random.uniform(0, 100):.2f}")  # Float column
                    else:
                        row.append(''.join(random.choices(string.ascii_letters, k=10)))  # Text column
                writer.writerow(row)

    def test_streaming_large_dsv_file_creation(self) -> None:
        """Test creating a data lake from a large DSV file using streaming."""
        # Create DsvSource
        dsv_source = DsvSource(self.csv_path)
        
        # Create data lake using factory with streaming
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Verify the data lake was created correctly
        self.assertIsInstance(data_lake, DataLake)
        self.assertEqual(len(data_lake.column_names), 5)
        self.assertEqual(data_lake.column_names, ["col_0", "col_1", "col_2", "col_3", "col_4"])

    def test_streaming_large_dsv_file_data_integrity(self) -> None:
        """Test data integrity when streaming large DSV files."""
        # Create DsvSource
        dsv_source = DsvSource(self.csv_path)
        
        # Create data lake using factory
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Verify data integrity by checking a few rows
        from sqlalchemy import create_engine, text
        engine = create_engine(data_lake.db_url)
        with engine.connect() as conn:
            # Check total row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
            row_count = result.scalar()
            self.assertEqual(row_count, 10000)
            
            # Check first row
            result = conn.execute(text(f"SELECT * FROM {data_lake.db_table} LIMIT 1"))
            first_row = result.fetchone()
            self.assertIsNotNone(first_row)
            self.assertEqual(first_row[0], "0")  # First ID should be "0"
            
            # Check last row
            result = conn.execute(text(f"SELECT * FROM {data_lake.db_table} ORDER BY col_0 DESC LIMIT 1"))
            last_row = result.fetchone()
            self.assertIsNotNone(last_row)
            self.assertEqual(last_row[0], "9999")  # Last ID should be "9999"

    def test_streaming_large_dsv_file_performance(self) -> None:
        """Test performance of streaming large DSV files."""
        import time
        
        # Create DsvSource
        dsv_source = DsvSource(self.csv_path)
        
        # Measure creation time
        start_time = time.time()
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        end_time = time.time()
        
        creation_time = end_time - start_time
        
        # Verify the data lake was created successfully
        self.assertIsInstance(data_lake, DataLake)
        
        # Performance assertion (should complete within reasonable time)
        # 10,000 rows should process in under 30 seconds
        self.assertLess(creation_time, 30.0, f"Data lake creation took {creation_time:.2f} seconds")

    def test_streaming_large_dsv_file_memory_usage(self) -> None:
        """Test memory usage when streaming large DSV files."""
        # Skip this test if psutil is not available
        try:
            import psutil
            import os
            
            # Get initial memory usage
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss
            
            # Create DsvSource
            dsv_source = DsvSource(self.csv_path)
            
            # Create data lake using factory
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            
            # Get final memory usage
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Verify the data lake was created successfully
            self.assertIsInstance(data_lake, DataLake)
            
            # Memory usage assertion (should not increase excessively)
            # Memory increase should be reasonable (less than 100MB for 10K rows)
            memory_increase_mb = memory_increase / (1024 * 1024)
            self.assertLess(memory_increase_mb, 100.0, 
                           f"Memory usage increased by {memory_increase_mb:.2f} MB")
        except ImportError:
            # Skip test if psutil is not available
            self.skipTest("psutil not available - skipping memory usage test")

    def test_streaming_large_dsv_file_with_different_delimiters(self) -> None:
        """Test streaming large DSV files with different delimiters."""
        import csv
        
        # Create a temporary pipe-delimited file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".txt")
        csv_path = Path(temp_path)
        
        try:
            # Generate pipe-delimited data
            with os.fdopen(temp_fd, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerow(["id", "name", "value"])
                for i in range(1000):
                    writer.writerow([str(i), f"name_{i}", str(i * 1.5)])
            
            # Create DsvSource with pipe delimiter
            dsv_source = DsvSource(csv_path, delimiter='|')
            
            # Create data lake using factory
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            
            # Verify the data lake was created correctly
            self.assertIsInstance(data_lake, DataLake)
            self.assertEqual(len(data_lake.column_names), 3)
            self.assertEqual(data_lake.column_names, ["id", "name", "value"])
            
        finally:
            try:
                os.remove(temp_path)
            except:
                pass

    def test_streaming_large_dsv_file_error_handling(self) -> None:
        """Test error handling when streaming large DSV files."""
        # Test with non-existent file
        non_existent_path = Path("/non/existent/file.csv")
        
        with self.assertRaises(RuntimeError):
            dsv_source = DsvSource(non_existent_path)
        
        # Test with empty file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        empty_path = Path(temp_path)
        
        try:
            # Create empty file
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                pass  # Empty file
            
            # Should raise RuntimeError for empty file
            with self.assertRaises(RuntimeError):
                dsv_source = DsvSource(empty_path)
            
        finally:
            try:
                os.remove(temp_path)
            except:
                pass


if __name__ == '__main__':
    unittest.main() 
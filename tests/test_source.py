import os
import tempfile
import unittest
from pathlib import Path
from typing import List
import random
import string

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text
from sqlalchemy.exc import SQLAlchemyError

from splurge_data_profiler.source import DataType, Column, Source, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_tools.dsv_helper import DsvHelper
from splurge_tools.tabular_data_model import TabularDataModel


class TestDataType(unittest.TestCase):
    """Test cases for DataType enum."""

    def test_data_type_values(self) -> None:
        """Test that all DataType enum values are correct."""
        expected_values = {
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE", 
            "DATETIME": "DATETIME",
            "FLOAT": "FLOAT",
            "INTEGER": "INTEGER",
            "TEXT": "TEXT",
            "TIME": "TIME"
        }
        
        for enum_name, expected_value in expected_values.items():
            enum_member = getattr(DataType, enum_name)
            self.assertEqual(enum_member.value, expected_value)

    def test_data_type_membership(self) -> None:
        """Test that DataType enum contains expected members."""
        expected_members = {"BOOLEAN", "DATE", "DATETIME", "FLOAT", "INTEGER", "TEXT", "TIME"}
        actual_members = {member.name for member in DataType}
        self.assertEqual(actual_members, expected_members)


class TestColumn(unittest.TestCase):
    """Test cases for Column class."""

    def test_column_initialization_defaults(self) -> None:
        """Test Column initialization with default values."""
        column = Column("test_column")
        
        self.assertEqual(column.name, "test_column")
        self.assertEqual(column.inferred_type, DataType.TEXT)
        self.assertEqual(column.raw_type, DataType.TEXT)
        self.assertTrue(column.is_nullable)

    def test_column_initialization_custom_values(self) -> None:
        """Test Column initialization with custom values."""
        column = Column(
            name="custom_column",
            inferred_type=DataType.INTEGER,
            is_nullable=False
        )
        
        self.assertEqual(column.name, "custom_column")
        self.assertEqual(column.inferred_type, DataType.INTEGER)
        self.assertEqual(column.raw_type, DataType.TEXT)
        self.assertFalse(column.is_nullable)

    def test_column_string_representation(self) -> None:
        """Test Column string representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT)
        
        expected_str = "test_column (DataType.FLOAT)"
        self.assertEqual(str(column), expected_str)

    def test_column_repr_representation(self) -> None:
        """Test Column repr representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT, is_nullable=False)
        
        expected_repr = "Column(name=test_column, inferred_type=DataType.FLOAT, raw_type=DataType.TEXT, is_nullable=False)"
        self.assertEqual(repr(column), expected_repr)


class TestSource(unittest.TestCase):
    """Test cases for Source abstract base class."""

    def test_source_initialization_defaults(self) -> None:
        """Test Source initialization with default values."""
        class TestSource(Source):
            pass
        
        source = TestSource()
        self.assertEqual(len(source.columns), 0)

    def test_source_initialization_custom_values(self) -> None:
        """Test Source initialization with custom values."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        self.assertEqual(len(source.columns), 2)
        self.assertEqual(source.columns[0].name, "col1")
        self.assertEqual(source.columns[1].name, "col2")

    def test_source_columns_property(self) -> None:
        """Test Source columns property."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        
        # Test that columns property returns the correct list
        self.assertEqual(source.columns, columns)
        
        # Test that modifying the returned list doesn't affect the source
        source.columns.append(Column("col3"))
        self.assertEqual(len(source.columns), 2)

    def test_source_iteration(self) -> None:
        """Test Source iteration."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        
        # Test iteration
        iterated_columns = list(source)
        self.assertEqual(iterated_columns, columns)

    def test_source_length(self) -> None:
        """Test Source length."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2"), Column("col3")]
        source = TestSource(columns=columns)
        
        self.assertEqual(len(source), 3)

    def test_source_indexing(self) -> None:
        """Test Source indexing."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        
        self.assertEqual(source[0], columns[0])
        self.assertEqual(source[1], columns[1])

    def test_source_equality(self) -> None:
        """Test Source equality."""
        class TestSource(Source):
            pass
        
        columns1 = [Column("col1"), Column("col2")]
        columns2 = [Column("col1"), Column("col2")]
        columns3 = [Column("col1"), Column("col3")]
        
        source1 = TestSource(columns=columns1)
        source2 = TestSource(columns=columns2)
        source3 = TestSource(columns=columns3)
        
        self.assertEqual(source1, source2)
        self.assertNotEqual(source1, source3)

    def test_source_equality_different_type(self) -> None:
        """Test Source equality with different type."""
        class TestSource(Source):
            pass
        
        source = TestSource()
        other = "not a source"
        
        self.assertNotEqual(source, other)

    def test_source_string_representation(self) -> None:
        """Test Source string representation."""
        class TestSource(Source):
            pass
        
        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        
        expected_str = f"Source(columns={columns})"
        self.assertEqual(str(source), expected_str)


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
        except:
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
            except:
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
        self.table = Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Ensure engine is disposed before removing file
        try:
            self.engine.dispose()
        except:
            pass
        os.close(self.db_fd)
        try:
            os.remove(self.db_path)
        except PermissionError:
            # File might still be in use, that's okay for tests
            pass

    def test_db_source_initialization_connection_error(self) -> None:
        """Test DbSource initialization with invalid database URL."""
        with self.assertRaises(RuntimeError):
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
            table = Table(
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
            
            # Since DbSource doesn't override __str__, it uses the parent class
            expected_str = f"Source(columns=[Column(name=id, inferred_type=DataType.TEXT, raw_type=DataType.TEXT, is_nullable=False)])"
            self.assertEqual(str(source), expected_str)
            
        finally:
            # Ensure engine is disposed before removing file
            try:
                engine.dispose()
            except:
                pass
            os.close(db_fd)
            try:
                os.remove(db_path)
            except PermissionError:
                # File might still be in use, that's okay for tests
                pass


class TestDsvSourceIntegration(unittest.TestCase):
    """Integration test for DsvSource using a real CSV file (no mocking)."""

    def setUp(self) -> None:
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        self.file_path = Path(self.temp_path)

    def tearDown(self) -> None:
        os.remove(self.temp_path)

    def test_dsv_source_real_file(self):
        # This will use the real DsvHelper and TabularDataModel
        source = DsvSource(self.file_path)
        try:
            columns = source._initialize()
        except Exception as exc:
            raise
        self.assertTrue(len(columns) >= 2)
        self.assertEqual(columns[0].name, "id")
        self.assertEqual(columns[1].name, "name")


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
        self.table = Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
        # Ensure engine is disposed before removing file
        try:
            self.engine.dispose()
        except:
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


class TestDataLakeFactoryStreaming(unittest.TestCase):
    """Comprehensive test for DataLakeFactory streaming functionality with large files."""

    def setUp(self) -> None:
        """Set up test fixtures for large file testing."""
        # Create a temporary CSV file with more than 5000 lines
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        self.test_file_path = Path(self.temp_path)
        
        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)
        
        # Generate large dataset
        self._generate_large_csv_file()

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

    def _generate_large_csv_file(self) -> None:
        """Generate a CSV file with more than 5000 lines of test data."""
        # Define column headers
        headers = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]
        
        # Generate random data
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "IT"]
        
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            # Write header
            f.write(','.join(headers) + '\n')
            
            # Generate 5500 data rows (more than 5000 as requested)
            for i in range(1, 5501):
                # Generate random data for each row
                name = f"Employee_{i:04d}"
                email = f"employee_{i:04d}@company.com"
                age = random.randint(22, 65)
                city = random.choice(cities)
                salary = random.randint(30000, 150000)
                department = random.choice(departments)
                hire_date = f"202{random.randint(0, 3)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
                
                # Create row data
                row_data = [str(i), name, email, str(age), city, str(salary), department, hire_date]
                f.write(','.join(row_data) + '\n')

    def test_streaming_large_dsv_file_creation(self) -> None:
        """Test that streaming can handle large DSV files (>5000 lines)."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Verify the source was created correctly
        self.assertEqual(len(dsv_source.columns), 8)
        expected_columns = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]
        actual_columns = [col.name for col in dsv_source.columns]
        self.assertEqual(actual_columns, expected_columns)
        
        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Verify the data lake was created
        self.assertIsInstance(data_lake, DataLake)
        self.assertIsInstance(data_lake.db_source, DbSource)
        
        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())
        
        # Verify the database URL is correct
        expected_db_url = f"sqlite:///{expected_db_path}"
        self.assertEqual(data_lake.db_url, expected_db_url)
        
        # Verify the table name is correct
        expected_table_name = self.test_file_path.stem
        self.assertEqual(data_lake.db_table, expected_table_name)
        
        # Verify the schema is None (SQLite doesn't use schemas)
        self.assertIsNone(data_lake.db_schema)
        
        # Verify the column names are preserved
        self.assertEqual(data_lake.column_names, expected_columns)

    def test_streaming_large_dsv_file_data_integrity(self) -> None:
        """Test that all data from the large DSV file is correctly inserted into SQLite."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Connect to the created database and verify data integrity
        engine = create_engine(data_lake.db_url)
        
        try:
            with engine.connect() as connection:
                # Count total rows
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                
                # Should have 5500 rows (excluding header)
                self.assertEqual(row_count, 5500)
                
                # Verify first row
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '1'"))
                first_row = result.fetchone()
                self.assertIsNotNone(first_row)
                self.assertEqual(first_row[0], "1")  # id
                self.assertEqual(first_row[1], "Employee_0001")  # name
                self.assertEqual(first_row[2], "employee_0001@company.com")  # email
                
                # Verify last row
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '5500'"))
                last_row = result.fetchone()
                self.assertIsNotNone(last_row)
                self.assertEqual(last_row[0], "5500")  # id
                self.assertEqual(last_row[1], "Employee_5500")  # name
                self.assertEqual(last_row[2], "employee_5500@company.com")  # email
                
                # Verify data types and constraints
                result = connection.execute(text(f"PRAGMA table_info({table_name})"))
                columns_info = result.fetchall()
                
                # Should have 8 columns
                self.assertEqual(len(columns_info), 8)
                
                # All columns should be TEXT/VARCHAR type (as per our implementation)
                for col_info in columns_info:
                    self.assertIn(col_info[2], ["TEXT", "VARCHAR"])  # type column
                
                # Verify some random rows for data integrity
                for i in range(1, 11):
                    row_id = random.randint(1, 5500)
                    result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '{row_id}'"))
                    row = result.fetchone()
                    self.assertIsNotNone(row)
                    self.assertEqual(row[0], str(row_id))
                    self.assertEqual(row[1], f"Employee_{row_id:04d}")
                    self.assertEqual(row[2], f"employee_{row_id:04d}@company.com")
                
        finally:
            engine.dispose()

    def test_streaming_large_dsv_file_performance(self) -> None:
        """Test that streaming performs efficiently with large files."""
        import time
        
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Measure processing time
        start_time = time.time()
        
        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify the operation completed successfully
        self.assertIsInstance(data_lake, DataLake)
        
        # Performance assertion: should complete within reasonable time
        # (adjust threshold based on system capabilities)
        self.assertLess(processing_time, 30.0, f"Processing took {processing_time:.2f} seconds, which is too slow")
        
        # Verify file size is reasonable (should be larger than original CSV due to SQLite overhead)
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())
        
        csv_size = self.test_file_path.stat().st_size
        db_size = expected_db_path.stat().st_size
        
        # SQLite file should be larger than CSV due to indexing and structure
        self.assertGreater(db_size, csv_size * 0.5)  # At least 50% of CSV size

    def test_streaming_large_dsv_file_memory_usage(self) -> None:
        """Test that streaming can handle large files without memory issues."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Verify the operation completed successfully
        self.assertIsInstance(data_lake, DataLake)
        
        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())
        
        # Verify data integrity by checking row count
        engine = create_engine(data_lake.db_url)
        try:
            with engine.connect() as connection:
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                self.assertEqual(row_count, 5500)
        finally:
            engine.dispose()
        
        # Test that we can process multiple large files in sequence without issues
        # This indirectly tests memory management
        for i in range(3):
            # Create another large file
            temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
            temp_file_path = Path(temp_path)
            
            try:
                # Generate a smaller but still substantial dataset
                with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                    f.write("id,name,value\n")
                    for j in range(1, 1001):  # 1000 rows
                        f.write(f"{j},Test_{j},{j * 10.5}\n")
                
                # Process the file
                dsv_source_2 = DsvSource(temp_file_path)
                data_lake_2 = DataLakeFactory.from_dsv_source(
                    dsv_source=dsv_source_2,
                    data_lake_path=self.data_lake_path
                )
                
                # Verify it was processed correctly
                self.assertIsInstance(data_lake_2, DataLake)
                
                # Verify data integrity
                engine_2 = create_engine(data_lake_2.db_url)
                try:
                    with engine_2.connect() as connection:
                        table_name = data_lake_2.db_table
                        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = result.scalar()
                        self.assertEqual(row_count, 1000)
                finally:
                    engine_2.dispose()
                    
            finally:
                try:
                    os.remove(temp_path)
                except:
                    pass

    def test_streaming_large_dsv_file_with_different_delimiters(self) -> None:
        """Test streaming with different delimiters (TSV format)."""
        # Create a TSV file with the same data
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        tsv_file_path = Path(tsv_path)
        
        try:
            # Copy the CSV content but replace commas with tabs
            with open(self.test_file_path, 'r', encoding='utf-8') as csv_file:
                csv_content = csv_file.read()
                tsv_content = csv_content.replace(',', '\t')
            
            with os.fdopen(tsv_fd, 'w', encoding='utf-8') as tsv_file:
                tsv_file.write(tsv_content)
            
            # Create DSV source with tab delimiter
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")
            
            # Create data lake using streaming
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            
            # Verify the data lake was created
            self.assertIsInstance(data_lake, DataLake)
            
            # Verify the SQLite file was created with the correct name
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            self.assertTrue(expected_db_path.exists())
            
            # Verify data integrity
            engine = create_engine(data_lake.db_url)
            try:
                with engine.connect() as connection:
                    table_name = data_lake.db_table
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    self.assertEqual(row_count, 5500)
            finally:
                engine.dispose()
                
        finally:
            try:
                os.remove(tsv_path)
            except:
                pass

    def test_streaming_large_dsv_file_error_handling(self) -> None:
        """Test error handling with malformed large files."""
        # Create a malformed CSV file (missing some values)
        malformed_fd, malformed_path = tempfile.mkstemp(suffix=".csv")
        malformed_file_path = Path(malformed_path)
        
        try:
            with os.fdopen(malformed_fd, 'w', encoding='utf-8') as f:
                f.write("id,name,email,age,city,salary,department,hire_date\n")
                # Add some malformed rows
                for i in range(1, 1001):
                    if i % 100 == 0:  # Every 100th row is malformed
                        f.write(f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000\n")  # Missing values
                    else:
                        f.write(f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000,Engineering,2023-01-01\n")
            
            # Create DSV source
            dsv_source = DsvSource(malformed_file_path)
            
            # This should still work as our implementation handles missing values
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            
            # Verify the data lake was created
            self.assertIsInstance(data_lake, DataLake)
            
            # Verify data was inserted (some rows may have NULL values)
            engine = create_engine(data_lake.db_url)
            try:
                with engine.connect() as connection:
                    table_name = data_lake.db_table
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    self.assertEqual(row_count, 1000)
                    
                    # Check that malformed rows have NULL values
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE department IS NULL"))
                    null_count = result.scalar()
                    self.assertEqual(null_count, 10)  # 10 malformed rows
            finally:
                engine.dispose()
                
        finally:
            try:
                os.remove(malformed_path)
            except:
                pass


class TestDataLakeFactory(unittest.TestCase):
    """Test cases for DataLakeFactory class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n')
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

    def test_from_dsv_source_creates_sqlite_table(self) -> None:
        """Test that from_dsv_source creates a SQLite table correctly."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Verify the data lake was created
        self.assertIsInstance(data_lake, DataLake)
        self.assertIsInstance(data_lake.db_source, DbSource)
        
        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())
        
        # Verify the database URL is correct
        expected_db_url = f"sqlite:///{expected_db_path}"
        self.assertEqual(data_lake.db_url, expected_db_url)
        
        # Verify the table name is correct
        expected_table_name = self.test_file_path.stem
        self.assertEqual(data_lake.db_table, expected_table_name)
        
        # Verify the schema is None (SQLite doesn't use schemas)
        self.assertIsNone(data_lake.db_schema)
        
        # Verify the column names are preserved
        expected_columns = ["id", "name", "value"]
        self.assertEqual(data_lake.column_names, expected_columns)
        
        # Verify the columns in the database source
        db_columns = data_lake.db_source.columns
        self.assertEqual(len(db_columns), 3)
        self.assertEqual(db_columns[0].name, "id")
        self.assertEqual(db_columns[1].name, "name")
        self.assertEqual(db_columns[2].name, "value")

    def test_from_dsv_source_creates_directory_if_not_exists(self) -> None:
        """Test that from_dsv_source creates the data lake directory if it doesn't exist."""
        # Create a non-existent directory path
        non_existent_path = self.data_lake_path / "new_directory"
        
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=non_existent_path
        )
        
        # Verify the directory was created
        self.assertTrue(non_existent_path.exists())
        self.assertTrue(non_existent_path.is_dir())
        
        # Verify the SQLite file was created in the new directory
        expected_db_path = non_existent_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())

    def test_from_dsv_source_with_different_file_types(self) -> None:
        """Test that from_dsv_source works with different file extensions."""
        # Create a TSV file
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        try:
            with os.fdopen(tsv_fd, 'w', encoding='utf-8') as f:
                f.write('id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n')
            
            tsv_file_path = Path(tsv_path)
            
            # Create DSV source with tab delimiter
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")
            
            # Create data lake
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )
            
            # Verify the SQLite file was created with the correct name
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            self.assertTrue(expected_db_path.exists())
            
            # Verify the table name is correct (without .tsv extension)
            expected_table_name = tsv_file_path.stem
            self.assertEqual(data_lake.db_table, expected_table_name)
            
        finally:
            try:
                os.remove(tsv_path)
            except:
                pass


if __name__ == '__main__':
    unittest.main() 
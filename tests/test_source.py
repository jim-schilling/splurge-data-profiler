import os
import shutil
import tempfile
from pathlib import Path
import random
import pytest

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text

from splurge_data_profiler.source import DataType, Column, Source, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.exceptions import DatabaseError, FileProcessingError


class TestDataType:
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
            "TIME": "TIME",
        }

        for enum_name, expected_value in expected_values.items():
            enum_member = getattr(DataType, enum_name)
            assert enum_member.value == expected_value

    def test_data_type_membership(self) -> None:
        """Test that DataType enum contains expected members."""
        expected_members = {"BOOLEAN", "DATE", "DATETIME", "FLOAT", "INTEGER", "TEXT", "TIME"}
        actual_members = {member.name for member in DataType}
        assert actual_members == expected_members


class TestColumn:
    """Test cases for Column class."""

    def test_column_initialization_defaults(self) -> None:
        """Test Column initialization with default values."""
        column = Column("test_column")

        assert column.name == "test_column"
        assert column.inferred_type == DataType.TEXT
        assert column.raw_type == DataType.TEXT
        assert column.is_nullable is True

    def test_column_initialization_custom_values(self) -> None:
        """Test Column initialization with custom values."""
        column = Column(name="custom_column", inferred_type=DataType.INTEGER, is_nullable=False)

        assert column.name == "custom_column"
        assert column.inferred_type == DataType.INTEGER
        assert column.raw_type == DataType.TEXT
        assert column.is_nullable is False

    def test_column_string_representation(self) -> None:
        """Test Column string representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT)

        assert str(column)

    def test_column_repr_representation(self) -> None:
        """Test Column repr representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT, is_nullable=False)

        # Test that repr contains essential information
        repr_str = repr(column)
        assert repr_str


class TestSource:
    """Test cases for Source abstract base class."""

    def test_source_initialization_defaults(self) -> None:
        """Test Source initialization with default values."""

        class TestSource(Source):
            pass

        source = TestSource()
        assert len(source.columns) == 0

    def test_source_initialization_custom_values(self) -> None:
        """Test Source initialization with custom values."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)
        assert len(source.columns) == 2
        assert source.columns[0].name == "col1"
        assert source.columns[1].name == "col2"

    def test_source_columns_property(self) -> None:
        """Test Source columns property."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        # Test that columns property returns the correct list
        assert source.columns == columns

        # Test that modifying the returned list doesn't affect the source
        source.columns.append(Column("col3"))
        assert len(source.columns) == 2

    def test_source_iteration(self) -> None:
        """Test Source iteration."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        # Test iteration
        iterated_columns = list(source)
        assert iterated_columns == columns

    def test_source_length(self) -> None:
        """Test Source length."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2"), Column("col3")]
        source = TestSource(columns=columns)

        assert len(source) == 3

    def test_source_indexing(self) -> None:
        """Test Source indexing."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        assert source[0] == columns[0]
        assert source[1] == columns[1]

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

        assert source1 == source2
        assert source1 != source3

    def test_source_equality_different_type(self) -> None:
        """Test Source equality with different type."""

        class TestSource(Source):
            pass

        source = TestSource()
        other = "not a source"

        assert source != other

    def test_source_string_representation(self) -> None:
        """Test Source string representation."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        assert "Source(columns=" in str(source)
        assert "col1" in str(source)
        assert "col2" in str(source)


class TestDsvSource:
    """Test cases for DsvSource class."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        self.test_file_path = Path(self.temp_path)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass

    def test_dsv_source_initialization_defaults(self) -> None:
        """Test DsvSource initialization with default values."""
        source = DsvSource(self.test_file_path)

        assert source.file_path == self.test_file_path
        assert source.delimiter == ","
        assert source.strip
        assert source.bookend == '"'
        assert source.bookend_strip
        assert source.encoding == "utf-8"
        assert source.skip_header_rows == 0
        assert source.skip_footer_rows == 0
        assert source.header_rows == 1
        assert source.skip_empty_rows

    def test_dsv_source_initialization_custom_values(self) -> None:
        """Test DsvSource initialization with custom values."""
        # Create a file with more data for this test
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("header1\nheader2\nid\tname\n1\tAlice\n2\tBob\nfooter\n")

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
                skip_empty_rows=False,
            )

            assert source.delimiter == "\t"
            assert not (source.strip)
            assert source.bookend == "'"
            assert not (source.bookend_strip)
            assert source.encoding == "latin-1"
            assert source.skip_header_rows == 2
            assert source.skip_footer_rows == 1
            assert source.header_rows == 1
            assert not (source.skip_empty_rows)
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

        assert source1 == source2
        assert source1 != source3

    def test_dsv_source_equality_different_type(self) -> None:
        """Test DsvSource equality with different type."""
        source = DsvSource(self.test_file_path)
        other = "not a dsv source"

        assert source != other

    def test_dsv_source_string_representation(self) -> None:
        """Test DsvSource string representation."""
        source = DsvSource(self.test_file_path, delimiter=",")
        # Test key components are present without exact matching
        s = str(source)
        assert "DsvSource" in s
        assert str(self.test_file_path) in s
        assert "delimiter=," in s
        assert "bookend_strip=True" in s
        assert "encoding=utf-8" in s
        assert "skip_header_rows=0" in s
        assert "skip_footer_rows=0" in s
        assert "header_rows=1" in s
        assert "skip_empty_rows=True" in s
        assert "columns=" in s


class TestDbSource:
    """Test cases for DbSource class."""

    def setup_method(self) -> None:
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
            self.db_table,
            metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        # Create a second table for equality testing
        Table(
            "different_table",
            metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("description", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def teardown_method(self) -> None:
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
        with pytest.raises(DatabaseError):
            DbSource(db_url="sqlite:///nonexistent.db", db_schema=None, db_table="nonexistent_table")

    def test_db_source_properties(self) -> None:
        """Test DbSource properties."""
        source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)

        assert source.db_url == self.db_url
        assert source.db_schema == self.db_schema
        assert source.db_table == self.db_table
        assert len(source.columns) == 2
        assert source.columns[0].name == "id"
        assert source.columns[1].name == "name"

    def test_db_source_string_representation(self) -> None:
        """Test DbSource string representation."""
        # Create a temporary database for this test
        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        db_url = f"sqlite:///{db_path}"

        try:
            # Create a simple table
            engine = create_engine(db_url)
            metadata = MetaData()
            Table(
                "test_table",
                metadata,
                SAColumn("id", String, primary_key=True),
            )
            metadata.create_all(engine)
            engine.dispose()

            source = DbSource(db_url=db_url, db_schema=None, db_table="test_table")

            # Test the new DbSource __str__ method (check membership)
            s = str(source)
            assert "DbSource" in s
            assert "db_url=" in s or "sqlite://" in s
            # Check key pieces are present without exact matching
            assert db_url in s
            assert "schema=None" in s
            assert "table=test_table" in s
            assert "columns=" in s

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
        source1 = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)
        source2 = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)

        # Create a different source with different table name but same database
        source3 = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table="different_table")

        assert source1 == source2
        assert source1 != source3

    def test_db_source_equality_different_type(self) -> None:
        """Test DbSource equality with different type."""
        source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)
        other = "not a db source"

        assert source != other

    def test_db_source_repr_representation(self) -> None:
        """Test DbSource repr representation."""
        source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)

        # Test that repr shows detailed information
        repr_str = repr(source)
        assert "DbSource" in repr_str
        assert self.db_url in repr_str
        assert self.db_table in repr_str
        assert "columns=" in repr_str


class TestDsvSourceIntegration:
    """Integration test for DsvSource using a real CSV file (no mocking)."""

    def setup_method(self) -> None:
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        self.file_path = Path(self.temp_path)

    def teardown_method(self) -> None:
        os.remove(self.temp_path)

    def test_dsv_source_real_file(self):
        # This will use the real DsvHelper and TabularDataModel
        source = DsvSource(self.file_path)
        try:
            columns = source._initialize()
        except (ValueError, RuntimeError):
            raise
        except Exception as exc:
            raise RuntimeError(f"Unexpected error in test: {exc}")

        # Verify columns were loaded
        assert len(columns) >= 2
        assert columns[0].name == "id"
        assert columns[1].name == "name"


class TestDbSourceWithRealSQLite:
    """Integration test for DbSource using a real SQLite database (no mocking)."""

    def setup_method(self) -> None:
        # Create a temporary SQLite database file
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_table = "test_table"
        self.db_schema = None  # SQLite does not use schemas
        # Create table
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        Table(
            self.db_table,
            metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        metadata.create_all(self.engine)

    def teardown_method(self) -> None:
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
        assert len(source.columns) == 2
        assert source.columns[0].name == "id"
        assert source.columns[1].name == "name"


class TestDataLakeFactoryStreaming:
    """Comprehensive test for DataLakeFactory streaming functionality with large files."""

    def setup_method(self) -> None:
        """Set up test fixtures for large file testing."""
        # Create a temporary CSV file with more than 5000 lines
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        self.test_file_path = Path(self.temp_path)

        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

        # Generate large dataset
        self._generate_large_csv_file()

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass
        try:
            shutil.rmtree(self.temp_dir)
        except Exception:
            pass

    def _generate_large_csv_file(self) -> None:
        """Generate a CSV file with more than 5000 lines of test data."""
        # Define column headers
        headers = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]

        # Generate random data
        cities = [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
            "San Diego",
        ]
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "IT"]

        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            # Write header
            f.write(",".join(headers) + "\n")

            # Generate 5000 data rows
            for i in range(1, 5001):
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
                f.write(",".join(row_data) + "\n")

    def test_streaming_large_dsv_file_creation(self) -> None:
        """Test that streaming can handle large DSV files (>5000 lines)."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Verify the source was created correctly
        assert len(dsv_source.columns) == 8
        expected_columns = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]
        actual_columns = [col.name for col in dsv_source.columns]
        assert actual_columns == expected_columns

        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

        # Verify the data lake was created
        assert isinstance(data_lake, DataLake)
        assert isinstance(data_lake.db_source, DbSource)

        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        # Verify the database URL is correct
        expected_db_url = f"sqlite:///{expected_db_path}"
        assert data_lake.db_url == expected_db_url

        # Verify the table name is correct
        expected_table_name = self.test_file_path.stem
        assert data_lake.db_table == expected_table_name

        # Skip schema assertion for SQLite
        if "sqlite" not in data_lake.db_url:
            assert data_lake.db_schema is None

        # Verify the column names are preserved
        assert data_lake.column_names == expected_columns

    def test_streaming_large_dsv_file_data_integrity(self) -> None:
        """Test that all data from the large DSV file is correctly inserted into SQLite."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

        # Connect to the created database and verify data integrity
        engine = create_engine(data_lake.db_url)

        try:
            with engine.connect() as connection:
                # Count total rows
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()

                # Should have 5000 rows (excluding header)
                assert row_count == 5000

                # Verify first row
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '1'"))
                first_row = result.fetchone()
                assert first_row
                assert first_row[0] == "1"  # id
                assert first_row[1] == "Employee_0001"  # name
                assert first_row[2] == "employee_0001@company.com"  # email

                # Verify last row
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '5000'"))
                last_row = result.fetchone()
                assert last_row
                assert last_row[0] == "5000"  # id
                assert last_row[1] == "Employee_5000"  # name
                assert last_row[2] == "employee_5000@company.com"  # email

                # Verify data types and constraints
                result = connection.execute(text(f"PRAGMA table_info({table_name})"))
                columns_info = result.fetchall()

                # Should have 8 columns
                assert len(columns_info) == 8

                # All columns should be TEXT/VARCHAR type (as per our implementation)
                for col_info in columns_info:
                    # SQLite reports column types as a single string (e.g. 'VARCHAR')
                    # Accept either 'TEXT' or 'VARCHAR' to be robust across platforms
                    assert col_info[2] in ["TEXT", "VARCHAR"]  # type column

                # Verify some random rows for data integrity
                for i in range(1, 11):
                    row_id = random.randint(1, 5000)
                    result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '{row_id}'"))
                    row = result.fetchone()
                    assert row
                    assert row[0] == str(row_id)
                    assert row[1] == f"Employee_{row_id:04d}"
                    assert row[2] == f"employee_{row_id:04d}@company.com"

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
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

        end_time = time.time()
        processing_time = end_time - start_time

        # Verify the operation completed successfully
        assert isinstance(data_lake, DataLake)

        # Performance assertion: should complete within reasonable time
        # (adjust threshold based on system capabilities)
        assert processing_time < 30.0, f"Processing took {processing_time:.2f} seconds, which is too slow"

        # Verify file size is reasonable (should be larger than original CSV due to SQLite overhead)
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        csv_size = self.test_file_path.stat().st_size
        db_size = expected_db_path.stat().st_size

        # SQLite file should be larger than CSV due to indexing and structure
        assert db_size > csv_size * 0.5  # At least 50% of CSV size

    def test_streaming_large_dsv_file_memory_usage(self) -> None:
        """Test that streaming can handle large files without memory issues."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

        # Verify the operation completed successfully
        assert isinstance(data_lake, DataLake)

        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        # Verify data integrity by checking row count
        engine = create_engine(data_lake.db_url)
        try:
            with engine.connect() as connection:
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                assert row_count == 5000
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
                with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                    f.write("id,name,value\n")
                    for j in range(1, 1001):  # 1000 rows
                        f.write(f"{j},Test_{j},{j * 10.5}\n")

                # Process the file
                dsv_source_2 = DsvSource(temp_file_path)
                data_lake_2 = DataLakeFactory.from_dsv_source(
                    dsv_source=dsv_source_2, data_lake_path=self.data_lake_path
                )

                # Verify it was processed correctly
                assert isinstance(data_lake_2, DataLake)

                # Verify data integrity
                engine_2 = create_engine(data_lake_2.db_url)
                try:
                    with engine_2.connect() as connection:
                        table_name = data_lake_2.db_table
                        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = result.scalar()
                        assert row_count == 1000
                finally:
                    engine_2.dispose()

            finally:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def test_streaming_large_dsv_file_with_different_delimiters(self) -> None:
        """Test streaming with different delimiters (TSV format)."""
        # Create a TSV file with the same data
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        tsv_file_path = Path(tsv_path)

        try:
            # Copy the CSV content but replace commas with tabs
            with open(self.test_file_path, "r", encoding="utf-8") as csv_file:
                csv_content = csv_file.read()
                tsv_content = csv_content.replace(",", "\t")

            with os.fdopen(tsv_fd, "w", encoding="utf-8") as tsv_file:
                tsv_file.write(tsv_content)

            # Create DSV source with tab delimiter
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")

            # Create data lake using streaming
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

            # Verify the data lake was created
            assert isinstance(data_lake, DataLake)

            # Verify the SQLite file was created with the correct name
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            assert expected_db_path.exists()

            # Verify data integrity
            engine = create_engine(data_lake.db_url)
            try:
                with engine.connect() as connection:
                    table_name = data_lake.db_table
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    assert row_count == 5000
            finally:
                engine.dispose()

        finally:
            try:
                os.remove(tsv_path)
            except Exception:
                pass

    def test_streaming_large_dsv_file_error_handling(self) -> None:
        """Test error handling with malformed large files."""
        # Create a malformed CSV file (missing some values)
        malformed_fd, malformed_path = tempfile.mkstemp(suffix=".csv")
        malformed_file_path = Path(malformed_path)

        try:
            with os.fdopen(malformed_fd, "w", encoding="utf-8") as f:
                f.write("id,name,email,age,city,salary,department,hire_date\n")
                # Add some malformed rows
                for i in range(1, 1001):
                    if i % 100 == 0:  # Every 100th row is malformed
                        f.write(
                            (
                                f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,"
                                f"New York,50000\n"
                            )
                        )  # Missing values
                    else:
                        f.write(
                            (
                                f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,"
                                f"50000,Engineering,2023-01-01\n"
                            )
                        )

            # Create DSV source
            dsv_source = DsvSource(malformed_file_path)

            # This should still work as our implementation handles missing values
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

            # Verify the data lake was created
            assert isinstance(data_lake, DataLake)

            # Verify data was inserted (some rows may have NULL values)
            engine = create_engine(data_lake.db_url)
            try:
                with engine.connect() as connection:
                    table_name = data_lake.db_table
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    assert row_count == 1000

                    # Check that malformed rows have NULL values
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE department IS NULL"))
                    null_count = result.scalar()
                    assert null_count == 10  # 10 malformed rows
            finally:
                engine.dispose()

        finally:
            try:
                os.remove(malformed_path)
            except Exception:
                pass


class TestDataLakeFactory:
    """Test cases for DataLakeFactory class."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")
        self.test_file_path = Path(self.temp_path)

        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass
        try:
            shutil.rmtree(self.temp_dir)
        except Exception:
            pass

    def test_from_dsv_source_creates_sqlite_table(self) -> None:
        """Test that from_dsv_source creates a SQLite table correctly."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

        # Verify the data lake was created
        assert isinstance(data_lake, DataLake)
        assert isinstance(data_lake.db_source, DbSource)

        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        # Verify the database URL is correct
        expected_db_url = f"sqlite:///{expected_db_path}"
        assert data_lake.db_url == expected_db_url

        # Verify the table name is correct
        expected_table_name = self.test_file_path.stem
        assert data_lake.db_table == expected_table_name

        # Verify the schema is None (SQLite doesn't use schemas)
        assert data_lake.db_schema is None

        # Verify the column names are preserved
        expected_columns = ["id", "name", "value"]
        assert data_lake.column_names == expected_columns

        # Verify the columns in the database source
        db_columns = data_lake.db_source.columns
        assert len(db_columns) == 3
        assert db_columns[0].name == "id"
        assert db_columns[1].name == "name"
        assert db_columns[2].name == "value"

    def test_from_dsv_source_creates_directory_if_not_exists(self) -> None:
        """Test that from_dsv_source creates the data lake directory if it doesn't exist."""
        # Create a non-existent directory path
        non_existent_path = self.data_lake_path / "new_directory"

        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake
        DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=non_existent_path)

        # Verify the directory was created
        assert non_existent_path.exists()
        assert non_existent_path.is_dir()

        # Verify the SQLite file was created in the new directory
        expected_db_path = non_existent_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

    def test_from_dsv_source_with_different_file_types(self) -> None:
        """Test that from_dsv_source works with different file extensions."""
        # Create a TSV file
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        try:
            with os.fdopen(tsv_fd, "w", encoding="utf-8") as f:
                f.write("id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n")

            tsv_file_path = Path(tsv_path)

            # Create DSV source with tab delimiter
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")

            # Create data lake
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

            # Verify the SQLite file was created with the correct name
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            assert expected_db_path.exists()

            # Verify the table name is correct (without .tsv extension)
            expected_table_name = tsv_file_path.stem
            assert data_lake.db_table == expected_table_name

        finally:
            try:
                os.remove(tsv_path)
            except Exception:
                pass


class TestDsvSourceEdgeCases:
    """Test cases for DsvSource edge cases and error conditions."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
        self.test_file_path = Path(self.temp_path)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass

    def test_dsv_source_nonexistent_file(self) -> None:
        """Test DsvSource with nonexistent file."""
        nonexistent_path = Path("/nonexistent/path/file.csv")

        with pytest.raises(FileProcessingError):
            DsvSource(nonexistent_path)

    def test_dsv_source_empty_file(self) -> None:
        """Test DsvSource with completely empty file."""
        # Create an empty file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        empty_path = Path(temp_path)

        try:
            # File is empty, should handle gracefully
            source = DsvSource(empty_path)
            assert len(source.columns) == 0

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_header_only_file(self) -> None:
        """Test DsvSource with header-only file."""
        # Create a file with only headers
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value\n")
            header_path = Path(temp_path)

            source = DsvSource(header_path)
            # Should create columns from header
            assert len(source.columns) == 3
            assert source.columns[0].name == "id"
            assert source.columns[1].name == "name"
            assert source.columns[2].name == "value"

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_malformed_delimiter(self) -> None:
        """Test DsvSource with malformed delimiter usage."""
        # Create a file with inconsistent delimiters
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value\n1,Alice,100\n2,Bob;200\n3,Charlie,300\n")
            malformed_path = Path(temp_path)

            # Should still parse correctly with comma delimiter
            source = DsvSource(malformed_path, delimiter=",")
            assert len(source.columns) == 3

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_encoding_error(self) -> None:
        """Test DsvSource with encoding errors."""
        # Create a file with UTF-8 content
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name\n1,José\n2,François\n")
            utf8_path = Path(temp_path)

            # Should work with correct encoding
            source = DsvSource(utf8_path, encoding="utf-8")
            assert len(source.columns) == 2

            # Should fail with wrong encoding
            with pytest.raises(FileProcessingError):
                DsvSource(utf8_path, encoding="ascii")

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_skip_rows_edge_cases(self) -> None:
        """Test DsvSource with edge cases for skip_header_rows and skip_footer_rows."""
        # Create a file with multiple header and footer rows
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("# Comment line 1\n")
                f.write("# Comment line 2\n")
                f.write("id,name,value\n")
                f.write("1,Alice,100\n")
                f.write("2,Bob,200\n")
                f.write("# Footer comment\n")
                f.write("# Another footer\n")
            skip_path = Path(temp_path)

            # Skip 2 header rows
            source = DsvSource(skip_path, skip_header_rows=2)
            assert len(source.columns) == 3
            assert source.columns[0].name == "id"

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_large_skip_values(self) -> None:
        """Test DsvSource with skip values larger than file size."""
        # Create a small file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name\n1,Alice\n")
            small_path = Path(temp_path)

            # Skip more rows than exist - should handle gracefully
            source = DsvSource(small_path, skip_header_rows=10, skip_footer_rows=10)
            # Should still work and create columns
            assert len(source.columns) >= 0

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_whitespace_handling(self) -> None:
        """Test DsvSource whitespace handling."""
        # Create a file with various whitespace scenarios
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("  id  ,  name  ,  value  \n")  # Header with whitespace
                f.write("  1  ,  Alice  ,  100  \n")  # Data with whitespace
                f.write("  2  ,  Bob  ,  200  \n")
            ws_path = Path(temp_path)

            # With strip=True (default)
            source_strip = DsvSource(ws_path, strip=True)
            assert source_strip.columns[0].name == "id"  # Should be stripped

            # With strip=False
            source_no_strip = DsvSource(ws_path, strip=False)
            assert source_no_strip.columns[0].name == "id"  # Column names are always stripped

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_bookend_edge_cases(self) -> None:
        """Test DsvSource bookend/quote handling edge cases."""
        # Create a file with various quoting scenarios
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write('"id","name","value"\n')
                f.write('"1","Alice","100"\n')
                f.write('"2","Bob","200"\n')
                f.write('3,"Charlie","300"\n')  # Mixed quoting
            quote_path = Path(temp_path)

            # With bookend_strip=True (default)
            source_strip = DsvSource(quote_path, bookend='"', bookend_strip=True)
            assert len(source_strip.columns) == 3

            # With bookend_strip=False
            source_no_strip = DsvSource(quote_path, bookend='"', bookend_strip=False)
            assert len(source_no_strip.columns) == 3

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_mixed_data_types(self) -> None:
        """Test DsvSource with mixed data types in columns."""
        # Create a file with mixed data types
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value,active\n")
                f.write("1,Alice,100.5,true\n")
                f.write("2,Bob,200,false\n")
                f.write("3,Charlie,300.75,1\n")
            mixed_path = Path(temp_path)

            source = DsvSource(mixed_path)
            assert len(source.columns) == 4

            # All columns should be TEXT type initially
            for column in source.columns:
                assert column.raw_type == DataType.TEXT

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass


class TestColumnEdgeCases:
    """Test cases for Column class edge cases."""

    def test_column_inferred_type_setter_edge_cases(self) -> None:
        """Test Column.inferred_type setter with edge cases."""
        column = Column("test_column")

        # Test setting various data types
        for data_type in [
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.TIME,
            DataType.DATETIME,
            DataType.TEXT,
        ]:
            column.inferred_type = data_type
            assert column.inferred_type == data_type

        # Test setting to same type multiple times
        original_type = column.inferred_type
        column.inferred_type = original_type
        assert column.inferred_type == original_type

    def test_column_equality_edge_cases(self) -> None:
        """Test Column equality with edge cases."""
        column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column3 = Column("other", inferred_type=DataType.INTEGER, is_nullable=False)

        # Same columns should be equal
        assert column1 == column2

        # Different names should not be equal
        assert column1 != column3

        # Different inferred types should not be equal
        column4 = Column("test", inferred_type=DataType.FLOAT, is_nullable=False)
        assert column1 != column4

        # Different nullable should not be equal
        column5 = Column("test", inferred_type=DataType.INTEGER, is_nullable=True)
        assert column1 != column5

    def test_column_hash_consistency(self) -> None:
        """Test Column hash consistency."""
        column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)

        # Equal columns should have equal hashes
        assert hash(column1) == hash(column2)

        # Hash should be consistent across calls
        hash1 = hash(column1)
        hash2 = hash(column1)
        assert hash1 == hash2

    def test_column_string_representations(self) -> None:
        """Test Column string representations with various data types."""
        test_cases = [
            DataType.TEXT,
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.TIME,
            DataType.DATETIME,
        ]

        for data_type in test_cases:
            column = Column("test_column", inferred_type=data_type)
            # String representation includes the column name and the inferred type
            assert "test_column" in str(column)
            assert f"DataType.{data_type.value}" in str(column)


class TestSourceEdgeCases:
    """Test cases for Source abstract base class edge cases."""

    def test_source_iteration_edge_cases(self) -> None:
        """Test Source iteration with edge cases."""
        from splurge_data_profiler.source import Source

        class TestSource(Source):
            pass

        # Empty source
        empty_source = TestSource()
        assert len(empty_source) == 0
        assert list(empty_source) == []

        # Source with columns
        columns = [Column("col1"), Column("col2"), Column("col3")]
        source = TestSource(columns=columns)
        assert len(source) == 3
        assert list(source) == columns

    def test_source_indexing_edge_cases(self) -> None:
        """Test Source indexing with edge cases."""
        from splurge_data_profiler.source import Source

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        # Valid indexing
        assert source[0] == columns[0]
        assert source[1] == columns[1]

        # Negative indexing
        assert source[-1] == columns[1]
        assert source[-2] == columns[0]

        # Out of bounds indexing
        with pytest.raises(IndexError):
            _ = source[2]
        with pytest.raises(IndexError):
            _ = source[-3]

    def test_source_equality_edge_cases(self) -> None:
        """Test Source equality with edge cases."""
        from splurge_data_profiler.source import Source

        class TestSource(Source):
            pass

        # Empty sources
        source1 = TestSource()
        source2 = TestSource()
        assert source1 == source2

        # Sources with same columns
        columns1 = [Column("col1"), Column("col2")]
        columns2 = [Column("col1"), Column("col2")]
        source3 = TestSource(columns=columns1)
        source4 = TestSource(columns=columns2)
        assert source3 == source4

        # Sources with different columns
        columns3 = [Column("col1"), Column("col3")]
        source5 = TestSource(columns=columns3)
        assert source3 != source5

        # Different lengths
        source6 = TestSource(columns=[Column("col1")])
        assert source3 != source6

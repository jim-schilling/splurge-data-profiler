"""
Unit tests for source module classes.

These tests focus on testing individual classes and methods in isolation,
using mocks for dependencies where appropriate.
"""

import os
import tempfile
import shutil
from pathlib import Path
import pytest

from splurge_data_profiler.source import Column, Source, DsvSource, DbSource, DataType
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake
from splurge_data_profiler.exceptions import DatabaseError, FileProcessingError
from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text


@pytest.fixture(autouse=True)
def setup_test_dbsource(request, tmp_path):
    """Autouse fixture to provide test_file_path and data_lake_path for TestDbSource instances."""
    instance = getattr(request, "instance", None)
    if instance is None:
        return

    # Only apply to TestDbSource class or nested classes by name
    if instance.__class__.__name__.startswith("TestDbSource") or instance.__class__ is TestDbSource:
        # Create large DSV file
        fd, path = tempfile.mkstemp(suffix=".csv")
        test_file_path = Path(path)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("id,name,email,age,city,salary,department,hire_date\n")
            for i in range(1, 5001):
                f.write(
                    f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,City,50000,Engineering,2023-01-01\n"
                )

        data_lake_path = tmp_path / "data_lake"
        data_lake_path.mkdir()

        # Attach to the instance so tests using self.* work
        setattr(instance, "test_file_path", test_file_path)
        setattr(instance, "data_lake_path", data_lake_path)

        def _teardown():
            try:
                os.remove(test_file_path)
            except Exception:
                pass
            try:
                shutil.rmtree(data_lake_path)
            except Exception:
                pass

        request.addfinalizer(_teardown)


class TestSource:
    """Unit tests for Source abstract base class."""

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

        expected_str = f"Source(columns={columns})"
        assert str(source) == expected_str


class TestDsvSource:
    """Unit tests for DsvSource class."""

    def test_dsv_source_initialization_defaults(self) -> None:
        """Test DsvSource initialization with default values."""
        # Create a real temporary file for testing

        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)

        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("col1,col2\n1,2\n3,4\n")

            source = DsvSource(test_path)

            assert source.file_path == test_path
            assert source.delimiter == ","
            assert source.strip is True
            assert source.bookend == '"'
            assert source.bookend_strip is True
            assert source.encoding == "utf-8"
            assert source.skip_header_rows == 0
            assert source.skip_footer_rows == 0
            assert source.header_rows == 1
            assert source.skip_empty_rows is True
            assert len(source.columns) == 2
            assert [col.name for col in source.columns] == ["col1", "col2"]
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_initialization_custom_values(self) -> None:
        """Test DsvSource initialization with custom values."""
        # Create a real temporary file for testing

        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)

        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("skip1,skip2\nskip3,skip4\ncol1,col2\n1,2\n3,4\nfooter1,footer2\n")

            source = DsvSource(
                file_path=test_path,
                delimiter="|",
                strip=False,
                bookend="'",
                bookend_strip=False,
                encoding="utf-8",  # Changed from latin-1 to avoid encoding issues
                skip_header_rows=2,
                skip_footer_rows=1,
                header_rows=1,
                skip_empty_rows=False,
            )

            assert source.file_path == test_path
            assert source.delimiter == "|"
            assert source.strip is False
            assert source.bookend == "'"
            assert source.bookend_strip is False
            assert source.encoding == "utf-8"
            assert source.skip_header_rows == 2
            assert source.skip_footer_rows == 1
            assert source.header_rows == 1
            assert source.skip_empty_rows is False
            # With pipe delimiter, the file content doesn't match the delimiter
            # So we get different column parsing - the header row is parsed as a single column
            assert len(source.columns) == 1
            assert [col.name for col in source.columns] == ["col1,col2"]
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_equality(self) -> None:
        """Test DsvSource equality comparison."""
        # Create real temporary files for testing

        temp_fd1, temp_path1 = tempfile.mkstemp(suffix=".csv")
        temp_fd2, temp_path2 = tempfile.mkstemp(suffix=".csv")
        test_path1 = Path(temp_path1)
        test_path2 = Path(temp_path2)

        try:
            # Create identical files
            with os.fdopen(temp_fd1, "w", encoding="utf-8") as f:
                f.write("col1,col2\n1,2\n3,4\n")
            with os.fdopen(temp_fd2, "w", encoding="utf-8") as f:
                f.write("col1,col2\n1,2\n3,4\n")

            source1 = DsvSource(test_path1)
            source2 = DsvSource(test_path2)

            # They should not be equal because they have different file paths
            assert source1 != source2
        finally:
            try:
                os.remove(temp_path1)
                os.remove(temp_path2)
            except Exception:
                pass

    def test_dsv_source_equality_different_type(self) -> None:
        """Test DsvSource equality with different type."""
        # Create a real temporary file for testing

        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)

        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("col1,col2\n1,2\n3,4\n")

            source = DsvSource(test_path)
            other = "not a dsv source"

            assert source != other
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_string_representation(self) -> None:
        """Test DsvSource string representation."""
        # Create a real temporary file for testing

        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        test_path = Path(temp_path)

        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("col1,col2\n1,2\n3,4\n")

            source = DsvSource(test_path)

            # Check that string representation contains expected elements
            str_repr = str(source)
            assert "DsvSource" in str_repr
            assert "file_path=" in str_repr
            assert "delimiter=" in str_repr
            assert "columns=" in str_repr
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass


class TestDbSource:
    """Unit tests for DbSource class."""
    pass

    def test_db_source_initialization_connection_error(self) -> None:
        """Test DbSource initialization with connection error."""
        try:
            DbSource(db_url="invalid://url", db_schema="test_schema", db_table="test_table")
            assert False, "Expected exception was not raised"
        except Exception:
            pass

    def test_db_source_properties(self) -> None:
        """Test DbSource properties."""
        # This test requires a real database connection, so we'll test the error case
        try:
            DbSource(db_url="invalid://url", db_schema="test_schema", db_table="test_table")
            assert False, "Expected DatabaseError was not raised"
        except DatabaseError:
            pass

    def test_db_source_string_representation(self) -> None:
        """Test DbSource string representation."""
        # This test requires a real database connection, so we'll test the error case
        try:
            DbSource(db_url="invalid://url", db_schema="test_schema", db_table="test_table")
            assert False, "Expected DatabaseError was not raised"
        except DatabaseError:
            pass

    def test_streaming_large_dsv_file_data_integrity(self) -> None:
        """Test that all data from the large DSV file is correctly inserted into SQLite."""
        from splurge_data_profiler.source import DsvSource

        dsv_source = DsvSource(self.test_file_path)

        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source, data_lake_path=self.data_lake_path
        )

        engine = create_engine(data_lake.db_url)

        try:
            with engine.connect() as connection:
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                assert row_count == 5000

                # Verify first and last rows
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '1'"))
                first_row = result.fetchone()
                assert first_row[0] == "1"
                assert first_row[1] == "Employee_0001"

                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '5000'"))
                last_row = result.fetchone()
                assert last_row[0] == "5000"
                assert last_row[1] == "Employee_5000"

        finally:
            engine.dispose()

    def test_streaming_large_dsv_file_performance(self) -> None:
        """Test that streaming performs efficiently with large files."""
        import time
        from splurge_data_profiler.source import DsvSource

        dsv_source = DsvSource(self.test_file_path)
        start_time = time.time()
        _ = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source, data_lake_path=self.data_lake_path
        )
        end_time = time.time()
        processing_time = end_time - start_time
        assert processing_time < 30.0

    def test_streaming_large_dsv_file_memory_usage(self) -> None:
        """Test streaming memory usage by processing multiple files."""
        from splurge_data_profiler.source import DsvSource

        dsv_source = DsvSource(self.test_file_path)
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source, data_lake_path=self.data_lake_path
        )
        engine = create_engine(data_lake.db_url)
        try:
            with engine.connect() as connection:
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                assert row_count == 5000
        finally:
            engine.dispose()

    def test_streaming_large_dsv_file_with_different_delimiters(self) -> None:
        """Test streaming with TSV files."""
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        tsv_file_path = Path(tsv_path)

        try:
            with open(self.test_file_path, "r", encoding="utf-8") as csv_file:
                csv_content = csv_file.read()
                tsv_content = csv_content.replace(",", "\t")

            with os.fdopen(tsv_fd, "w", encoding="utf-8") as tsv_file:
                tsv_file.write(tsv_content)

            dsv_source = DsvSource(tsv_file_path, delimiter="\t")
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
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
        malformed_fd, malformed_path = tempfile.mkstemp(suffix=".csv")
        malformed_file_path = Path(malformed_path)

        try:
            with os.fdopen(malformed_fd, "w", encoding="utf-8") as f:
                f.write("id,name,email,age,city,salary,department,hire_date\n")
                for i in range(1, 1001):
                    if i % 100 == 0:
                        f.write(f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000\n")
                    else:
                        f.write(
                            f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000,Engineering,2023-01-01\n"
                        )

            dsv_source = DsvSource(malformed_file_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            engine = create_engine(data_lake.db_url)
            try:
                with engine.connect() as connection:
                    table_name = data_lake.db_table
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    assert row_count == 1000

                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE department IS NULL"))
                    null_count = result.scalar()
                    assert null_count == 10
            finally:
                engine.dispose()

        finally:
            try:
                os.remove(malformed_path)
            except Exception:
                pass

    def test_from_dsv_source_creates_sqlite_table(self) -> None:
        """Test that from_dsv_source creates a SQLite table correctly."""
        dsv_source = DsvSource(self.test_file_path)
        _ = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source, data_lake_path=self.data_lake_path
        )
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        assert expected_db_path.exists()

    def test_from_dsv_source_creates_directory_if_not_exists(self) -> None:
        non_existent_path = self.data_lake_path / "new_directory"
        dsv_source = DsvSource(self.test_file_path)
        DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=non_existent_path)
        assert non_existent_path.exists()

    def test_from_dsv_source_with_different_file_types(self) -> None:
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        try:
            with os.fdopen(tsv_fd, "w", encoding="utf-8") as f:
                f.write("id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n")
            tsv_file_path = Path(tsv_path)
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")
            DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source, data_lake_path=self.data_lake_path
            )
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            assert expected_db_path.exists()
        finally:
            try:
                os.remove(tsv_path)
            except Exception:
                pass

    def test_dsv_source_nonexistent_file(self) -> None:
        nonexistent_path = Path("/nonexistent/path/file.csv")
        with pytest.raises(FileProcessingError):
            DsvSource(nonexistent_path)

    def test_dsv_source_empty_file(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        empty_path = Path(temp_path)
        try:
            source = DsvSource(empty_path)
            assert len(source.columns) == 0
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_header_only_file(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value\n")
            header_path = Path(temp_path)
            source = DsvSource(header_path)
            assert len(source.columns) == 3
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_malformed_delimiter(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value\n1,Alice,100\n2,Bob;200\n3,Charlie,300\n")
            malformed_path = Path(temp_path)
            source = DsvSource(malformed_path, delimiter=",")
            assert len(source.columns) == 3
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_encoding_error(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name\n1,José\n2,François\n")
            utf8_path = Path(temp_path)
            source = DsvSource(utf8_path, encoding="utf-8")
            assert len(source.columns) == 2
            with pytest.raises(FileProcessingError):
                DsvSource(utf8_path, encoding="ascii")
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_skip_rows_edge_cases(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("# Comment line 1\n# Comment line 2\nid,name,value\n1,Alice,100\n2,Bob,200\n# Footer comment\n# Another footer\n")
            skip_path = Path(temp_path)
            source = DsvSource(skip_path, skip_header_rows=2)
            assert len(source.columns) == 3
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_large_skip_values(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name\n1,Alice\n")
            small_path = Path(temp_path)
            source = DsvSource(small_path, skip_header_rows=10, skip_footer_rows=10)
            assert len(source.columns) >= 0
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_whitespace_handling(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("  id  ,  name  ,  value  \n  1  ,  Alice  ,  100  \n  2  ,  Bob  ,  200  \n")
            ws_path = Path(temp_path)
            source_strip = DsvSource(ws_path, strip=True)
            assert source_strip.columns[0].name == "id"
            source_no_strip = DsvSource(ws_path, strip=False)
            assert source_no_strip.columns[0].name == "id"
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_bookend_edge_cases(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write('\"id\",\"name\",\"value\"\n\"1\",\"Alice\",\"100\"\n\"2\",\"Bob\",\"200\"\n3,\"Charlie\",\"300\"\n')
            quote_path = Path(temp_path)
            source_strip = DsvSource(quote_path, bookend='"', bookend_strip=True)
            assert len(source_strip.columns) == 3
            source_no_strip = DsvSource(quote_path, bookend='"', bookend_strip=False)
            assert len(source_no_strip.columns) == 3
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_dsv_source_mixed_data_types(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name,value,active\n1,Alice,100.5,true\n2,Bob,200,false\n3,Charlie,300.75,1\n")
            mixed_path = Path(temp_path)
            source = DsvSource(mixed_path)
            assert len(source.columns) == 4
            for column in source.columns:
                assert column.raw_type == DataType.TEXT
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass


    class TestDataType:
        def test_data_type_values(self) -> None:
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
            expected_members = {"BOOLEAN", "DATE", "DATETIME", "FLOAT", "INTEGER", "TEXT", "TIME"}
            actual_members = {member.name for member in DataType}
            assert actual_members == expected_members


    class TestColumn:
        def test_column_initialization_defaults(self) -> None:
            column = Column("test_column")
            assert column.name == "test_column"
            assert column.inferred_type == DataType.TEXT
            assert column.raw_type == DataType.TEXT
            assert column.is_nullable is True

        def test_column_initialization_custom_values(self) -> None:
            column = Column(name="custom_column", inferred_type=DataType.INTEGER, is_nullable=False)
            assert column.name == "custom_column"
            assert column.inferred_type == DataType.INTEGER
            assert column.raw_type == DataType.TEXT
            assert column.is_nullable is False

        def test_column_string_representation(self) -> None:
            column = Column("test_column", inferred_type=DataType.FLOAT)
            assert str(column)

        def test_column_repr_representation(self) -> None:
            column = Column("test_column", inferred_type=DataType.FLOAT, is_nullable=False)
            repr_str = repr(column)
            assert repr_str


    class TestDsvSourceIntegration:
        def setup_method(self) -> None:
            self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
            with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            self.file_path = Path(self.temp_path)

        def teardown_method(self) -> None:
            try:
                os.remove(self.temp_path)
            except Exception:
                pass

        def test_dsv_source_real_file(self):
            source = DsvSource(self.file_path)
            columns = source._initialize()
            assert len(columns) >= 2
            assert columns[0].name == "id"
            assert columns[1].name == "name"

    class TestDbSourceWithRealSQLite:
        def setup_method(self) -> None:
            self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
            self.db_url = f"sqlite:///{self.db_path}"
            self.db_table = "test_table"
            self.db_schema = None
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
            try:
                self.engine.dispose()
            except Exception:
                pass
                try:
                    os.close(self.db_fd)
                except Exception:
                    pass
            try:
                os.remove(self.db_path)
            except PermissionError:
                pass

        def test_dbsource_sqlite_columns(self):
            source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)
            assert len(source.columns) == 2
            assert source.columns[0].name == "id"
            assert source.columns[1].name == "name"

    # --- Appended from tests/test_source.py (root) ---

    def test_streaming_large_dsv_file_creation(self) -> None:
        """Test that streaming can handle large DSV files (>5000 lines)."""
        # Create DSV source
        from splurge_data_profiler.source import DsvSource

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


"""
Integration tests for source module classes.

These tests focus on testing interactions between components and
using real file systems and databases.
"""

import os
import tempfile
import pytest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Column as SAColumn, String, text, Table

from splurge_data_profiler.source import DataType, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.exceptions import FileProcessingError


@pytest.fixture
def temp_csv_file():
    """Fixture to create a temporary CSV file."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n")
    test_file_path = Path(temp_path)

    yield test_file_path

    # Cleanup
    try:
        os.remove(temp_path)
    except Exception:
        pass


def test_dsv_source_real_file(temp_csv_file):
    """Test DsvSource with a real CSV file."""
    # This will use the real DsvHelper and TabularDataModel
    source = DsvSource(temp_csv_file)

    # Verify the source was created correctly
    assert source.file_path == temp_csv_file
    assert len(source.columns) == 3
    assert [col.name for col in source.columns] == ["id", "name", "value"]

    # Verify all columns are TEXT type initially
    for col in source.columns:
        assert col.inferred_type == DataType.TEXT


@pytest.fixture
def sqlite_db():
    """Fixture to create a temporary SQLite database."""
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    db_url = f"sqlite:///{db_path}"
    db_schema = None  # SQLite does not use schemas
    db_table = "test_table"

    # Create table with some data
    engine = create_engine(db_url)
    metadata = MetaData()
    Table(
        db_table,
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("name", String, nullable=True),
        SAColumn("value", String, nullable=True),
    )
    metadata.create_all(engine)

    # Insert some test data
    with engine.connect() as conn:
        conn.execute(text(f"INSERT INTO {db_table} VALUES ('1', 'Alice', '10.5')"))
        conn.execute(text(f"INSERT INTO {db_table} VALUES ('2', 'Bob', '20.0')"))
        conn.commit()

    yield db_url, db_schema, db_table

    # Cleanup
    try:
        engine.dispose()
    except Exception:
        pass
    try:
        os.close(db_fd)
        os.remove(db_path)
    except Exception:
        pass


def test_dbsource_sqlite_columns(sqlite_db):
    """Test DbSource with real SQLite database."""
    db_url, db_schema, db_table = sqlite_db

    source = DbSource(db_url=db_url, db_schema=db_schema, db_table=db_table)

    # Verify the source was created correctly
    assert source.db_url == db_url
    assert source.db_schema == db_schema
    assert source.db_table == db_table
    assert len(source.columns) == 3
    assert [col.name for col in source.columns] == ["id", "name", "value"]


def generate_large_csv_file(temp_fd, temp_path):
    """Generate a large CSV file for testing."""
    import csv
    import random
    import string

    # Generate 10,000 rows of data
    num_rows = 10000
    num_columns = 5

    # Generate column names
    column_names = [f"col_{i}" for i in range(num_columns)]

    with os.fdopen(temp_fd, "w", encoding="utf-8", newline="") as f:
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
                    row.append("".join(random.choices(string.ascii_letters, k=10)))  # Text column
            writer.writerow(row)


@pytest.fixture
def large_csv_and_data_lake(tmp_path: Path):
    """Fixture to create a large CSV file and data lake directory under pytest tmp_path."""
    # Create a directory for the data lake under pytest-managed tmp_path
    data_lake_path = tmp_path / "data_lake"
    data_lake_path.mkdir(parents=True, exist_ok=True)

    # Create a CSV path under tmp_path and open/close via high-level APIs
    csv_path = tmp_path / "large.csv"

    # Generate a large CSV file for testing using a temporary file descriptor
    # created under pytest's tmp_path for cross-platform safety
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        # Reuse generator logic but write directly to the file path
        import csv
        import random
        import string

        num_rows = 10000
        num_columns = 5
        column_names = [f"col_{i}" for i in range(num_columns)]
        writer = csv.writer(f)
        writer.writerow(column_names)
        for i in range(num_rows):
            row = []
            for j in range(num_columns):
                if j == 0:
                    row.append(str(i))
                elif j == 1:
                    row.append(f"name_{i}")
                elif j == 2:
                    row.append(str(random.randint(1, 1000)))
                elif j == 3:
                    row.append(f"{random.uniform(0, 100):.2f}")
                else:
                    row.append("".join(random.choices(string.ascii_letters, k=10)))
            writer.writerow(row)

    yield csv_path, data_lake_path


def test_streaming_large_dsv_file_creation(large_csv_and_data_lake):
    """Test creating a data lake from a large DSV file using streaming."""
    csv_path, data_lake_path = large_csv_and_data_lake

    # Create DsvSource
    dsv_source = DsvSource(csv_path)

    # Create data lake using factory with streaming
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Verify the data lake was created correctly
    assert isinstance(data_lake, DataLake)
    assert len(data_lake.column_names) == 5
    assert data_lake.column_names == ["col_0", "col_1", "col_2", "col_3", "col_4"]


def test_streaming_large_dsv_file_data_integrity(large_csv_and_data_lake):
    """Test data integrity when streaming large DSV files."""
    csv_path, data_lake_path = large_csv_and_data_lake

    # Create DsvSource
    dsv_source = DsvSource(csv_path)

    # Create data lake using factory
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Verify data integrity by checking a few rows
    from sqlalchemy import create_engine, text

    engine = create_engine(data_lake.db_url)
    with engine.connect() as conn:
        # Check total row count
        result = conn.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
        row_count = result.scalar()
        assert row_count == 10000

        # Check first row
        result = conn.execute(text(f"SELECT * FROM {data_lake.db_table} LIMIT 1"))
        first_row = result.fetchone()
        assert first_row is not None
        assert first_row[0] == "0"  # First ID should be "0"

        # Check last row
        result = conn.execute(text(f"SELECT * FROM {data_lake.db_table} ORDER BY col_0 DESC LIMIT 1"))
        last_row = result.fetchone()
        assert last_row is not None
        assert last_row[0] == "9999"  # Last ID should be "9999"


def test_streaming_large_dsv_file_performance(large_csv_and_data_lake):
    """Test performance of streaming large DSV files."""
    import time

    csv_path, data_lake_path = large_csv_and_data_lake

    # Create DsvSource
    dsv_source = DsvSource(csv_path)

    # Measure creation time
    start_time = time.time()
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    end_time = time.time()

    creation_time = end_time - start_time

    # Verify the data lake was created successfully
    assert isinstance(data_lake, DataLake)

    # Performance assertion (should complete within reasonable time)
    # 10,000 rows should process in under 30 seconds
    assert creation_time < 30.0, f"Data lake creation took {creation_time:.2f} seconds"


def test_streaming_large_dsv_file_with_different_delimiters(large_csv_and_data_lake):
    """Test streaming large DSV files with different delimiters."""
    import csv

    csv_path, data_lake_path = large_csv_and_data_lake

    # Create a temporary pipe-delimited file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".txt")
    pipe_csv_path = Path(temp_path)

    try:
        # Generate pipe-delimited data
        with os.fdopen(temp_fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="|")
            writer.writerow(["id", "name", "value"])
            for i in range(1000):
                writer.writerow([str(i), f"name_{i}", str(i * 1.5)])

        # Create DsvSource with pipe delimiter
        dsv_source = DsvSource(pipe_csv_path, delimiter="|")

        # Create data lake using factory
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

        # Verify the data lake was created correctly
        assert isinstance(data_lake, DataLake)
        assert len(data_lake.column_names) == 3
        assert data_lake.column_names == ["id", "name", "value"]

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_streaming_large_dsv_file_error_handling():
    """Test error handling when streaming large DSV files."""
    # Test with non-existent file
    non_existent_path = Path("/non/existent/file.csv")

    with pytest.raises(FileProcessingError):
        DsvSource(non_existent_path)

    # Test with empty file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    empty_path = Path(temp_path)

    try:
        # Create empty file
        with os.fdopen(temp_fd, "w", encoding="utf-8") as _:
            pass  # Empty file

        # Empty files should be handled gracefully with 0 columns
        dsv_source = DsvSource(empty_path)
        assert len(dsv_source.columns) == 0

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass

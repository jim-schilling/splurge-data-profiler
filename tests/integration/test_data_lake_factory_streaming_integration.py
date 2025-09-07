"""
Integration tests for DataLakeFactory streaming functionality.

These tests focus on testing DataLakeFactory with large files and real file systems,
validating streaming performance and data integrity without mocking.
"""

import os
import random
import tempfile
import pytest
from pathlib import Path

from sqlalchemy import create_engine, text

from splurge_data_profiler.source import DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake


def generate_large_csv_file(temp_fd, temp_path):
    """Generate a CSV file with more than 5000 lines of test data."""
    # Define column headers
    headers = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]

    # Generate random data
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "IT"]

    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        # Write header
        f.write(",".join(headers) + "\n")

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
            f.write(",".join(row_data) + "\n")


@pytest.fixture
def large_csv_and_data_lake():
    """Fixture to create a large CSV file and data lake directory."""
    # Create a temporary CSV file with more than 5000 lines
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    test_file_path = Path(temp_path)

    # Create a temporary directory for the data lake
    temp_dir = tempfile.mkdtemp()
    data_lake_path = Path(temp_dir)

    # Generate large dataset
    generate_large_csv_file(temp_fd, temp_path)

    yield test_file_path, data_lake_path

    # Cleanup
    try:
        os.remove(temp_path)
    except Exception:
        pass
    try:
        import shutil

        shutil.rmtree(temp_dir)
    except Exception:
        pass


def test_streaming_large_dsv_file_creation(large_csv_and_data_lake):
    """Test that streaming can handle large DSV files (>5000 lines)."""
    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Verify the source was created correctly
    assert len(dsv_source.columns) == 8
    expected_columns = ["id", "name", "email", "age", "city", "salary", "department", "hire_date"]
    actual_columns = [col.name for col in dsv_source.columns]
    assert actual_columns == expected_columns

    # Create data lake using streaming
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Verify the data lake was created
    assert isinstance(data_lake, DataLake)
    assert isinstance(data_lake.db_source, DbSource)

    # Verify the SQLite file was created
    expected_db_path = data_lake_path / f"{test_file_path.stem}.sqlite"
    assert expected_db_path.exists()

    # Verify the database URL is correct
    expected_db_url = f"sqlite:///{expected_db_path}"
    assert data_lake.db_url == expected_db_url

    # Verify the table name is correct
    expected_table_name = test_file_path.stem
    assert data_lake.db_table == expected_table_name

    # Verify the schema is None (SQLite doesn't use schemas)
    assert data_lake.db_schema is None

    # Verify the column names are preserved
    assert data_lake.column_names == expected_columns


def test_streaming_large_dsv_file_data_integrity(large_csv_and_data_lake):
    """Test that all data from the large DSV file is correctly inserted into SQLite."""
    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake using streaming
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Connect to the created database and verify data integrity
    engine = create_engine(data_lake.db_url)

    try:
        with engine.connect() as connection:
            # Count total rows
            table_name = data_lake.db_table
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()

            # Should have 5500 rows (excluding header)
            assert row_count == 5500

            # Verify first row
            result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '1'"))
            first_row = result.fetchone()
            assert first_row is not None
            assert first_row[0] == "1"  # id
            assert first_row[1] == "Employee_0001"  # name
            assert first_row[2] == "employee_0001@company.com"  # email

            # Verify last row
            result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '5500'"))
            last_row = result.fetchone()
            assert last_row is not None
            assert last_row[0] == "5500"  # id
            assert last_row[1] == "Employee_5500"  # name
            assert last_row[2] == "employee_5500@company.com"  # email

            # Verify data types and constraints
            result = connection.execute(text(f"PRAGMA table_info({table_name})"))
            columns_info = result.fetchall()

            # Should have 8 columns
            assert len(columns_info) == 8

            # All columns should be TEXT/VARCHAR type (as per our implementation)
            for col_info in columns_info:
                assert col_info[2] in ["TEXT", "VARCHAR"]  # type column

            # Verify some random rows for data integrity
            for i in range(1, 11):
                row_id = random.randint(1, 5500)
                result = connection.execute(text(f"SELECT * FROM {table_name} WHERE id = '{row_id}'"))
                row = result.fetchone()
                assert row is not None
                assert row[0] == str(row_id)
                assert row[1] == f"Employee_{row_id:04d}"
                assert row[2] == f"employee_{row_id:04d}@company.com"

    finally:
        engine.dispose()


def test_streaming_large_dsv_file_performance(large_csv_and_data_lake):
    """Test that streaming performs efficiently with large files."""
    import time

    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Measure processing time
    start_time = time.time()

    # Create data lake using streaming
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    end_time = time.time()
    processing_time = end_time - start_time

    # Verify the operation completed successfully
    assert isinstance(data_lake, DataLake)

    # Performance assertion: should complete within reasonable time
    # (adjust threshold based on system capabilities)
    assert processing_time < 30.0, f"Processing took {processing_time:.2f} seconds, which is too slow"

    # Verify file size is reasonable (should be larger than original CSV due to SQLite overhead)
    expected_db_path = data_lake_path / f"{test_file_path.stem}.sqlite"
    assert expected_db_path.exists()

    csv_size = test_file_path.stat().st_size
    db_size = expected_db_path.stat().st_size

    # SQLite file should be larger than CSV due to indexing and structure
    assert db_size > csv_size * 0.5  # At least 50% of CSV size


def test_streaming_large_dsv_file_memory_usage(large_csv_and_data_lake):
    """Test that streaming can handle large files without memory issues."""
    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake using streaming
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Verify the operation completed successfully
    assert isinstance(data_lake, DataLake)

    # Verify the SQLite file was created
    expected_db_path = data_lake_path / f"{test_file_path.stem}.sqlite"
    assert expected_db_path.exists()

    # Verify data integrity by checking row count
    engine = create_engine(data_lake.db_url)
    try:
        with engine.connect() as connection:
            table_name = data_lake.db_table
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
            assert row_count == 5500
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
            data_lake_2 = DataLakeFactory.from_dsv_source(dsv_source=dsv_source_2, data_lake_path=data_lake_path)

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


def test_streaming_large_dsv_file_with_different_delimiters(large_csv_and_data_lake):
    """Test streaming with different delimiters (TSV format)."""
    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create a TSV file with the same data
    tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
    tsv_file_path = Path(tsv_path)

    try:
        # Copy the CSV content but replace commas with tabs
        with open(test_file_path, "r", encoding="utf-8") as csv_file:
            csv_content = csv_file.read()
            tsv_content = csv_content.replace(",", "\t")

        with os.fdopen(tsv_fd, "w", encoding="utf-8") as tsv_file:
            tsv_file.write(tsv_content)

        # Create DSV source with tab delimiter
        dsv_source = DsvSource(tsv_file_path, delimiter="\t")

        # Create data lake using streaming
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

        # Verify the data lake was created
        assert isinstance(data_lake, DataLake)

        # Verify the SQLite file was created with the correct name
        expected_db_path = data_lake_path / f"{tsv_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        # Verify data integrity
        engine = create_engine(data_lake.db_url)
        try:
            with engine.connect() as connection:
                table_name = data_lake.db_table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                assert row_count == 5500
        finally:
            engine.dispose()

    finally:
        try:
            os.remove(tsv_path)
        except Exception:
            pass


def test_streaming_large_dsv_file_error_handling(large_csv_and_data_lake):
    """Test error handling with malformed large files."""
    test_file_path, data_lake_path = large_csv_and_data_lake

    # Create a malformed CSV file (missing some values)
    malformed_fd, malformed_path = tempfile.mkstemp(suffix=".csv")
    malformed_file_path = Path(malformed_path)

    try:
        with os.fdopen(malformed_fd, "w", encoding="utf-8") as f:
            f.write("id,name,email,age,city,salary,department,hire_date\n")
            # Add some malformed rows
            for i in range(1, 1001):
                if i % 100 == 0:  # Every 100th row is malformed
                    f.write(f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000\n")  # Missing values
                else:
                    f.write(
                        f"{i},Employee_{i:04d},employee_{i:04d}@company.com,25,New York,50000,Engineering,2023-01-01\n"
                    )

        # Create DSV source
        dsv_source = DsvSource(malformed_file_path)

        # This should still work as our implementation handles missing values
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

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

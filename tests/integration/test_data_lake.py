"""
Integration tests for DataLake class.

These tests focus on testing DataLake with real file systems and database operations,
validating end-to-end functionality without mocking.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake


@pytest.fixture
def temp_files():
    """Set up test fixtures."""
    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n")
    test_file_path = Path(temp_path)

    # Create a temporary directory for the data lake
    temp_dir = tempfile.mkdtemp()
    data_lake_path = Path(temp_dir)

    yield test_file_path, data_lake_path

    # Clean up test fixtures
    try:
        os.remove(temp_path)
    except Exception:
        pass
    try:
        import shutil

        shutil.rmtree(temp_dir)
    except Exception:
        pass


def test_data_lake_from_factory(temp_files) -> None:
    """Test DataLake creation through DataLakeFactory."""
    test_file_path, data_lake_path = temp_files

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake using factory
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # Test DataLake properties
    assert isinstance(data_lake, DataLake)
    assert isinstance(data_lake.db_source, DbSource)
    assert data_lake.column_names == ["id", "name", "value"]
    assert data_lake.db_table == test_file_path.stem
    assert data_lake.db_schema is None  # SQLite doesn't use schemas

    # Test string representation
    expected_str = f"DataLake(db_url={data_lake.db_url}, schema=None, table={data_lake.db_table}, columns=3)"
    assert str(data_lake) == expected_str


def test_data_lake_equality_with_factory_created(temp_files) -> None:
    """Test DataLake equality with factory-created instances."""
    test_file_path, data_lake_path = temp_files

    # Create two data lakes from the same source
    dsv_source = DsvSource(test_file_path)

    data_lake1 = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    data_lake2 = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

    # They should be equal since they have the same configuration
    assert data_lake1 == data_lake2


def test_data_lake_empty_dsv(temp_files):
    """Test DataLake creation from an empty DSV file."""
    _, data_lake_path = temp_files

    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n")  # Only header, no data
    dsv_source = DsvSource(temp_path)
    # Should not raise, but will create an empty table
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    os.remove(temp_path)


def test_data_lake_dsv_missing_columns(temp_files):
    """Test DataLake creation from DSV with missing columns."""
    _, data_lake_path = temp_files

    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n1\n2,Bob\n")
    dsv_source = DsvSource(temp_path)
    # Should not raise, but will have None for missing values
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    os.remove(temp_path)


def test_data_lake_dsv_extra_columns(temp_files):
    """Test DataLake creation from DSV with extra columns in data rows."""
    _, data_lake_path = temp_files

    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n1,Alice,Extra\n2,Bob\n")
    dsv_source = DsvSource(temp_path)
    # Should not raise, extra columns are ignored
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    os.remove(temp_path)


def test_data_lake_batch_size_edge_case(temp_files):
    """Test DataLake batch insertion with minimum batch_size (edge case)."""
    _, data_lake_path = temp_files

    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n1,Alice\n2,Bob\n")
    dsv_source = DsvSource(temp_path)
    # Patch DataLakeFactory to use minimum batch_size
    orig_stream = DataLakeFactory._stream_dsv_to_sqlite

    def patched_stream(*args, **kwargs):
        return orig_stream(*args, **kwargs, batch_size=100)

    DataLakeFactory._stream_dsv_to_sqlite, orig = patched_stream, DataLakeFactory._stream_dsv_to_sqlite
    try:
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
        assert isinstance(data_lake, DataLake)
    finally:
        DataLakeFactory._stream_dsv_to_sqlite = orig
    os.remove(temp_path)


# --- Merged from test_data_lake_factory.py (unique tests) ---
def test_from_dsv_source_creates_sqlite_table(temp_files) -> None:
    """Test that from_dsv_source creates a SQLite table correctly."""
    test_file_path, data_lake_path = temp_files

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake
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
    expected_columns = ["id", "name", "value"]
    assert data_lake.column_names == expected_columns

    # Verify the columns in the database source
    db_columns = data_lake.db_source.columns
    assert len(db_columns) == 3
    assert db_columns[0].name == "id"
    assert db_columns[1].name == "name"
    assert db_columns[2].name == "value"


def test_from_dsv_source_creates_directory_if_not_exists(temp_files) -> None:
    """Test that from_dsv_source creates the data lake directory if it doesn't exist."""
    test_file_path, data_lake_path = temp_files

    # Create a non-existent directory path
    non_existent_path = data_lake_path / "new_directory"

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake
    DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=non_existent_path)

    # Verify the directory was created
    assert non_existent_path.exists()
    assert non_existent_path.is_dir()

    # Verify the SQLite file was created in the new directory
    expected_db_path = non_existent_path / f"{test_file_path.stem}.sqlite"
    assert expected_db_path.exists()


def test_from_dsv_source_with_different_file_types(temp_files) -> None:
    """Test that from_dsv_source works with different file extensions."""
    test_file_path, data_lake_path = temp_files

    # Create a TSV file
    tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
    try:
        with os.fdopen(tsv_fd, "w", encoding="utf-8") as f:
            f.write("id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n")

        tsv_file_path = Path(tsv_path)

        # Create DSV source with tab delimiter
        dsv_source = DsvSource(tsv_file_path, delimiter="\t")

        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)

        # Verify the SQLite file was created with the correct name
        expected_db_path = data_lake_path / f"{tsv_file_path.stem}.sqlite"
        assert expected_db_path.exists()

        # Verify the table name is correct (without .tsv extension)
        expected_table_name = tsv_file_path.stem
        assert data_lake.db_table == expected_table_name

    finally:
        try:
            os.remove(tsv_path)
        except Exception:
            pass


# --- End merged content ---


if __name__ == "__main__":
    pytest.main([__file__])

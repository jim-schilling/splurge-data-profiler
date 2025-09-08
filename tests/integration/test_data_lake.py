"""
Integration tests for DataLake class.

These tests focus on testing DataLake with real file systems and database operations,
validating end-to-end functionality without mocking.
"""

import os
import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake


@pytest.fixture
def temp_files(tmp_path: Path):
    """Set up test fixtures using pytest tmp_path."""
    # Create a temporary CSV file under pytest tmp_path
    test_file_path = tmp_path / "test.csv"
    test_file_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n", encoding="utf-8")

    # Create a temporary directory for the data lake under tmp_path
    data_lake_path = tmp_path / "data_lake"
    data_lake_path.mkdir(parents=True, exist_ok=True)

    yield test_file_path, data_lake_path


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
    empty_file = data_lake_path / "empty.csv"
    empty_file.write_text("id,name\n", encoding="utf-8")
    dsv_source = DsvSource(empty_file)
    # Should not raise, but will create an empty table
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    # file created under data_lake_path/tmp handled by pytest tmp_path


def test_data_lake_dsv_missing_columns(temp_files):
    """Test DataLake creation from DSV with missing columns."""
    _, data_lake_path = temp_files
    missing_file = data_lake_path / "missing.csv"
    missing_file.write_text("id,name\n1\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(missing_file)
    # Should not raise, but will have None for missing values
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    # file created under data_lake_path/tmp handled by pytest tmp_path


def test_data_lake_dsv_extra_columns(temp_files):
    """Test DataLake creation from DSV with extra columns in data rows."""
    _, data_lake_path = temp_files
    extra_file = data_lake_path / "extra.csv"
    extra_file.write_text("id,name\n1,Alice,Extra\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(extra_file)
    # Should not raise, extra columns are ignored
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    # file created under data_lake_path/tmp handled by pytest tmp_path


def test_data_lake_batch_size_edge_case(temp_files):
    """Test DataLake batch insertion with minimum batch_size (edge case)."""
    _, data_lake_path = temp_files
    batch_file = data_lake_path / "batch.csv"
    batch_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(batch_file)
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
    # no explicit removal needed; files live under pytest tmp_path


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

    # Create a TSV file under the pytest temp directory
    tsv_file_path = data_lake_path / "sample.tsv"
    tsv_file_path.write_text("id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n", encoding="utf-8")

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


# --- End merged content ---


if __name__ == "__main__":
    pytest.main([__file__])

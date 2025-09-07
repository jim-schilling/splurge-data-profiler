"""
Integration tests for DataLakeFactory class.

These tests focus on testing DataLakeFactory with real file systems and database operations,
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
    # Create a temporary CSV file for testing
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n')
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


def test_from_dsv_source_creates_sqlite_table(temp_files) -> None:
    """Test that from_dsv_source creates a SQLite table correctly."""
    test_file_path, data_lake_path = temp_files

    # Create DSV source
    dsv_source = DsvSource(test_file_path)

    # Create data lake
    data_lake = DataLakeFactory.from_dsv_source(
        dsv_source=dsv_source,
        data_lake_path=data_lake_path
    )

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
    DataLakeFactory.from_dsv_source(
        dsv_source=dsv_source,
        data_lake_path=non_existent_path
    )

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
        with os.fdopen(tsv_fd, 'w', encoding='utf-8') as f:
            f.write('id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n')

        tsv_file_path = Path(tsv_path)

        # Create DSV source with tab delimiter
        dsv_source = DsvSource(tsv_file_path, delimiter="\t")

        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=data_lake_path
        )

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


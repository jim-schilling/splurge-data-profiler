import os
import tempfile
import pytest

from sqlalchemy import create_engine, MetaData, Column as SAColumn, String, Table

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.data_lake import DataLake


@pytest.fixture
def temp_sqlite_data_lake():
    """Create a temporary SQLite database and DataLake for testing."""
    # Create a temporary SQLite database file
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    db_url = f"sqlite:///{db_path}"
    db_table = "test_table"
    db_schema = None  # SQLite does not use schemas

    # Create table
    engine = create_engine(db_url)
    metadata = MetaData()
    Table(
        db_table,
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
    metadata.create_all(engine)

    # Create DbSource and DataLake
    db_source = DbSource(db_url=db_url, db_schema=db_schema, db_table=db_table)
    data_lake = DataLake(db_source=db_source)

    yield data_lake, db_source, db_url, db_schema, db_table

    # Cleanup
    try:
        engine.dispose()
    except Exception:
        pass
    os.close(db_fd)
    try:
        os.remove(db_path)
    except PermissionError:
        pass


def test_data_lake_initialization(temp_sqlite_data_lake):
    """Test DataLake initialization."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    assert isinstance(data_lake, DataLake)
    assert data_lake.db_source == db_source
    assert data_lake.db_url == db_url
    assert data_lake.db_schema == db_schema
    assert data_lake.db_table == db_table
    assert data_lake.column_names == ["id", "name"]


def test_data_lake_string_representation(temp_sqlite_data_lake):
    """Test DataLake string representation."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    expected_str = f"DataLake(db_url={db_url}, schema=None, table={db_table}, columns=2)"
    assert str(data_lake) == expected_str


def test_data_lake_repr_representation(temp_sqlite_data_lake):
    """Test DataLake repr representation."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    repr_str = repr(data_lake)
    assert "DataLake" in repr_str
    assert db_url in repr_str
    assert db_table in repr_str
    assert "columns=" in repr_str


def test_data_lake_equality(temp_sqlite_data_lake):
    """Test DataLake equality comparison."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    data_lake1 = DataLake(db_source=db_source)
    data_lake2 = DataLake(db_source=db_source)

    # They should be equal since they have the same db_source
    assert data_lake1 == data_lake2

    # Create a different db_source using the different table in the same database
    different_db_source = DbSource(db_url=db_url, db_schema=None, db_table="different_table")
    data_lake3 = DataLake(db_source=different_db_source)

    # They should not be equal since they have different db_sources
    assert data_lake1 != data_lake3


def test_data_lake_equality_different_type(temp_sqlite_data_lake):
    """Test DataLake equality with different type."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    other = "not a data lake"
    assert data_lake != other


def test_data_lake_properties(temp_sqlite_data_lake):
    """Test DataLake properties."""
    data_lake, db_source, db_url, db_schema, db_table = temp_sqlite_data_lake

    # Test db_source property
    assert data_lake.db_source == db_source

    # Test column_names property
    assert data_lake.column_names == ["id", "name"]

    # Test db_url property
    assert data_lake.db_url == db_url

    # Test db_schema property
    assert data_lake.db_schema == db_schema

    # Test db_table property
    assert data_lake.db_table == db_table

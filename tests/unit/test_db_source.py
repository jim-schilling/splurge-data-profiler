"""
Unit tests for DbSource class.

These tests focus on testing the DbSource class in isolation,
validating its initialization, properties, and core functionality.
"""

import os
import pytest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table as SATable, Column as SAColumn, String

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.exceptions import DatabaseError


@pytest.fixture
def db_file(tmp_path: Path):
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"

    # Create table schema
    engine = create_engine(db_url)
    metadata = MetaData()
    SATable(
        "test_table",
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("name", String, nullable=True),
    )
    SATable(
        "different_table",
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("description", String, nullable=True),
    )
    metadata.create_all(engine)
    engine.dispose()

    yield db_url

    # Teardown: remove file if it exists
    try:
        os.remove(db_path)
    except Exception:
        pass


def test_db_source_initialization_connection_error() -> None:
    """Test DbSource initialization with invalid database URL."""
    with pytest.raises(DatabaseError):
        DbSource(db_url="sqlite:///nonexistent.db", db_schema=None, db_table="nonexistent_table")


def test_db_source_properties(db_file: str) -> None:
    """Test DbSource properties."""
    db_url = db_file
    source = DbSource(db_url=db_url, db_schema=None, db_table="test_table")

    assert source.db_url == db_url
    assert source.db_schema is None
    assert source.db_table == "test_table"
    assert len(source.columns) == 2
    assert source.columns[0].name == "id"
    assert source.columns[1].name == "name"


def test_db_source_string_representation(tmp_path: Path) -> None:
    """Test DbSource string representation."""
    db_path = tmp_path / "repr_test.db"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url)
    metadata = MetaData()
    SATable(
        "test_table",
        metadata,
        SAColumn("id", String, primary_key=True),
    )
    metadata.create_all(engine)
    engine.dispose()

    source = DbSource(db_url=db_url, db_schema=None, db_table="test_table")

    expected_str = f"DbSource(db_url={db_url}, schema=None, table=test_table, columns=1)"
    assert str(source) == expected_str


def test_db_source_equality(db_file: str) -> None:
    db_url = db_file
    source1 = DbSource(db_url=db_url, db_schema=None, db_table="test_table")
    source2 = DbSource(db_url=db_url, db_schema=None, db_table="test_table")

    source3 = DbSource(db_url=db_url, db_schema=None, db_table="different_table")

    assert source1 == source2
    assert source1 != source3


def test_db_source_equality_different_type(db_file: str) -> None:
    source = DbSource(db_url=db_file, db_schema=None, db_table="test_table")
    other = "not a db source"

    assert source != other


def test_db_source_repr_representation(db_file: str) -> None:
    source = DbSource(db_url=db_file, db_schema=None, db_table="test_table")

    repr_str = repr(source)
    assert "DbSource" in repr_str
    assert "test_table" in repr_str
    assert "columns=" in repr_str

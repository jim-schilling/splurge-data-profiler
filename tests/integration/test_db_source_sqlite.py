"""
Integration tests for DbSource with real SQLite database.

These tests focus on testing DbSource with real database connections and file systems,
validating end-to-end functionality without mocking.
"""

import pytest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Column as SAColumn, String, Table

from splurge_data_profiler.source import DbSource


@pytest.fixture
def temp_sqlite_db(tmp_path: Path):
    """Create a temporary SQLite database for testing using pytest tmp_path."""
    db_path = tmp_path / "test.db"
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
    metadata.create_all(engine)

    yield db_url, db_table, db_schema

    # Cleanup
    try:
        engine.dispose()
    except Exception:
        pass


def test_dbsource_sqlite_columns(temp_sqlite_db):
    """Test DbSource with real SQLite database."""
    db_url, db_table, db_schema = temp_sqlite_db
    source = DbSource(db_url=db_url, db_schema=db_schema, db_table=db_table)
    assert len(source.columns) == 2
    assert source.columns[0].name == "id"
    assert source.columns[1].name == "name"
    assert all(col.raw_type.name == "TEXT" for col in source.columns)

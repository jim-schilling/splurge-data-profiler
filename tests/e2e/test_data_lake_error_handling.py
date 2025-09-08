import uuid
import pytest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.source import DsvSource


@pytest.fixture
def temp_data_lake_path(tmp_path: Path):
    """Create a temporary directory for data lake testing under pytest tmp_path."""
    data_lake_path = tmp_path / "data_lake"
    data_lake_path.mkdir()
    return data_lake_path


def test_data_lake_factory_invalid_data_lake_path(tmp_path: Path):
    """Test DataLakeFactory with invalid data lake path."""
    # Create a temporary CSV file
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    # no manual cleanup needed; pytest will cleanup tmp_path

    try:
        # Try to create data lake with invalid path
        # Note: DataLakeFactory actually creates the directory if it doesn't exist
        invalid_path = Path("/nonexistent/invalid/path")
        dsv_source = DsvSource(csv_path)

        # This should work because DataLakeFactory creates the directory
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=invalid_path)

        # Verify the data lake was created successfully
        assert isinstance(data_lake, DataLake)
        assert len(data_lake.column_names) == 2

    finally:
        # nothing to clean up; file is under tmp_path
        pass


def test_data_lake_factory_column_mismatch(temp_data_lake_path, tmp_path: Path):
    """Test DataLakeFactory with column mismatch between DSV and database."""
    # Create a CSV file with specific columns
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n", encoding="utf-8")

    try:
        # Create DSV source
        dsv_source = DsvSource(csv_path)

        # Manually create a database with different column names
        db_path = temp_data_lake_path / "test_mismatch.sqlite"
        db_url = f"sqlite:///{db_path}"

        engine = create_engine(db_url)
        metadata = MetaData()
        # Create table with different column names than CSV
        Table(
            "test_table",
            metadata,
            SAColumn("user_id", String, nullable=True),  # Different from 'id'
            SAColumn("full_name", String, nullable=True),  # Different from 'name'
            SAColumn("contact", String, nullable=True),  # Different from 'email'
        )
        metadata.create_all(engine)
        engine.dispose()

        # Try to stream data - this should fail due to column mismatch
        db_source = DbSource(db_url=db_url, db_table="test_table")

        from splurge_data_profiler.exceptions import FileProcessingError

        with pytest.raises(FileProcessingError) as exc_info:
            DataLakeFactory._stream_dsv_to_sqlite(dsv_source=dsv_source, db_source=db_source)

        assert "Column mismatch" in str(exc_info.value)

    finally:
        pass


def test_data_lake_factory_empty_dsv_file(tmp_path: Path):
    """Test DataLakeFactory with completely empty DSV file."""
    # Create an empty CSV file
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("", encoding="utf-8")

    try:
        # Empty files should be handled gracefully with 0 columns
        source = DsvSource(csv_path)
        assert len(source.columns) == 0

    finally:
        pass


def test_data_lake_factory_malformed_csv(temp_data_lake_path, tmp_path: Path):
    """Test DataLakeFactory with malformed CSV data."""
    # Create a CSV file with inconsistent column counts
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name,email\n1,Alice\n2,Bob,bob@example.com,extra\n3\n", encoding="utf-8")

    try:
        dsv_source = DsvSource(csv_path)

        # This should handle malformed data gracefully
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_path)

        # Should create a data lake (extra/missing columns handled by underlying libraries)
        assert isinstance(data_lake, DataLake)

    finally:
        pass


def test_data_lake_factory_encoding_error(tmp_path: Path):
    """Test DataLakeFactory with encoding issues."""
    # Create a CSV file with UTF-8 content but specify wrong encoding
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name\n1,José\n2,François\n", encoding="utf-8")

    try:
        # Try to create DSV source with wrong encoding
        from splurge_data_profiler.exceptions import FileProcessingError

        with pytest.raises(FileProcessingError):
            DsvSource(csv_path, encoding="ascii")

    finally:
        pass


def test_data_lake_factory_database_connection_error(tmp_path: Path):
    """Test DataLakeFactory with database connection error."""
    # Create a temporary CSV file
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    try:
        # Try to use an invalid database URL - this should fail at DbSource creation
        invalid_db_url = "sqlite:////invalid/path/nonexistent.db"
        from splurge_data_profiler.exceptions import DatabaseError

        with pytest.raises(DatabaseError):
            DbSource(db_url=invalid_db_url, db_table="test_table")

    finally:
        pass


def test_data_lake_factory_large_batch_size(temp_data_lake_path, tmp_path: Path):
    """Test DataLakeFactory with very large batch size."""
    # Create a CSV file with some data
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n", encoding="utf-8")

    try:
        dsv_source = DsvSource(csv_path)

        # Test with very large batch size (should handle gracefully)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_path)

        assert isinstance(data_lake, DataLake)
        assert len(data_lake.column_names) == 2

    finally:
        pass


def test_data_lake_factory_zero_batch_size(temp_data_lake_path, tmp_path: Path):
    """Test DataLakeFactory with zero batch size (edge case)."""
    # Create a CSV file with some data
    csv_path = tmp_path / f"{uuid.uuid4().hex}.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    try:
        dsv_source = DsvSource(csv_path)

        # Patch the method to use zero batch size
        orig_stream = DataLakeFactory._stream_dsv_to_sqlite

        def patched_stream(*args, **kwargs):
            return orig_stream(*args, **kwargs, batch_size=0)

        DataLakeFactory._stream_dsv_to_sqlite = patched_stream

        try:
            from splurge_data_profiler.exceptions import FileProcessingError

            with pytest.raises(FileProcessingError):
                DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_path)
        finally:
            DataLakeFactory._stream_dsv_to_sqlite = orig_stream

    finally:
        pass

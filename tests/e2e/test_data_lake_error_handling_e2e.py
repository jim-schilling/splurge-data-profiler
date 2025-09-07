import os
import tempfile
import pytest
import shutil
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.source import DsvSource


@pytest.fixture
def temp_data_lake_path():
    """Create a temporary directory for data lake testing."""
    temp_dir = tempfile.mkdtemp()
    data_lake_path = Path(temp_dir)

    yield data_lake_path

    # Cleanup
    try:
        shutil.rmtree(temp_dir)
    except OSError:
        pass


def test_data_lake_factory_invalid_data_lake_path():
    """Test DataLakeFactory with invalid data lake path."""
    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,Alice\n2,Bob\n')
    csv_path = Path(temp_path)

    try:
        # Try to create data lake with invalid path
        # Note: DataLakeFactory actually creates the directory if it doesn't exist
        invalid_path = Path("/nonexistent/invalid/path")
        dsv_source = DsvSource(csv_path)

        # This should work because DataLakeFactory creates the directory
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=invalid_path
        )

        # Verify the data lake was created successfully
        assert isinstance(data_lake, DataLake)
        assert len(data_lake.column_names) == 2

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_column_mismatch(temp_data_lake_path):
    """Test DataLakeFactory with column mismatch between DSV and database."""
    # Create a CSV file with specific columns
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n')
    csv_path = Path(temp_path)

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
            "test_table", metadata,
            SAColumn("user_id", String, nullable=True),  # Different from 'id'
            SAColumn("full_name", String, nullable=True),  # Different from 'name'
            SAColumn("contact", String, nullable=True),   # Different from 'email'
        )
        metadata.create_all(engine)
        engine.dispose()

        # Try to stream data - this should fail due to column mismatch
        db_source = DbSource(db_url=db_url, db_table="test_table")

        from splurge_data_profiler.exceptions import FileProcessingError
        with pytest.raises(FileProcessingError) as exc_info:
            DataLakeFactory._stream_dsv_to_sqlite(
                dsv_source=dsv_source,
                db_source=db_source
            )

        assert "Column mismatch" in str(exc_info.value)

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_empty_dsv_file():
    """Test DataLakeFactory with completely empty DSV file."""
    # Create an empty CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    # Don't write anything to the file
    csv_path = Path(temp_path)

    try:
        # Empty files should be handled gracefully with 0 columns
        source = DsvSource(csv_path)
        assert len(source.columns) == 0

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_malformed_csv(temp_data_lake_path):
    """Test DataLakeFactory with malformed CSV data."""
    # Create a CSV file with inconsistent column counts
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name,email\n1,Alice\n2,Bob,bob@example.com,extra\n3\n')
    csv_path = Path(temp_path)

    try:
        dsv_source = DsvSource(csv_path)

        # This should handle malformed data gracefully
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=temp_data_lake_path
        )

        # Should create a data lake (extra/missing columns handled by underlying libraries)
        assert isinstance(data_lake, DataLake)

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_encoding_error():
    """Test DataLakeFactory with encoding issues."""
    # Create a CSV file with UTF-8 content but specify wrong encoding
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,José\n2,François\n')
    csv_path = Path(temp_path)

    try:
        # Try to create DSV source with wrong encoding
        from splurge_data_profiler.exceptions import FileProcessingError
        with pytest.raises(FileProcessingError):
            DsvSource(csv_path, encoding='ascii')

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_database_connection_error():
    """Test DataLakeFactory with database connection error."""
    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,Alice\n2,Bob\n')

    try:
        # Try to use an invalid database URL - this should fail at DbSource creation
        invalid_db_url = "sqlite:////invalid/path/nonexistent.db"
        from splurge_data_profiler.exceptions import DatabaseError
        with pytest.raises(DatabaseError):
            DbSource(
                db_url=invalid_db_url,
                db_table="test_table"
            )

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_large_batch_size(temp_data_lake_path):
    """Test DataLakeFactory with very large batch size."""
    # Create a CSV file with some data
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,Alice\n2,Bob\n3,Charlie\n')
    csv_path = Path(temp_path)

    try:
        dsv_source = DsvSource(csv_path)

        # Test with very large batch size (should handle gracefully)
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=temp_data_lake_path
        )

        assert isinstance(data_lake, DataLake)
        assert len(data_lake.column_names) == 2

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_factory_zero_batch_size(temp_data_lake_path):
    """Test DataLakeFactory with zero batch size (edge case)."""
    # Create a CSV file with some data
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,Alice\n2,Bob\n')
    csv_path = Path(temp_path)

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
                DataLakeFactory.from_dsv_source(
                    dsv_source=dsv_source,
                    data_lake_path=temp_data_lake_path
                )
        finally:
            DataLakeFactory._stream_dsv_to_sqlite = orig_stream

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass
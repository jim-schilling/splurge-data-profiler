import os
import tempfile
import pytest
from pathlib import Path

from sqlalchemy import create_engine, text

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.source import DsvSource


@pytest.fixture
def temp_data_lake_dir():
    """Create a temporary directory for data lake testing."""
    temp_dir = tempfile.mkdtemp()
    data_lake_path = Path(temp_dir)

    yield data_lake_path

    # Cleanup
    try:
        import shutil

        shutil.rmtree(temp_dir)
    except OSError:
        pass


def test_data_lake_engine_disposal(temp_data_lake_dir):
    """Test that database engines are properly disposed."""
    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n1,Alice\n2,Bob\n")
    csv_path = Path(temp_path)

    try:
        dsv_source = DsvSource(csv_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_dir)

        # Check that we can access the database after creation
        engine = create_engine(data_lake.db_url)
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
            count = result.fetchone()[0]
            assert count == 2
        engine.dispose()

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_multiple_connections(temp_data_lake_dir):
    """Test DataLake with multiple simultaneous connections."""
    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
    csv_path = Path(temp_path)

    try:
        dsv_source = DsvSource(csv_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_dir)

        # Test multiple connections to the same database
        engines = []
        try:
            for i in range(3):
                engine = create_engine(data_lake.db_url)
                engines.append(engine)

                with engine.connect() as connection:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
                    count = result.fetchone()[0]
                    assert count == 3
        finally:
            # Ensure all engines are disposed
            for engine in engines:
                try:
                    engine.dispose()
                except Exception:
                    pass

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass


def test_data_lake_concurrent_access(temp_data_lake_dir):
    """Test DataLake concurrent access patterns."""
    import threading

    # Create a temporary CSV file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
        f.write("id,name\n")
        for i in range(100):
            f.write(f"{i},Name{i}\n")
    csv_path = Path(temp_path)

    try:
        dsv_source = DsvSource(csv_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=temp_data_lake_dir)

        results = []
        errors = []

        def worker(worker_id: int) -> None:
            """Worker function for concurrent access."""
            try:
                engine = create_engine(data_lake.db_url)
                with engine.connect() as connection:
                    # Perform a query
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
                    count = result.fetchone()[0]
                    results.append((worker_id, count))
                engine.dispose()
            except Exception as e:
                errors.append((worker_id, str(e)))

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify results
        assert len(results) == 5
        assert len(errors) == 0
        for worker_id, count in results:
            assert count == 100

    finally:
        try:
            os.remove(temp_path)
        except Exception:
            pass

from pathlib import Path
import pytest

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text

from splurge_data_profiler.source import DbSource, DsvSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.exceptions import FileProcessingError, DatabaseError


def test_data_lake_initialization(tmp_path: Path) -> None:
    db_path = tmp_path / "test_data_lake.db"
    db_url = f"sqlite:///{db_path}"
    db_table = "test_table"
    db_schema = None

    engine = create_engine(db_url)
    metadata = MetaData()
    Table(
        db_table,
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("name", String, nullable=True),
    )
    Table(
        "different_table",
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("description", String, nullable=True),
    )
    metadata.create_all(engine)
    engine.dispose()

    db_source = DbSource(db_url=db_url, db_schema=db_schema, db_table=db_table)
    data_lake = DataLake(db_source=db_source)

    assert isinstance(data_lake, DataLake)
    assert data_lake.db_source == db_source
    assert data_lake.db_url == db_url
    assert data_lake.db_schema == db_schema
    assert data_lake.db_table == db_table
    assert data_lake.column_names == ["id", "name"]


def test_data_lake_string_representation(tmp_path: Path) -> None:
    db_path = tmp_path / "test_data_lake.db"
    db_url = f"sqlite:///{db_path}"
    db_table = "test_table"

    engine = create_engine(db_url)
    metadata = MetaData()
    Table(
        db_table,
        metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("name", String, nullable=True),
    )
    metadata.create_all(engine)
    engine.dispose()

    db_source = DbSource(db_url=db_url, db_schema=None, db_table=db_table)
    data_lake = DataLake(db_source=db_source)

    expected_str = f"DataLake(db_url={db_url}, schema=None, table={db_table}, columns=2)"
    assert str(data_lake) == expected_str


@pytest.fixture
def data_lake_paths(tmp_path: Path):
    """Helper fixture to provide a sample CSV and a data lake directory under tmp_path."""
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n", encoding="utf-8")

    data_lake_dir = tmp_path / "datalake"
    data_lake_dir.mkdir()
    return csv_path, data_lake_dir

def test_data_lake_from_factory(data_lake_paths: tuple[Path, Path]) -> None:
    csv_path, data_lake_path = data_lake_paths
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=data_lake_path)
    assert isinstance(data_lake, DataLake)
    assert isinstance(data_lake.db_source, DbSource)
    assert data_lake.column_names == ["id", "name", "value"]
    assert data_lake.db_table == csv_path.stem

def test_data_lake_empty_dsv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("id,name\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    assert isinstance(data_lake, DataLake)

def test_data_lake_dsv_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "missing.csv"
    csv_path.write_text("id,name\n1\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    assert isinstance(data_lake, DataLake)

def test_data_lake_dsv_extra_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "extra.csv"
    csv_path.write_text("id,name\n1,Alice,Extra\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    assert isinstance(data_lake, DataLake)

def test_data_lake_batch_size_edge_case(tmp_path: Path) -> None:
    csv_path = tmp_path / "batch.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    orig_stream = DataLakeFactory._stream_dsv_to_sqlite

    def patched_stream(*args, **kwargs):
        return orig_stream(*args, **kwargs, batch_size=100)

    DataLakeFactory._stream_dsv_to_sqlite, orig = patched_stream, DataLakeFactory._stream_dsv_to_sqlite
    try:
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
        assert isinstance(data_lake, DataLake)
    finally:
        DataLakeFactory._stream_dsv_to_sqlite = orig

def test_data_lake_factory_invalid_data_lake_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "invalid_path.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    invalid_path = Path("/nonexistent/invalid/path")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=invalid_path)
    assert isinstance(data_lake, DataLake)
    assert len(data_lake.column_names) == 2

def test_data_lake_factory_column_mismatch(tmp_path: Path) -> None:
    csv_path = tmp_path / "mismatch.csv"
    csv_path.write_text("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n", encoding="utf-8")

    dsv_source = DsvSource(csv_path)
    db_path = tmp_path / "test_mismatch.sqlite"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url)
    metadata = MetaData()
    Table(
        "test_table",
        metadata,
        SAColumn("user_id", String, nullable=True),
        SAColumn("full_name", String, nullable=True),
        SAColumn("contact", String, nullable=True),
    )
    metadata.create_all(engine)
    engine.dispose()

    db_source = DbSource(db_url=db_url, db_table="test_table")

    with pytest.raises(FileProcessingError) as context:
        DataLakeFactory._stream_dsv_to_sqlite(dsv_source=dsv_source, db_source=db_source)

    assert "Column mismatch" in str(context.value)

def test_data_lake_factory_empty_dsv_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty_file.csv"
    csv_path.write_text("", encoding="utf-8")
    source = DsvSource(csv_path)
    assert len(source.columns) == 0

def test_data_lake_factory_malformed_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "malformed.csv"
    csv_path.write_text("id,name,email\n1,Alice\n2,Bob,bob@example.com,extra\n3\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    assert isinstance(data_lake, DataLake)

def test_data_lake_factory_encoding_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "encoding.csv"
    csv_path.write_text("id,name\n1,José\n2,François\n", encoding="utf-8")
    with pytest.raises(FileProcessingError):
        DsvSource(csv_path, encoding="ascii")

def test_data_lake_factory_database_connection_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "db_error.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    invalid_db_url = "sqlite:////invalid/path/nonexistent.db"
    with pytest.raises(DatabaseError):
        DbSource(db_url=invalid_db_url, db_table="test_table")

def test_data_lake_factory_large_batch_size(tmp_path: Path) -> None:
    csv_path = tmp_path / "large_batch.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    assert isinstance(data_lake, DataLake)
    assert len(data_lake.column_names) == 2

def test_data_lake_factory_zero_batch_size(tmp_path: Path) -> None:
    csv_path = tmp_path / "zero_batch.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    orig_stream = DataLakeFactory._stream_dsv_to_sqlite

    def patched_stream(*args, **kwargs):
        return orig_stream(*args, **kwargs, batch_size=0)

    DataLakeFactory._stream_dsv_to_sqlite = patched_stream

    try:
        with pytest.raises(FileProcessingError):
            DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)
    finally:
        DataLakeFactory._stream_dsv_to_sqlite = orig_stream

def test_data_lake_engine_disposal(tmp_path: Path) -> None:
    csv_path = tmp_path / "engine.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)

    engine = create_engine(data_lake.db_url)
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
        count = result.fetchone()[0]
        assert count == 2
    engine.dispose()

def test_data_lake_multiple_connections(tmp_path: Path) -> None:
    csv_path = tmp_path / "multi.csv"
    csv_path.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n", encoding="utf-8")
    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)

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
        for engine in engines:
            try:
                engine.dispose()
            except Exception:
                pass

def test_data_lake_concurrent_access(tmp_path: Path) -> None:
    import threading

    csv_path = tmp_path / "concurrent.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("id,name\n")
        for i in range(100):
            f.write(f"{i},Name{i}\n")

    dsv_source = DsvSource(csv_path)
    data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=tmp_path)

    results = []
    errors = []

    def worker(worker_id: int) -> None:
        try:
            engine = create_engine(data_lake.db_url)
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
                count = result.fetchone()[0]
                results.append((worker_id, count))
            engine.dispose()
        except Exception as e:
            errors.append((worker_id, str(e)))

    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(results) == 5
    assert len(errors) == 0
    for worker_id, count in results:
        assert count == 100


import os
import tempfile
from pathlib import Path
import pytest

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text

from splurge_data_profiler.source import DbSource, DsvSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.exceptions import FileProcessingError, DatabaseError


class TestDataLake:
    def setup_method(self) -> None:
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_table = "test_table"
        self.db_schema = None

        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        Table(
            self.db_table,
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
        metadata.create_all(self.engine)

        self.db_source = DbSource(db_url=self.db_url, db_schema=self.db_schema, db_table=self.db_table)
        self.data_lake = DataLake(db_source=self.db_source)

    def teardown_method(self) -> None:
        try:
            self.engine.dispose()
        except Exception:
            pass
        os.close(self.db_fd)
        try:
            os.remove(self.db_path)
        except PermissionError:
            pass

    def test_data_lake_initialization(self) -> None:
        assert isinstance(self.data_lake, DataLake)
        assert self.data_lake.db_source == self.db_source
        assert self.data_lake.db_url == self.db_url
        assert self.data_lake.db_schema == self.db_schema
        assert self.data_lake.db_table == self.db_table
        assert self.data_lake.column_names == ["id", "name"]

    def test_data_lake_string_representation(self) -> None:
        expected_str = f"DataLake(db_url={self.db_url}, schema=None, table={self.db_table}, columns=2)"
        assert str(self.data_lake) == expected_str


class TestDataLakeIntegration:
    def setup_method(self) -> None:
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n")
        self.test_file_path = Path(self.temp_path)

        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def teardown_method(self) -> None:
        try:
            os.remove(self.temp_path)
        except Exception:
            pass
        try:
            import shutil

            shutil.rmtree(self.temp_dir)
        except Exception:
            pass

    def test_data_lake_from_factory(self) -> None:
        dsv_source = DsvSource(self.test_file_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        assert isinstance(data_lake.db_source, DbSource)
        assert data_lake.column_names == ["id", "name", "value"]
        assert data_lake.db_table == self.test_file_path.stem

    def test_data_lake_empty_dsv(self):
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n")
        dsv_source = DsvSource(temp_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_missing_columns(self):
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1\n2,Bob\n")
        dsv_source = DsvSource(temp_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_extra_columns(self):
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice,Extra\n2,Bob\n")
        dsv_source = DsvSource(temp_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_batch_size_edge_case(self):
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        dsv_source = DsvSource(temp_path)
        orig_stream = DataLakeFactory._stream_dsv_to_sqlite

        def patched_stream(*args, **kwargs):
            return orig_stream(*args, **kwargs, batch_size=100)

        DataLakeFactory._stream_dsv_to_sqlite, orig = patched_stream, DataLakeFactory._stream_dsv_to_sqlite
        try:
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            assert isinstance(data_lake, DataLake)
        finally:
            DataLakeFactory._stream_dsv_to_sqlite = orig
        os.remove(temp_path)

    def test_data_lake_factory_invalid_data_lake_path(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        csv_path = Path(temp_path)

        try:
            invalid_path = Path("/nonexistent/invalid/path")
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=invalid_path)
            assert isinstance(data_lake, DataLake)
            assert len(data_lake.column_names) == 2
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_column_mismatch(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            db_path = self.data_lake_path / "test_mismatch.sqlite"
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

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_empty_dsv_file(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        csv_path = Path(temp_path)
        try:
            source = DsvSource(csv_path)
            assert len(source.columns) == 0
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_malformed_csv(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,email\n1,Alice\n2,Bob,bob@example.com,extra\n3\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            assert isinstance(data_lake, DataLake)
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_encoding_error(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,José\n2,François\n")
        csv_path = Path(temp_path)

        try:
            with pytest.raises(FileProcessingError):
                DsvSource(csv_path, encoding="ascii")
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_database_connection_error(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")

        try:
            invalid_db_url = "sqlite:////invalid/path/nonexistent.db"
            with pytest.raises(DatabaseError):
                DbSource(db_url=invalid_db_url, db_table="test_table")
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_large_batch_size(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            assert isinstance(data_lake, DataLake)
            assert len(data_lake.column_names) == 2
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_zero_batch_size(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            orig_stream = DataLakeFactory._stream_dsv_to_sqlite

            def patched_stream(*args, **kwargs):
                return orig_stream(*args, **kwargs, batch_size=0)

            DataLakeFactory._stream_dsv_to_sqlite = patched_stream

            try:
                with pytest.raises(FileProcessingError):
                    DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
            finally:
                DataLakeFactory._stream_dsv_to_sqlite = orig_stream

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_engine_disposal(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

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

    def test_data_lake_multiple_connections(self) -> None:
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

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

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_concurrent_access(self) -> None:
        import threading

        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n")
            for i in range(100):
                f.write(f"{i},Name{i}\n")
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)

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

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass


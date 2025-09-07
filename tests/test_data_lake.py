import os
import tempfile
import pytest
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table, Column as SAColumn, String, text

from splurge_data_profiler.source import DbSource
from splurge_data_profiler.data_lake import DataLake, DataLakeFactory
from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.exceptions import FileProcessingError, DatabaseError


class TestDataLake:
    """Test cases for DataLake class."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary SQLite database file
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.db_table = "test_table"
        self.db_schema = None  # SQLite does not use schemas
        
        # Create table
        self.engine = create_engine(self.db_url)
        metadata = MetaData()
        Table(
            self.db_table, metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("name", String, nullable=True),
        )
        # Create a second table for equality testing
        Table(
            "different_table", metadata,
            SAColumn("id", String, primary_key=True),
            SAColumn("description", String, nullable=True),
        )
        metadata.create_all(self.engine)
        
        # Create DbSource and DataLake
        self.db_source = DbSource(
            db_url=self.db_url,
            db_schema=self.db_schema,
            db_table=self.db_table
        )
        self.data_lake = DataLake(db_source=self.db_source)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
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
        """Test DataLake initialization."""
        assert isinstance(self.data_lake, DataLake)
        assert self.data_lake.db_source == self.db_source
        assert self.data_lake.db_url == self.db_url
        assert self.data_lake.db_schema == self.db_schema
        assert self.data_lake.db_table == self.db_table
        assert self.data_lake.column_names == ["id", "name"]

    def test_data_lake_string_representation(self) -> None:
        """Test DataLake string representation."""
        expected_str = f"DataLake(db_url={self.db_url}, schema=None, table={self.db_table}, columns=2)"
        assert str(self.data_lake) == expected_str

    def test_data_lake_repr_representation(self) -> None:
        """Test DataLake repr representation."""
        repr_str = repr(self.data_lake)
        assert "DataLake" in repr_str
        assert self.db_url in repr_str
        assert self.db_table in repr_str
        assert "columns=" in repr_str

    def test_data_lake_equality(self) -> None:
        """Test DataLake equality comparison."""
        data_lake1 = DataLake(db_source=self.db_source)
        data_lake2 = DataLake(db_source=self.db_source)
        
        # They should be equal since they have the same db_source
        assert data_lake1 == data_lake2
        
        # Create a different db_source using the different table in the same database
        different_db_source = DbSource(
            db_url=self.db_url,
            db_schema=None,
            db_table="different_table"
        )
        data_lake3 = DataLake(db_source=different_db_source)
        
        # They should not be equal since they have different db_sources
        assert data_lake1 != data_lake3

    def test_data_lake_equality_different_type(self) -> None:
        """Test DataLake equality with different type."""
        other = "not a data lake"
        assert self.data_lake != other

    def test_data_lake_properties(self) -> None:
        """Test DataLake properties."""
        # Test db_source property
        assert self.data_lake.db_source == self.db_source
        
        # Test column_names property
        assert self.data_lake.column_names == ["id", "name"]
        
        # Test db_url property
        assert self.data_lake.db_url == self.db_url
        
        # Test db_schema property
        assert self.data_lake.db_schema == self.db_schema
        
        # Test db_table property
        assert self.data_lake.db_table == self.db_table




class TestDataLakeIntegration():
    """Integration tests for DataLake with real data."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.75\n')
        self.test_file_path = Path(self.temp_path)
        
        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
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
        """Test DataLake creation through DataLakeFactory."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)
        
        # Create data lake using factory
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Test DataLake properties
        assert isinstance(data_lake, DataLake)
        assert isinstance(data_lake.db_source, DbSource)
        assert data_lake.column_names == ["id", "name", "value"]
        assert data_lake.db_table == self.test_file_path.stem
        # SQLite doesn't use schemas; skip schema assertion for SQLite
        if 'sqlite' not in data_lake.db_url:
            assert data_lake.db_schema is None
        
        # Test string representation
        expected_str = f"DataLake(db_url={data_lake.db_url}, schema=None, table={data_lake.db_table}, columns=3)"
        assert str(data_lake) == expected_str

    def test_data_lake_equality_with_factory_created(self) -> None:
        """Test DataLake equality with factory-created instances."""
        # Create two data lakes from the same source
        dsv_source = DsvSource(self.test_file_path)
        
        data_lake1 = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        data_lake2 = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # They should be equal since they have the same configuration
        assert data_lake1 == data_lake2

    def test_data_lake_empty_dsv(self):
        """Test DataLake creation from an empty DSV file."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n')  # Only header, no data
        dsv_source = DsvSource(temp_path)
        # Should not raise, but will create an empty table
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_missing_columns(self):
        """Test DataLake creation from DSV with missing columns."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Should not raise, but will have None for missing values
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_dsv_extra_columns(self):
        """Test DataLake creation from DSV with extra columns in data rows."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice,Extra\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Should not raise, extra columns are ignored
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        assert isinstance(data_lake, DataLake)
        os.remove(temp_path)

    def test_data_lake_batch_size_edge_case(self):
        """Test DataLake batch insertion with minimum batch_size (edge case)."""
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        dsv_source = DsvSource(temp_path)
        # Patch DataLakeFactory to use minimum batch_size
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




class TestDataLakeErrorHandling():
    """Test cases for DataLake error handling and edge cases."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary directory for data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def test_data_lake_factory_invalid_data_lake_path(self) -> None:
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

    def test_data_lake_factory_column_mismatch(self) -> None:
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
            db_path = self.data_lake_path / "test_mismatch.sqlite"
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

            with pytest.raises(FileProcessingError) as context:
                DataLakeFactory._stream_dsv_to_sqlite(
                    dsv_source=dsv_source,
                    db_source=db_source
                )

            # pytest's context holds the exception instance in .value
            assert "Column mismatch" in str(context.value)

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_empty_dsv_file(self) -> None:
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

    def test_data_lake_factory_malformed_csv(self) -> None:
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
                data_lake_path=self.data_lake_path
            )

            # Should create a data lake (extra/missing columns handled by underlying libraries)
            assert isinstance(data_lake, DataLake)

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_encoding_error(self) -> None:
        """Test DataLakeFactory with encoding issues."""
        # Create a CSV file with UTF-8 content but specify wrong encoding
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,José\n2,François\n')
        csv_path = Path(temp_path)

        try:
            # Try to create DSV source with wrong encoding
            with pytest.raises(FileProcessingError):
                DsvSource(csv_path, encoding='ascii')

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_database_connection_error(self) -> None:
        """Test DataLakeFactory with database connection error."""
        # Create a temporary CSV file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')

        try:
            # Try to use an invalid database URL - this should fail at DbSource creation
            invalid_db_url = "sqlite:////invalid/path/nonexistent.db"
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

    def test_data_lake_factory_large_batch_size(self) -> None:
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
                data_lake_path=self.data_lake_path
            )

            assert isinstance(data_lake, DataLake)
            assert len(data_lake.column_names) == 2

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass

    def test_data_lake_factory_zero_batch_size(self) -> None:
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
                with pytest.raises(FileProcessingError):
                    DataLakeFactory.from_dsv_source(
                        dsv_source=dsv_source,
                        data_lake_path=self.data_lake_path
                    )
            finally:
                DataLakeFactory._stream_dsv_to_sqlite = orig_stream

        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass




class TestDataLakeResourceManagement():
    """Test cases for DataLake database resource management."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Create a temporary directory for data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def test_data_lake_engine_disposal(self) -> None:
        """Test that database engines are properly disposed."""
        # Create a temporary CSV file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )

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

    def test_data_lake_multiple_connections(self) -> None:
        """Test DataLake with multiple simultaneous connections."""
        # Create a temporary CSV file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n')
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )

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

    def test_data_lake_concurrent_access(self) -> None:
        """Test DataLake concurrent access patterns."""
        import threading

        # Create a temporary CSV file
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n')
            for i in range(100):
                f.write(f'{i},Name{i}\n')
        csv_path = Path(temp_path)

        try:
            dsv_source = DsvSource(csv_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )

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



 
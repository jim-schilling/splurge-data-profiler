import os
import pytest
import re
from pathlib import Path
from datetime import datetime, date, time, timedelta
import csv
from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, String

from splurge_data_profiler.source import DataType, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.exceptions import DatabaseError


@pytest.fixture(autouse=True)
def _setup_profiler_instance(request, tmp_path: Path):
    """Prepare TestProfilerComprehensive instances to use pytest's tmp_path.

    This fixture only acts when the running test is a method on
    `TestProfilerComprehensive`. It creates a CSV file and a data lake
    directory under `tmp_path`, sets instance attributes used by the
    class's methods, and calls the existing `_generate_comprehensive_csv`
    helper which writes to the provided file descriptor.
    """
    inst = getattr(request, "instance", None)
    # Only apply for the target test class
    if inst is None or inst.__class__.__name__ != "TestProfilerComprehensive":
        yield
        return

    import os

    # Create data lake dir under tmp_path
    temp_dir = tmp_path / "lake"
    temp_dir.mkdir(exist_ok=True)
    inst.temp_dir = str(temp_dir)
    inst.data_lake_path = Path(inst.temp_dir)

    # Create CSV file path and open a real fd so existing generation code (os.fdopen)
    # continues to work unchanged.
    csv_path = tmp_path / "comprehensive.csv"
    fd = os.open(str(csv_path), os.O_WRONLY | os.O_CREAT)
    inst.temp_fd = fd
    inst.temp_path = str(csv_path)
    inst.csv_path = Path(inst.temp_path)

    # Call the class helper to populate the CSV
    inst._generate_comprehensive_csv()

    # Build DsvSource, DataLake, and Profiler as the original setup did
    inst.dsv_source = DsvSource(inst.csv_path, delimiter="|", bookend='"')
    inst.data_lake = DataLakeFactory.from_dsv_source(dsv_source=inst.dsv_source, data_lake_path=inst.data_lake_path)
    inst.profiler = Profiler(data_lake=inst.data_lake)

    yield

    # Teardown: ensure fd closed; tmp_path cleanup handled by pytest
    try:
        os.close(fd)
    except Exception:
        pass


class TestProfilerComprehensive:
    """Test comprehensive profiling functionality."""
    # setup is handled by module autouse fixture `_setup_profiler_instance`

    def _generate_comprehensive_csv(self) -> None:
        """Generate a comprehensive CSV file with all data types and 1000 rows."""
        column_configs = [
            # TEXT columns
            ("text_simple", "TEXT", self._generate_text_values),
            ("text_names", "TEXT", self._generate_name_values),
            ("text_emails", "TEXT", self._generate_email_values),
            ("text_addresses", "TEXT", self._generate_address_values),
            # INTEGER columns
            ("integer_small", "INTEGER", self._generate_small_integer_values),
            ("integer_large", "INTEGER", self._generate_large_integer_values),
            ("integer_negative", "INTEGER", self._generate_negative_integer_values),
            ("integer_mixed", "INTEGER", self._generate_mixed_integer_values),
            # FLOAT columns
            ("float_simple", "FLOAT", self._generate_simple_float_values),
            ("float_precise", "FLOAT", self._generate_precise_float_values),
            ("float_scientific", "FLOAT", self._generate_scientific_float_values),
            ("float_currency", "FLOAT", self._generate_currency_float_values),
            # BOOLEAN columns
            ("boolean_simple", "BOOLEAN", self._generate_boolean_values),
            ("boolean_text", "BOOLEAN", self._generate_boolean_text_values),
            ("boolean_mixed", "BOOLEAN", self._generate_mixed_boolean_values),
            # DATE columns
            ("date_simple", "DATE", self._generate_date_values),
            ("date_formatted", "DATE", self._generate_formatted_date_values),
            ("date_mixed", "DATE", self._generate_mixed_date_values),
            # TIME columns
            ("time_simple", "TIME", self._generate_time_values),
            ("time_formatted", "TIME", self._generate_formatted_time_values),
            ("time_mixed", "TIME", self._generate_mixed_time_values),
            # DATETIME columns
            ("datetime_simple", "DATETIME", self._generate_datetime_values),
            ("datetime_formatted", "DATETIME", self._generate_formatted_datetime_values),
            ("datetime_mixed", "DATETIME", self._generate_mixed_datetime_values),
        ]
        header = [config[0] for config in column_configs]
        # Use pipe as delimiter and double quote as bookend
        delimiter = "|"
        bookend = '"'
        with os.fdopen(self.temp_fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for i in range(1000):
                row_data = [str(generator_func(i)) for _, _, generator_func in column_configs]
                writer.writerow(row_data)

    def _generate_text_values(self, index: int) -> str:
        """Generate text values."""
        texts = [
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor incididunt",
            "ut labore et dolore magna aliqua",
            "Ut enim ad minim veniam",
            "quis nostrud exercitation ullamco",
            "laboris nisi ut aliquip ex ea commodo consequat",
            "Duis aute irure dolor in reprehenderit",
            "in voluptate velit esse cillum dolore",
            "eu fugiat nulla pariatur",
        ]
        return texts[index % len(texts)]

    def _generate_name_values(self, index: int) -> str:
        """Generate name values."""
        first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
        return f"{first_names[index % len(first_names)]} {last_names[index % len(last_names)]}"

    def _generate_email_values(self, index: int) -> str:
        """Generate email values."""
        domains = ["example.com", "test.org", "sample.net", "demo.co.uk"]
        return f"user{index}@{domains[index % len(domains)]}"

    def _generate_address_values(self, index: int) -> str:
        """Generate address values."""
        streets = ["123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm Blvd"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston"]
        return f"{streets[index % len(streets)]}, {cities[index % len(cities)]}"

    def _generate_small_integer_values(self, index: int) -> int:
        """Generate small integer values."""
        return index % 100

    def _generate_large_integer_values(self, index: int) -> int:
        """Generate large integer values."""
        return 1000000 + index

    def _generate_negative_integer_values(self, index: int) -> int:
        """Generate negative integer values."""
        return -1000 - index

    def _generate_mixed_integer_values(self, index: int) -> int:
        """Generate mixed integer values (including some text)."""
        if index % 20 == 0:  # 5% of values are text
            return f"text_{index}"
        return index * 10

    def _generate_simple_float_values(self, index: int) -> float:
        """Generate simple float values."""
        return index * 1.5

    def _generate_precise_float_values(self, index: int) -> float:
        """Generate precise float values."""
        return round(index * 3.14159, 5)

    def _generate_scientific_float_values(self, index: int) -> float:
        """Generate scientific notation float values."""
        return index * 1e6

    def _generate_currency_float_values(self, index: int) -> float:
        """Generate currency-like float values."""
        return round(index * 10.99, 2)

    def _generate_boolean_values(self, index: int) -> bool:
        """Generate boolean values."""
        return bool(index % 2)

    def _generate_boolean_text_values(self, index: int) -> str:
        """Generate boolean text values."""
        return "true" if index % 2 else "false"

    def _generate_mixed_boolean_values(self, index: int) -> str:
        """Generate mixed boolean values."""
        values = ["true", "false", "yes", "no", "1", "0", "Y", "N"]
        return values[index % len(values)]

    def _generate_date_values(self, index: int) -> date:
        """Generate date values."""
        start_date = date(2020, 1, 1)
        return start_date + timedelta(days=index)

    def _generate_formatted_date_values(self, index: int) -> str:
        """Generate formatted date values."""
        start_date = date(2020, 1, 1)
        date_obj = start_date + timedelta(days=index)
        return date_obj.strftime("%m/%d/%Y")

    def _generate_mixed_date_values(self, index: int) -> str:
        """Generate mixed date values."""
        if index % 10 == 0:  # 10% are invalid dates
            return f"invalid_date_{index}"
        start_date = date(2020, 1, 1)
        date_obj = start_date + timedelta(days=index)
        return date_obj.strftime("%Y-%m-%d")

    def _generate_time_values(self, index: int) -> time:
        """Generate time values."""
        return time(hour=index % 24, minute=index % 60, second=index % 60)

    def _generate_formatted_time_values(self, index: int) -> str:
        """Generate formatted time values."""
        time_obj = time(hour=index % 24, minute=index % 60, second=index % 60)
        return time_obj.strftime("%H:%M:%S")

    def _generate_mixed_time_values(self, index: int) -> str:
        """Generate mixed time values."""
        if index % 15 == 0:  # ~6.7% are invalid times
            return f"invalid_time_{index}"
        time_obj = time(hour=index % 24, minute=index % 60, second=index % 60)
        return time_obj.strftime("%I:%M %p")

    def _generate_datetime_values(self, index: int) -> str:
        """Generate datetime values in ISO 8601 format."""
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")

    def _generate_formatted_datetime_values(self, index: int) -> str:
        """Generate formatted datetime values in ISO 8601 format."""
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")

    def _generate_mixed_datetime_values(self, index: int) -> str:
        """Generate mixed datetime values."""
        if index % 25 == 0:  # 4% are invalid datetimes
            return f"invalid_datetime_{index}"
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%m/%d/%Y %I:%M %p")

    def test_profiler_initialization(self) -> None:
        """Test Profiler initialization."""
        assert self.profiler
        assert len(self.profiler.profiled_columns) == len(self.dsv_source.columns)

        # Check that profiled columns are copies
        for i, column in enumerate(self.profiler.profiled_columns):
            assert column.name == self.dsv_source.columns[i].name
            assert column is not self.dsv_source.columns[i]

    def test_profiler_string_representation(self) -> None:
        """Test Profiler string representation."""
        expected_str = f"Profiler(data_lake={self.data_lake}, profiled_columns={len(self.profiler.profiled_columns)})"
        assert str(self.profiler) == expected_str

    def test_profiler_repr_representation(self) -> None:
        """Test Profiler repr representation."""
        repr_str = repr(self.profiler)
        assert "Profiler" in repr_str
        assert "data_lake=" in repr_str
        assert "profiled_columns=" in repr_str

    def test_profiler_equality(self) -> None:
        """Test Profiler equality comparison."""
        profiler1 = Profiler(data_lake=self.data_lake)
        profiler2 = Profiler(data_lake=self.data_lake)

        # They should be equal since they have the same data lake
        assert profiler1 == profiler2

        # Create a different data lake by modifying the profiled columns
        profiler1.profile(sample_size=100)  # This modifies the profiled columns
        profiler3 = Profiler(data_lake=self.data_lake)  # Fresh profiler with same data lake

        # They should not be equal since profiler1 has profiled columns and profiler3 doesn't
        assert profiler1 != profiler3

    def test_profiler_equality_different_type(self) -> None:
        """Test Profiler equality with different type."""
        other = "not a profiler"
        assert self.profiler != other

    def test_profiler_comprehensive_profiling(self) -> None:
        """Test comprehensive profiling with all data types."""
        # Run profiling (reduced sample size for performance)
        self.profiler.profile(sample_size=500)

        # Get profiled columns
        profiled_columns = self.profiler.profiled_columns

        # Verify that all columns were profiled
        assert len(profiled_columns) == 24  # 24 columns total

        # Check specific data type inferences
        expected_types = {
            # TEXT columns
            "text_simple": DataType.TEXT,
            "text_names": DataType.TEXT,
            "text_emails": DataType.TEXT,
            "text_addresses": DataType.TEXT,
            # INTEGER columns
            "integer_small": DataType.INTEGER,
            "integer_large": DataType.INTEGER,
            "integer_negative": DataType.INTEGER,
            "integer_mixed": DataType.TEXT,  # Mixed with text
            # FLOAT columns
            "float_simple": DataType.FLOAT,
            "float_precise": DataType.FLOAT,
            "float_scientific": DataType.FLOAT,
            "float_currency": DataType.FLOAT,
            # BOOLEAN columns
            "boolean_simple": DataType.BOOLEAN,
            "boolean_text": DataType.BOOLEAN,
            "boolean_mixed": DataType.TEXT,  # Mixed formats
            # DATE columns
            "date_simple": DataType.DATE,
            "date_formatted": DataType.DATE,
            "date_mixed": DataType.TEXT,  # Mixed with invalid dates
            # TIME columns
            "time_simple": DataType.TIME,
            "time_formatted": DataType.TIME,
            "time_mixed": DataType.TEXT,  # Mixed with invalid times
            # DATETIME columns
            "datetime_simple": DataType.DATETIME,
            "datetime_formatted": DataType.DATETIME,
            "datetime_mixed": DataType.TEXT,  # Mixed with invalid datetimes
        }

        # Verify each column's inferred type
        for column in profiled_columns:
            if column.name in expected_types:
                assert column.inferred_type == expected_types[column.name], (
                    f"Column {column.name} should be {expected_types[column.name]} but got {column.inferred_type}"
                )

    def test_profiler_sample_size_effectiveness(self) -> None:
        """Test that different sample sizes produce consistent results."""
        # Profile with different sample sizes (reduced for performance)
        self.profiler.profile(sample_size=100)
        results_100 = [col.inferred_type for col in self.profiler.profiled_columns]

        self.profiler.profile(sample_size=500)
        results_500 = [col.inferred_type for col in self.profiler.profiled_columns]

        self.profiler.profile(sample_size=1000)
        results_1000 = [col.inferred_type for col in self.profiler.profiled_columns]

        # Results should be consistent across sample sizes for well-defined data types
        # (Allow some variation for mixed columns)
        for i, (col_100, col_500, col_1000) in enumerate(zip(results_100, results_500, results_1000)):
            column_name = self.profiler.profiled_columns[i].name
            if "mixed" not in column_name:  # Skip mixed columns
                assert col_100 == col_500, f"Sample size 100 vs 500 inconsistent for {column_name}"
                assert col_500 == col_1000, f"Sample size 500 vs 1000 inconsistent for {column_name}"

    def test_profiler_original_data_unmodified(self) -> None:
        """Test that original DataLake and DbSource remain unmodified."""
        # Store original inferred types
        original_types = [col.inferred_type for col in self.data_lake.db_source.columns]

        # Run profiling (reduced sample size for performance)
        self.profiler.profile(sample_size=500)

        # Check that original types are unchanged
        current_types = [col.inferred_type for col in self.data_lake.db_source.columns]
        assert original_types == current_types

        # Check that profiled columns have updated types
        profiled_types = [col.inferred_type for col in self.profiler.profiled_columns]
        assert original_types != profiled_types

    def test_profiler_large_dataset_performance(self) -> None:
        """Test profiling performance with large dataset."""
        import time

        # Time the profiling operation (reduced sample size for performance)
        start_time = time.time()
        self.profiler.profile(sample_size=1000)
        end_time = time.time()

        profiling_time = end_time - start_time

        # Profiling should complete within reasonable time (reduced threshold)
        assert profiling_time < 30.0, f"Profiling took {profiling_time:.2f} seconds, should be under 30 seconds"

        # Verify results were obtained
        profiled_columns = self.profiler.profiled_columns
        assert any(col.inferred_type != DataType.TEXT for col in profiled_columns)

    def test_profiler_error_handling(self) -> None:
        """Test profiler error handling with invalid database connection."""
        # Create profiler with invalid data lake
        invalid_data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=self.dsv_source, data_lake_path=self.data_lake_path
        )

        # Manually corrupt the database URL
        invalid_data_lake._db_url = "sqlite:///nonexistent.db"

        invalid_profiler = Profiler(data_lake=invalid_data_lake)

        # Should raise DatabaseError when trying to profile
        with pytest.raises(DatabaseError):
            invalid_profiler.profile(sample_size=1000)

    def test_profiler_create_inferred_table(self) -> None:
        """Test creating inferred table with cast columns."""
        # First profile the data (reduced sample size for performance)
        self.profiler.profile(sample_size=500)

        # Add a short delay and force engine disposal to avoid SQLite locking
        import time
        from sqlalchemy import create_engine

        engine = create_engine(self.data_lake.db_url)
        engine.dispose()
        time.sleep(0.2)

        # Create the inferred table
        new_table_name = self.profiler.create_inferred_table()

        # Verify the table was created
        assert new_table_name
        assert new_table_name == f"{self.data_lake.db_table}_inferred"

        # Connect to database and verify table structure
        engine = create_engine(self.data_lake.db_url)

        try:
            with engine.connect() as connection:
                # Get table information
                inspector = inspect(engine)
                columns_info = inspector.get_columns(new_table_name)

                # Verify we have the expected number of columns
                # Original columns + cast columns = 24 * 2 = 48 columns
                assert len(columns_info) == 48

                # Verify column structure
                column_names = [col["name"] for col in columns_info]

                # Check that we have both original and cast columns
                for column in self.profiler.profiled_columns:
                    # Original column should exist
                    assert column.name in column_names, f"Original column {column.name} not found"

                    # Cast column should exist
                    cast_col_name = f"{column.name}_cast"
                    assert cast_col_name in column_names, f"Cast column {cast_col_name} not found"

                # Verify data was populated
                result = connection.execute(text(f"SELECT COUNT(*) FROM {new_table_name}"))
                row_count = result.fetchone()[0]
                assert row_count == 1000, "Table should have 1000 rows"

                # Test specific casting examples
                self._verify_casting_examples(connection, new_table_name)

        finally:
            engine.dispose()

    def _verify_casting_examples(self, connection, table_name: str) -> None:
        """Verify specific casting examples work correctly."""

        # Test integer casting
        result = connection.execute(
            text(f"SELECT integer_small, integer_small_cast FROM {table_name} WHERE integer_small IS NOT NULL LIMIT 5")
        )
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                assert isinstance(cast_value, int)
                assert int(original) == cast_value

        # Test float casting
        result = connection.execute(
            text(f"SELECT float_simple, float_simple_cast FROM {table_name} WHERE float_simple IS NOT NULL LIMIT 5")
        )
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                assert isinstance(cast_value, float)
                assert abs(float(original) - cast_value) < 0.000001

        # Test boolean casting
        result = connection.execute(
            text(
                f"SELECT boolean_simple, boolean_simple_cast FROM {table_name} WHERE boolean_simple IS NOT NULL LIMIT 5"
            )
        )
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                # SQLite stores booleans as 0 or 1, so check for integer values
                if isinstance(cast_value, int):
                    # Convert 0/1 to True/False for testing
                    cast_value = bool(cast_value)
                assert isinstance(cast_value, bool)
                # Boolean casting should work correctly
                expected_bool = original.lower() in ["true", "1", "yes", "y"]
                assert expected_bool == cast_value

        # Test date casting
        result = connection.execute(
            text(f"SELECT date_simple, date_simple_cast FROM {table_name} WHERE date_simple IS NOT NULL LIMIT 5")
        )
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                # SQLite returns dates as strings, but we can verify format
                assert isinstance(cast_value, str)
                # Should be in YYYY-MM-DD format
                assert re.search(r"^\d{4}-\d{2}-\d{2}$", cast_value) is not None

        # Test text columns (should remain as text)
        result = connection.execute(
            text(f"SELECT text_simple, text_simple_cast FROM {table_name} WHERE text_simple IS NOT NULL LIMIT 5")
        )
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original:
                assert original == cast_value

    def test_profiler_empty_table(self, tmp_path: Path):
        """Test profiling on an empty table."""
        db_path = tmp_path / "empty_table.sqlite"
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url)
        empty_table_name = "empty_table"
        metadata = MetaData()
        Table(
            empty_table_name,
            metadata,
            Column("id", String, primary_key=True),
        )
        metadata.create_all(engine)
        engine.dispose()

        # Create a DataLake for the empty table
        db_source = DbSource(db_url=db_url, db_schema=None, db_table=empty_table_name)
        data_lake = DataLake(db_source=db_source)
        profiler = Profiler(data_lake=data_lake)
        # Should not raise, but profiled_columns should be empty or TEXT
        profiler.profile(sample_size=10)
        for col in profiler.profiled_columns:
            assert col.inferred_type == DataType.TEXT

    def test_profiler_all_nulls(self, tmp_path: Path):
        """Test profiling on a table with only nulls."""
        db_path = tmp_path / "null_table.sqlite"
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url)
        null_table_name = "null_table"
        metadata = MetaData()
        table = Table(
            null_table_name,
            metadata,
            Column("id", String, primary_key=True),
            Column("value", String, nullable=True),
        )
        metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(table.insert(), [{"id": "1", "value": None}, {"id": "2", "value": None}])
            conn.commit()
        engine.dispose()

        db_source = DbSource(db_url=db_url, db_schema=None, db_table=null_table_name)
        data_lake = DataLake(db_source=db_source)
        profiler = Profiler(data_lake=data_lake)
        profiler.profile(sample_size=10)

        # Check that we have the expected columns
        assert len(profiler.profiled_columns) == 2

        # Find the value column (which should be all nulls and infer as TEXT)
        value_col = next(col for col in profiler.profiled_columns if col.name == "value")
        assert value_col.inferred_type == DataType.TEXT

        # The id column should be inferred as INTEGER since it contains numeric strings
        id_col = next(col for col in profiler.profiled_columns if col.name == "id")
        assert id_col.inferred_type == DataType.INTEGER

    def test_profiler_mixed_types(self, tmp_path: Path):
        """Test profiling on a table with mixed types."""
        db_path = tmp_path / "mixed_table.sqlite"
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url)
        mixed_table_name = "mixed_table"
        metadata = MetaData()
        table = Table(
            mixed_table_name,
            metadata,
            Column("id", String, primary_key=True),
            Column("value", String, nullable=True),
        )
        metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(
                table.insert(),
                [
                    {"id": "1", "value": "123"},
                    {"id": "2", "value": "abc"},
                    {"id": "3", "value": "456.7"},
                    {"id": "4", "value": "True"},
                ],
            )
            conn.commit()
        engine.dispose()

        db_source = DbSource(db_url=db_url, db_schema=None, db_table=mixed_table_name)
        data_lake = DataLake(db_source=db_source)
        profiler = Profiler(data_lake=data_lake)
        profiler.profile(sample_size=10)

        # Check that we have the expected columns
        assert len(profiler.profiled_columns) == 2

        # The value column should be inferred as TEXT since it contains mixed types
        value_col = next(col for col in profiler.profiled_columns if col.name == "value")
        assert value_col.inferred_type == DataType.TEXT

        # The id column should be inferred as INTEGER since it contains numeric strings
        id_col = next(col for col in profiler.profiled_columns if col.name == "id")
        assert id_col.inferred_type == DataType.INTEGER

    def test_profiler_db_connection_error(self):
        """Test profiler error on DB connection failure."""
        with pytest.raises(DatabaseError):
            DbSource(db_url="sqlite:///nonexistent.db", db_schema=None, db_table="no_table")


class TestProfilerEdgeCases:
    """Test edge cases and error conditions for Profiler."""

    def test_profiler_with_none_data_lake(self):
        """Test profiler initialization with None data lake."""
        with pytest.raises(ValueError):
            Profiler(data_lake=None)

    def test_profiler_reprofile_same_data(self, tmp_path: Path):
        """Test that reprofiling the same data produces consistent results."""
        # Create a simple test setup under pytest tmp_path
        temp_dir = tmp_path / "lake"
        temp_dir.mkdir()
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n", encoding="utf-8")

        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=Path(temp_dir))

        profiler = Profiler(data_lake=data_lake)

        # Profile first time (reduced sample size for performance)
        profiler.profile(sample_size=5)
        first_results = {col.name: col.inferred_type for col in profiler.profiled_columns}

        # Profile second time (reduced sample size for performance)
        profiler.profile(sample_size=5)
        second_results = {col.name: col.inferred_type for col in profiler.profiled_columns}

        # Results should be identical
        assert first_results == second_results

    def test_profiler_large_sample_size(self, tmp_path: Path):
        """Test profiler with sample size larger than available data."""
        temp_dir = tmp_path / "lake"
        temp_dir.mkdir()
        csv_path = tmp_path / "small.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Diana\n5,Eve\n", encoding="utf-8")

        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=Path(temp_dir))

        profiler = Profiler(data_lake=data_lake)

        # Try to profile with sample size larger than available data
        profiler.profile(sample_size=100)  # More than 5 rows

        # Should still work and profile all available data
        assert len(profiler.profiled_columns) > 0

    def test_calculate_adaptive_sample_size(self, tmp_path: Path):
        """
        Test the _calculate_adaptive_sample_size method directly to validate all assumptions.

        Uses pytest's tmp_path for temporary files/directories instead of tempfile.
        """

        # Create a minimal profiler environment under pytest tmp_path
        temp_dir = tmp_path / "lake"
        temp_dir.mkdir()
        csv_path = tmp_path / "sample.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        dsv_source = DsvSource(str(csv_path))
        DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=Path(temp_dir))

        # Test datasets < 5K rows (100% sample)
        test_cases_small = [
            (0, 0),
            (1, 1),
            (1000, 1000),
            (4999, 4999),
        ]

        for total_rows, expected_sample in test_cases_small:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test datasets 5K-10K rows (80% sample)
        test_cases_80 = [
            (5000, 4000),
            (7500, 6000),
            (9999, 7999),
        ]
        for total_rows, expected_sample in test_cases_80:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test datasets 10K-25K rows (60% sample)
        test_cases_60 = [
            (10000, 6000),
            (15000, 9000),
            (20000, 12000),
            (24999, 14999),
        ]
        for total_rows, expected_sample in test_cases_60:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test datasets 25K-100K rows (40% sample)
        test_cases_40 = [
            (25000, 10000),
            (50000, 20000),
            (75000, 30000),
            (99999, 39999),
        ]
        for total_rows, expected_sample in test_cases_40:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test datasets 100K-500K rows (20% sample)
        test_cases_20 = [
            (100000, 20000),
            (200000, 40000),
            (300000, 60000),
            (499999, 99999),
        ]
        # Test datasets > 500K rows (10% sample)
        test_cases_10 = [
            (500000, 50000),
            (1000000, 100000),
            (5000000, 500000),
            (10000000, 1000000),
        ]
        for total_rows, expected_sample in test_cases_10:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Ensure the test_cases_20 are also exercised to avoid unused assignment
        for total_rows, expected_sample in test_cases_20:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test boundary conditions and edge cases
        boundary_tests = [
            # Test exact boundaries
            (5000, 4000),  # Exactly at 5K boundary (80% sample)
            (10000, 6000),  # Exactly at 10K boundary (60% sample)
            (25000, 10000),  # Exactly at 25K boundary (40% sample)
            (100000, 20000),  # Exactly at 100K boundary (20% sample)
            (500000, 50000),  # Exactly at 500K boundary (10% sample)
            # Test one row before boundaries
            (4999, 4999),  # One row before 5K boundary (100% sample)
            (9999, 7999),  # One row before 10K boundary (80% sample)
            (24999, 14999),  # One row before 25K boundary (60% sample)
            (99999, 39999),  # One row before 100K boundary (40% sample)
            (499999, 99999),  # One row before 500K boundary (20% sample)
            # Test one row after boundaries
            (5001, 4000),  # One row after 5K boundary (80% sample)
            (10001, 6000),  # One row after 10K boundary (60% sample)
            (25001, 10000),  # One row after 25K boundary (40% sample)
            (100001, 20000),  # One row after 100K boundary (20% sample)
            (500001, 50000),  # One row after 500K boundary (10% sample)
        ]
        for total_rows, expected_sample in boundary_tests:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            expected = expected_sample
            assert sample_size == expected, f"Expected {expected} for {total_rows} rows, got {sample_size}"

        # Test that sample size never exceeds total rows
        for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size <= total_rows, f"Sample size {sample_size} should not exceed total rows {total_rows}"

        # Test that sample size is always non-negative
        for total_rows in [0, 1, 1000, 25000, 50000, 100000, 500000, 1000000]:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size >= 0, f"Sample size {sample_size} should be non-negative for {total_rows} rows"

        # Test that sample size is always an integer
        for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert isinstance(sample_size, int), (
                f"Sample size {sample_size} should be an integer for {total_rows} rows"
            )

    def test_profiler_properties(self, tmp_path: Path):
        """Test profiler properties."""
        temp_dir = tmp_path / "lake"
        temp_dir.mkdir()
        csv_path = tmp_path / "props.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=Path(temp_dir))

        profiler = Profiler(data_lake=data_lake)

        # Test properties before profiling
        assert profiler.data_lake == data_lake
        assert len(profiler.profiled_columns) == 2  # Always has columns with default TEXT type

        # Profile the data (reduced sample size for performance)
        profiler.profile(sample_size=5)

        # Test properties after profiling
        assert profiler.data_lake == data_lake
        assert len(profiler.profiled_columns) > 0


class TestProfilerTypeCasting:
    """Test cases for Profiler type casting functionality."""
    @pytest.fixture(autouse=True)
    def _class_tmpdir(self, tmp_path: Path):
        """Provide a pytest-managed temporary dir to class instances as self.temp_dir/self.data_lake_path."""
        self.temp_dir = tmp_path / "type_casting"
        self.temp_dir.mkdir()
        self.data_lake_path = Path(self.temp_dir)
        yield

    def test_cast_value_none_input(self, tmp_path: Path) -> None:
        """Test _cast_value with None input."""

        # Create a minimal profiler instance under pytest tmp_path
        csv_path = tmp_path / "none_input.csv"
        csv_path.write_text("id\n1\n", encoding="utf-8")
        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        profiler = Profiler(data_lake=data_lake)

        # Test None input for all data types -> expect None
        for data_type in [
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.TIME,
            DataType.DATETIME,
            DataType.TEXT,
        ]:
            result = profiler._cast_value(None, target_type=data_type)
            assert result is None

    def test_cast_value_empty_string(self, tmp_path: Path) -> None:
        """Test _cast_value with empty string input."""

        # Create a minimal profiler instance under pytest tmp_path
        csv_path = tmp_path / "empty_string.csv"
        csv_path.write_text("id\n1\n", encoding="utf-8")
        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        profiler = Profiler(data_lake=data_lake)

        # Test empty string input for all data types -> expect None
        for data_type in [
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.TIME,
            DataType.DATETIME,
            DataType.TEXT,
        ]:
            result = profiler._cast_value("", target_type=data_type)
            assert result is None

    def test_cast_value_integer_conversion(self, tmp_path: Path) -> None:
        """Test _cast_value integer conversion with various inputs."""

        # Create a minimal profiler instance under pytest tmp_path
        csv_path = tmp_path / "integer_conversion.csv"
        csv_path.write_text("id\n1\n", encoding="utf-8")
        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        profiler = Profiler(data_lake=data_lake)

        # Test valid integer conversions
        valid_integers = [
            ("123", 123),
            ("-456", -456),
            ("0", 0),
            ("  789  ", 789),  # With whitespace
            ("00123", 123),  # Leading zeros
        ]

        for input_str, expected in valid_integers:
            result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
            assert result == expected, f"Failed to convert '{input_str}' to {expected}"

        # Test invalid integer conversions
        invalid_integers = [
            "123.45",  # Float
            "abc",  # Text
            "12.3.4",  # Invalid format
            "",  # Empty
            " ",  # Whitespace
        ]

        for input_str in invalid_integers:
            result = profiler._cast_value(input_str, target_type=DataType.INTEGER)
            assert result is None

    def test_cast_value_boolean_conversion(self, tmp_path: Path) -> None:
        """Test _cast_value boolean conversion with various inputs."""

        # Create a minimal profiler instance under pytest tmp_path
        csv_path = tmp_path / "boolean_conversion.csv"
        csv_path.write_text("id\n1\n", encoding="utf-8")
        dsv_source = DsvSource(str(csv_path))
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=self.data_lake_path)
        profiler = Profiler(data_lake=data_lake)

        # Test true values (only lowercase versions are accepted)
        true_values = ["true", "yes", "y", "1", "on"]

        for input_str in true_values:
            result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
            assert result, f"'{input_str}' should convert to True"

        # Test false values (only lowercase versions are accepted)
        false_values = ["false", "no", "n", "0", "off"]

        for input_str in false_values:
            result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
            assert not result, f"'{input_str}' should convert to False"

        # Test uppercase/mixed case values (some are accepted by String.to_bool)
        mixed_case_cases = [
            ("True", True),  # Accepted
            ("TRUE", True),  # Accepted
            ("T", None),  # Not accepted (single char, uppercase only)
            ("Yes", True),  # Accepted
            ("YES", True),  # Accepted
            ("Y", True),  # Accepted (lowercases to 'y')
            ("False", False),  # Accepted
            ("FALSE", False),  # Accepted
            ("F", None),  # Not accepted (single char, uppercase only)
            ("No", False),  # Accepted
            ("NO", False),  # Accepted (lowercases to 'no')
            ("N", False),  # Accepted (lowercases to 'n')
        ]

        for input_str, expected in mixed_case_cases:
            result = profiler._cast_value(input_str, target_type=DataType.BOOLEAN)
            assert result == expected, f"'{input_str}' should convert to {expected}"

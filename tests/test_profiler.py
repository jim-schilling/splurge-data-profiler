import os
import tempfile
import unittest
from pathlib import Path
from typing import List
import random
import string
from datetime import datetime, date, time, timedelta
import math
import csv
from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, String

from splurge_data_profiler.source import DataType, DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake
from splurge_data_profiler.profiler import Profiler


class TestProfilerComprehensive(unittest.TestCase):
    """Test comprehensive profiling functionality."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test fixtures once for the entire test class."""
        # Create temporary directory for data lake
        cls.temp_dir = tempfile.mkdtemp()
        cls.data_lake_path = Path(cls.temp_dir)
        
        # Create temporary CSV file
        cls.temp_fd, cls.temp_path = tempfile.mkstemp(suffix=".csv")
        cls.csv_path = Path(cls.temp_path)
        
        # Generate comprehensive test data (reduced from 15000 to 1000 rows)
        cls._generate_comprehensive_csv()
        
        # Create DsvSource and DataLake
        cls.dsv_source = DsvSource(cls.csv_path, delimiter='|', bookend='"')
        cls.data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=cls.dsv_source,
            data_lake_path=cls.data_lake_path
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Clean up test fixtures once for the entire test class."""
        # Close and remove temporary CSV file
        try:
            os.close(cls.temp_fd)
            os.unlink(cls.temp_path)
        except (OSError, AttributeError):
            pass
        
        # Remove temporary directory and contents
        try:
            import shutil
            shutil.rmtree(cls.temp_dir)
        except OSError:
            pass

    def setUp(self) -> None:
        """Set up test fixtures for each test method."""
        # Create a fresh Profiler instance for each test
        self.profiler = Profiler(data_lake=self.data_lake)

    @classmethod
    def _generate_comprehensive_csv(cls) -> None:
        """Generate a comprehensive CSV file with all data types and 1000 rows."""
        column_configs = [
            # TEXT columns
            ("text_simple", "TEXT", cls._generate_text_values),
            ("text_names", "TEXT", cls._generate_name_values),
            ("text_emails", "TEXT", cls._generate_email_values),
            ("text_addresses", "TEXT", cls._generate_address_values),
            
            # INTEGER columns
            ("integer_small", "INTEGER", cls._generate_small_integer_values),
            ("integer_large", "INTEGER", cls._generate_large_integer_values),
            ("integer_negative", "INTEGER", cls._generate_negative_integer_values),
            ("integer_mixed", "INTEGER", cls._generate_mixed_integer_values),
            
            # FLOAT columns
            ("float_simple", "FLOAT", cls._generate_simple_float_values),
            ("float_precise", "FLOAT", cls._generate_precise_float_values),
            ("float_scientific", "FLOAT", cls._generate_scientific_float_values),
            ("float_currency", "FLOAT", cls._generate_currency_float_values),
            
            # BOOLEAN columns
            ("boolean_simple", "BOOLEAN", cls._generate_boolean_values),
            ("boolean_text", "BOOLEAN", cls._generate_boolean_text_values),
            ("boolean_mixed", "BOOLEAN", cls._generate_mixed_boolean_values),
            
            # DATE columns
            ("date_simple", "DATE", cls._generate_date_values),
            ("date_formatted", "DATE", cls._generate_formatted_date_values),
            ("date_mixed", "DATE", cls._generate_mixed_date_values),
            
            # TIME columns
            ("time_simple", "TIME", cls._generate_time_values),
            ("time_formatted", "TIME", cls._generate_formatted_time_values),
            ("time_mixed", "TIME", cls._generate_mixed_time_values),
            
            # DATETIME columns
            ("datetime_simple", "DATETIME", cls._generate_datetime_values),
            ("datetime_formatted", "DATETIME", cls._generate_formatted_datetime_values),
            ("datetime_mixed", "DATETIME", cls._generate_mixed_datetime_values),
        ]
        header = [config[0] for config in column_configs]
        # Use pipe as delimiter and double quote as bookend
        delimiter = '|'
        bookend = '"'
        with os.fdopen(cls.temp_fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for i in range(1000):
                row_data = [str(generator_func(i)) for _, _, generator_func in column_configs]
                writer.writerow(row_data)


    @classmethod
    def _generate_text_values(cls, index: int) -> str:
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
            "eu fugiat nulla pariatur"
        ]
        return texts[index % len(texts)]

    @classmethod
    def _generate_name_values(cls, index: int) -> str:
        """Generate name values."""
        first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
        return f"{first_names[index % len(first_names)]} {last_names[index % len(last_names)]}"

    @classmethod
    def _generate_email_values(cls, index: int) -> str:
        """Generate email values."""
        domains = ["example.com", "test.org", "sample.net", "demo.co.uk"]
        return f"user{index}@{domains[index % len(domains)]}"

    @classmethod
    def _generate_address_values(cls, index: int) -> str:
        """Generate address values."""
        streets = ["123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm Blvd"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston"]
        return f"{streets[index % len(streets)]}, {cities[index % len(cities)]}"

    @classmethod
    def _generate_small_integer_values(cls, index: int) -> int:
        """Generate small integer values."""
        return index % 100

    @classmethod
    def _generate_large_integer_values(cls, index: int) -> int:
        """Generate large integer values."""
        return 1000000 + index

    @classmethod
    def _generate_negative_integer_values(cls, index: int) -> int:
        """Generate negative integer values."""
        return -1000 - index

    @classmethod
    def _generate_mixed_integer_values(cls, index: int) -> int:
        """Generate mixed integer values (including some text)."""
        if index % 20 == 0:  # 5% of values are text
            return f"text_{index}"
        return index * 10

    @classmethod
    def _generate_simple_float_values(cls, index: int) -> float:
        """Generate simple float values."""
        return index * 1.5

    @classmethod
    def _generate_precise_float_values(cls, index: int) -> float:
        """Generate precise float values."""
        return round(index * 3.14159, 5)

    @classmethod
    def _generate_scientific_float_values(cls, index: int) -> float:
        """Generate scientific notation float values."""
        return index * 1e6

    @classmethod
    def _generate_currency_float_values(cls, index: int) -> float:
        """Generate currency-like float values."""
        return round(index * 10.99, 2)

    @classmethod
    def _generate_boolean_values(cls, index: int) -> bool:
        """Generate boolean values."""
        return bool(index % 2)

    @classmethod
    def _generate_boolean_text_values(cls, index: int) -> str:
        """Generate boolean text values."""
        return "true" if index % 2 else "false"

    @classmethod
    def _generate_mixed_boolean_values(cls, index: int) -> str:
        """Generate mixed boolean values."""
        values = ["true", "false", "yes", "no", "1", "0", "Y", "N"]
        return values[index % len(values)]

    @classmethod
    def _generate_date_values(cls, index: int) -> date:
        """Generate date values."""
        start_date = date(2020, 1, 1)
        return start_date + timedelta(days=index)

    @classmethod
    def _generate_formatted_date_values(cls, index: int) -> str:
        """Generate formatted date values."""
        start_date = date(2020, 1, 1)
        date_obj = start_date + timedelta(days=index)
        return date_obj.strftime("%m/%d/%Y")

    @classmethod
    def _generate_mixed_date_values(cls, index: int) -> str:
        """Generate mixed date values."""
        if index % 10 == 0:  # 10% are invalid dates
            return f"invalid_date_{index}"
        start_date = date(2020, 1, 1)
        date_obj = start_date + timedelta(days=index)
        return date_obj.strftime("%Y-%m-%d")

    @classmethod
    def _generate_time_values(cls, index: int) -> time:
        """Generate time values."""
        return time(hour=index % 24, minute=index % 60, second=index % 60)

    @classmethod
    def _generate_formatted_time_values(cls, index: int) -> str:
        """Generate formatted time values."""
        time_obj = time(hour=index % 24, minute=index % 60, second=index % 60)
        return time_obj.strftime("%H:%M:%S")

    @classmethod
    def _generate_mixed_time_values(cls, index: int) -> str:
        """Generate mixed time values."""
        if index % 15 == 0:  # ~6.7% are invalid times
            return f"invalid_time_{index}"
        time_obj = time(hour=index % 24, minute=index % 60, second=index % 60)
        return time_obj.strftime("%I:%M %p")

    @classmethod
    def _generate_datetime_values(cls, index: int) -> str:
        """Generate datetime values in ISO 8601 format."""
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def _generate_formatted_datetime_values(cls, index: int) -> str:
        """Generate formatted datetime values in ISO 8601 format."""
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def _generate_mixed_datetime_values(cls, index: int) -> str:
        """Generate mixed datetime values."""
        if index % 25 == 0:  # 4% are invalid datetimes
            return f"invalid_datetime_{index}"
        start_datetime = datetime(2020, 1, 1, 0, 0, 0)
        datetime_obj = start_datetime + timedelta(hours=index)
        return datetime_obj.strftime("%m/%d/%Y %I:%M %p")

    def test_profiler_initialization(self) -> None:
        """Test Profiler initialization."""
        self.assertIsNotNone(self.profiler)
        self.assertEqual(len(self.profiler.profiled_columns), len(self.dsv_source.columns))
        
        # Check that profiled columns are copies
        for i, column in enumerate(self.profiler.profiled_columns):
            self.assertEqual(column.name, self.dsv_source.columns[i].name)
            self.assertIsNot(column, self.dsv_source.columns[i])

    def test_profiler_string_representation(self) -> None:
        """Test Profiler string representation."""
        expected_str = f"Profiler(data_lake={self.data_lake}, profiled_columns={len(self.profiler.profiled_columns)})"
        self.assertEqual(str(self.profiler), expected_str)

    def test_profiler_repr_representation(self) -> None:
        """Test Profiler repr representation."""
        repr_str = repr(self.profiler)
        self.assertIn("Profiler", repr_str)
        self.assertIn("data_lake=", repr_str)
        self.assertIn("profiled_columns=", repr_str)

    def test_profiler_equality(self) -> None:
        """Test Profiler equality comparison."""
        profiler1 = Profiler(data_lake=self.data_lake)
        profiler2 = Profiler(data_lake=self.data_lake)
        
        # They should be equal since they have the same data lake
        self.assertEqual(profiler1, profiler2)
        
        # Create a different data lake by modifying the profiled columns
        profiler1.profile(sample_size=100)  # This modifies the profiled columns
        profiler3 = Profiler(data_lake=self.data_lake)  # Fresh profiler with same data lake
        
        # They should not be equal since profiler1 has profiled columns and profiler3 doesn't
        self.assertNotEqual(profiler1, profiler3)

    def test_profiler_equality_different_type(self) -> None:
        """Test Profiler equality with different type."""
        other = "not a profiler"
        self.assertNotEqual(self.profiler, other)

    def test_profiler_comprehensive_profiling(self) -> None:
        """Test comprehensive profiling with all data types."""
        # Run profiling (reduced sample size for performance)
        self.profiler.profile(sample_size=500)
        
        # Get profiled columns
        profiled_columns = self.profiler.profiled_columns
        

        
        # Verify that all columns were profiled
        self.assertEqual(len(profiled_columns), 24)  # 24 columns total
        
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
                self.assertEqual(
                    column.inferred_type,
                    expected_types[column.name],
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
                self.assertEqual(
                    col_100, col_500,
                    f"Sample size 100 vs 500 inconsistent for {column_name}"
                )
                self.assertEqual(
                    col_500, col_1000,
                    f"Sample size 500 vs 1000 inconsistent for {column_name}"
                )

    def test_profiler_original_data_unmodified(self) -> None:
        """Test that original DataLake and DbSource remain unmodified."""
        # Store original inferred types
        original_types = [col.inferred_type for col in self.data_lake.db_source.columns]
        
        # Run profiling (reduced sample size for performance)
        self.profiler.profile(sample_size=500)
        
        # Check that original types are unchanged
        current_types = [col.inferred_type for col in self.data_lake.db_source.columns]
        self.assertEqual(original_types, current_types)
        
        # Check that profiled columns have updated types
        profiled_types = [col.inferred_type for col in self.profiler.profiled_columns]
        self.assertNotEqual(original_types, profiled_types)

    def test_profiler_large_dataset_performance(self) -> None:
        """Test profiling performance with large dataset."""
        import time
        
        # Time the profiling operation (reduced sample size for performance)
        start_time = time.time()
        self.profiler.profile(sample_size=1000)
        end_time = time.time()
        
        profiling_time = end_time - start_time
        
        # Profiling should complete within reasonable time (reduced threshold)
        self.assertLess(profiling_time, 30.0, f"Profiling took {profiling_time:.2f} seconds, should be under 30 seconds")
        
        # Verify results were obtained
        profiled_columns = self.profiler.profiled_columns
        self.assertTrue(any(col.inferred_type != DataType.TEXT for col in profiled_columns))

    def test_profiler_error_handling(self) -> None:
        """Test profiler error handling with invalid database connection."""
        # Create profiler with invalid data lake
        invalid_data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=self.dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Manually corrupt the database URL
        invalid_data_lake._db_url = "sqlite:///nonexistent.db"
        
        invalid_profiler = Profiler(data_lake=invalid_data_lake)
        
        # Should raise RuntimeError when trying to profile
        with self.assertRaises(RuntimeError):
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
        self.assertIsNotNone(new_table_name)
        self.assertEqual(new_table_name, f"{self.data_lake.db_table}_inferred")
        
        # Connect to database and verify table structure
        engine = create_engine(self.data_lake.db_url)
        
        try:
            with engine.connect() as connection:
                # Get table information
                inspector = inspect(engine)
                columns_info = inspector.get_columns(new_table_name)
                
                # Verify we have the expected number of columns
                # Original columns + cast columns = 24 * 2 = 48 columns
                self.assertEqual(len(columns_info), 48)
                
                # Verify column structure
                column_names = [col['name'] for col in columns_info]
                
                # Check that we have both original and cast columns
                for column in self.profiler.profiled_columns:
                    # Original column should exist
                    self.assertIn(column.name, column_names, 
                                f"Original column {column.name} not found")
                    
                    # Cast column should exist
                    cast_col_name = f"{column.name}_cast"
                    self.assertIn(cast_col_name, column_names,
                                f"Cast column {cast_col_name} not found")
                
                # Verify data was populated
                result = connection.execute(text(f"SELECT COUNT(*) FROM {new_table_name}"))
                row_count = result.fetchone()[0]
                self.assertEqual(row_count, 1000, "Table should have 1000 rows")
                
                # Test specific casting examples
                self._verify_casting_examples(connection, new_table_name)
                
        finally:
            engine.dispose()

    def _verify_casting_examples(
            self,
            connection,
            table_name: str
    ) -> None:
        """Verify specific casting examples work correctly."""
        
        # Test integer casting
        result = connection.execute(text(
            f"SELECT integer_small, integer_small_cast FROM {table_name} "
            "WHERE integer_small IS NOT NULL LIMIT 5"
        ))
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                self.assertIsInstance(cast_value, int)
                self.assertEqual(int(original), cast_value)
        
        # Test float casting
        result = connection.execute(text(
            f"SELECT float_simple, float_simple_cast FROM {table_name} "
            "WHERE float_simple IS NOT NULL LIMIT 5"
        ))
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                self.assertIsInstance(cast_value, float)
                self.assertAlmostEqual(float(original), cast_value, places=6)
        
        # Test boolean casting
        result = connection.execute(text(
            f"SELECT boolean_simple, boolean_simple_cast FROM {table_name} "
            "WHERE boolean_simple IS NOT NULL LIMIT 5"
        ))
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                # SQLite stores booleans as 0 or 1, so check for integer values
                if isinstance(cast_value, int):
                    # Convert 0/1 to True/False for testing
                    cast_value = bool(cast_value)
                self.assertIsInstance(cast_value, bool)
                # Boolean casting should work correctly
                expected_bool = original.lower() in ['true', '1', 'yes', 'y']
                self.assertEqual(expected_bool, cast_value)
        
        # Test date casting
        result = connection.execute(text(
            f"SELECT date_simple, date_simple_cast FROM {table_name} "
            "WHERE date_simple IS NOT NULL LIMIT 5"
        ))
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original and cast_value is not None:
                # SQLite returns dates as strings, but we can verify format
                self.assertIsInstance(cast_value, str)
                # Should be in YYYY-MM-DD format
                self.assertRegex(cast_value, r'^\d{4}-\d{2}-\d{2}$')
        
        # Test text columns (should remain as text)
        result = connection.execute(text(
            f"SELECT text_simple, text_simple_cast FROM {table_name} "
            "WHERE text_simple IS NOT NULL LIMIT 5"
        ))
        rows = result.fetchall()
        for row in rows:
            original, cast_value = row
            if original:
                self.assertEqual(original, cast_value)

    def test_profiler_empty_table(self):
        """Test profiling on an empty table."""
        # Create a new empty table in a separate database
        temp_db_path = tempfile.mktemp(suffix=".sqlite")
        db_url = f"sqlite:///{temp_db_path}"
        engine = create_engine(db_url)
        empty_table_name = "empty_table"
        metadata = MetaData()
        table = Table(
            empty_table_name, metadata,
            Column("id", String, primary_key=True),
        )
        metadata.create_all(engine)
        engine.dispose()
        
        try:
            # Create a DataLake for the empty table
            db_source = DbSource(
                db_url=db_url,
                db_schema=None,
                db_table=empty_table_name
            )
            data_lake = DataLake(db_source=db_source)
            profiler = Profiler(data_lake=data_lake)
            # Should not raise, but profiled_columns should be empty or TEXT
            profiler.profile(sample_size=10)
            for col in profiler.profiled_columns:
                self.assertEqual(col.inferred_type, DataType.TEXT)
        finally:
            # Clean up temporary database
            try:
                os.remove(temp_db_path)
            except OSError:
                pass

    def test_profiler_all_nulls(self):
        """Test profiling on a table with only nulls."""
        # Create a new table in a separate database
        temp_db_path = tempfile.mktemp(suffix=".sqlite")
        db_url = f"sqlite:///{temp_db_path}"
        engine = create_engine(db_url)
        null_table_name = "null_table"
        metadata = MetaData()
        table = Table(
            null_table_name, metadata,
            Column("id", String, primary_key=True),
            Column("value", String, nullable=True),
        )
        metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(table.insert(), [{"id": "1", "value": None}, {"id": "2", "value": None}])
            conn.commit()
        engine.dispose()
        
        try:
            db_source = DbSource(
                db_url=db_url,
                db_schema=None,
                db_table=null_table_name
            )
            data_lake = DataLake(db_source=db_source)
            profiler = Profiler(data_lake=data_lake)
            profiler.profile(sample_size=10)
            
            # Check that we have the expected columns
            self.assertEqual(len(profiler.profiled_columns), 2)
            
            # Find the value column (which should be all nulls and infer as TEXT)
            value_col = next(col for col in profiler.profiled_columns if col.name == "value")
            self.assertEqual(value_col.inferred_type, DataType.TEXT)
            
            # The id column should be inferred as INTEGER since it contains numeric strings
            id_col = next(col for col in profiler.profiled_columns if col.name == "id")
            self.assertEqual(id_col.inferred_type, DataType.INTEGER)
        finally:
            # Clean up temporary database
            try:
                os.remove(temp_db_path)
            except OSError:
                pass

    def test_profiler_mixed_types(self):
        """Test profiling on a table with mixed types."""
        # Create a new table in a separate database
        temp_db_path = tempfile.mktemp(suffix=".sqlite")
        db_url = f"sqlite:///{temp_db_path}"
        engine = create_engine(db_url)
        mixed_table_name = "mixed_table"
        metadata = MetaData()
        table = Table(
            mixed_table_name, metadata,
            Column("id", String, primary_key=True),
            Column("value", String, nullable=True),
        )
        metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(table.insert(), [
                {"id": "1", "value": "123"},
                {"id": "2", "value": "abc"},
                {"id": "3", "value": "456.7"},
                {"id": "4", "value": "True"},
            ])
            conn.commit()
        engine.dispose()
        
        try:
            db_source = DbSource(
                db_url=db_url,
                db_schema=None,
                db_table=mixed_table_name
            )
            data_lake = DataLake(db_source=db_source)
            profiler = Profiler(data_lake=data_lake)
            profiler.profile(sample_size=10)
            
            # Check that we have the expected columns
            self.assertEqual(len(profiler.profiled_columns), 2)
            
            # The value column should be inferred as TEXT since it contains mixed types
            value_col = next(col for col in profiler.profiled_columns if col.name == "value")
            self.assertEqual(value_col.inferred_type, DataType.TEXT)
            
            # The id column should be inferred as INTEGER since it contains numeric strings
            id_col = next(col for col in profiler.profiled_columns if col.name == "id")
            self.assertEqual(id_col.inferred_type, DataType.INTEGER)
        finally:
            # Clean up temporary database
            try:
                os.remove(temp_db_path)
            except OSError:
                pass

    def test_profiler_db_connection_error(self):
        """Test profiler error on DB connection failure."""
        with self.assertRaises(RuntimeError):
            db_source = DbSource(
                db_url="sqlite:///nonexistent.db",
                db_schema=None,
                db_table="no_table"
            )


class TestProfilerEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for Profiler."""

    def test_profiler_with_none_data_lake(self):
        """Test profiler initialization with None data lake."""
        with self.assertRaises(ValueError):
            Profiler(data_lake=None)

    def test_profiler_reprofile_same_data(self):
        """Test that reprofiling the same data produces consistent results."""
        # Create a simple test setup
        temp_dir = tempfile.mkdtemp()
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        
        try:
            # Create simple test data
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")
            
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=Path(temp_dir)
            )
            
            profiler = Profiler(data_lake=data_lake)
            
            # Profile first time (reduced sample size for performance)
            profiler.profile(sample_size=5)
            first_results = {col.name: col.inferred_type for col in profiler.profiled_columns}
            
            # Profile second time (reduced sample size for performance)
            profiler.profile(sample_size=5)
            second_results = {col.name: col.inferred_type for col in profiler.profiled_columns}
            
            # Results should be identical
            self.assertEqual(first_results, second_results)
            
        finally:
            # Robust cleanup with exception handling
            try:
                os.close(temp_fd)
            except OSError:
                pass  # File descriptor already closed
            try:
                os.unlink(temp_path)
            except OSError:
                pass  # File may not exist or be locked
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except OSError:
                pass  # Directory may not exist or be locked

    def test_profiler_large_sample_size(self):
        """Test profiler with sample size larger than available data."""
        temp_dir = tempfile.mkdtemp()
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        
        try:
            # Create small test data (only 5 rows)
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Diana\n5,Eve\n")
            
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=Path(temp_dir)
            )
            
            profiler = Profiler(data_lake=data_lake)
            
            # Try to profile with sample size larger than available data
            profiler.profile(sample_size=100)  # More than 5 rows
            
            # Should still work and profile all available data
            self.assertGreater(len(profiler.profiled_columns), 0)
            
        finally:
            # Robust cleanup with exception handling
            try:
                os.close(temp_fd)
            except OSError:
                pass  # File descriptor already closed
            try:
                os.unlink(temp_path)
            except OSError:
                pass  # File may not exist or be locked
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except OSError:
                pass  # Directory may not exist or be locked

    def test_calculate_adaptive_sample_size(self):
        """
        Test the _calculate_adaptive_sample_size method directly to validate all assumptions.
        
        Tests all the adaptive sampling strategy boundaries and calculations:
        - Datasets < 25K rows: 100% sample
        - Datasets 25K-50K rows: 50% sample
        - Datasets 50K-100K rows: 25% sample  
        - Datasets 100K-500K rows: 20% sample
        - Datasets > 500K rows: 10% sample
        """
        # Create a minimal profiler instance for testing
        temp_dir = tempfile.mkdtemp()
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        
        try:
            # Create minimal test data
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=Path(temp_dir)
            )
            
            profiler = Profiler(data_lake=data_lake)
            
            # Test datasets < 25K rows (100% sample)
            test_cases_small = [
                (0, 0),
                (1, 1),
                (1000, 1000),
                (10000, 10000),
                (24999, 24999),
            ]
            
            for total_rows, expected_sample in test_cases_small:
                with self.subTest(total_rows=total_rows):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample, 
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test datasets 25K-50K rows (50% sample)
            test_cases_medium = [
                (25000, 12500),
                (30000, 15000),
                (40000, 20000),
                (49999, 24999),
            ]
            
            for total_rows, expected_sample in test_cases_medium:
                with self.subTest(total_rows=total_rows):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample,
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test datasets 50K-100K rows (25% sample)
            test_cases_large = [
                (50000, 12500),
                (60000, 15000),
                (80000, 20000),
                (99999, 24999),
            ]
            
            for total_rows, expected_sample in test_cases_large:
                with self.subTest(total_rows=total_rows):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample,
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test datasets 100K-500K rows (20% sample)
            test_cases_xlarge = [
                (100000, 20000),
                (200000, 40000),
                (300000, 60000),
                (499999, 99999),
            ]
            
            for total_rows, expected_sample in test_cases_xlarge:
                with self.subTest(total_rows=total_rows):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample,
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test datasets > 500K rows (10% sample)
            test_cases_huge = [
                (500000, 50000),
                (1000000, 100000),
                (5000000, 500000),
                (10000000, 1000000),
            ]
            
            for total_rows, expected_sample in test_cases_huge:
                with self.subTest(total_rows=total_rows):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample,
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test boundary conditions and edge cases
            boundary_tests = [
                # Test exact boundaries
                (25000, 12500),  # Exactly at 25K boundary
                (50000, 12500),  # Exactly at 50K boundary
                (100000, 20000), # Exactly at 100K boundary
                (500000, 50000), # Exactly at 500K boundary
                
                # Test one row before boundaries
                (24999, 24999),  # One row before 25K boundary
                (49999, 24999),  # One row before 50K boundary
                (99999, 24999),  # One row before 100K boundary
                (499999, 99999), # One row before 500K boundary
                
                # Test one row after boundaries
                (25001, 12500),  # One row after 25K boundary
                (50001, 12500),  # One row after 50K boundary (25% of 50001 = 12500)
                (100001, 20000), # One row after 100K boundary
                (500001, 50000), # One row after 500K boundary
            ]
            
            for total_rows, expected_sample in boundary_tests:
                with self.subTest(total_rows=total_rows, boundary_test=True):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, expected_sample,
                                   f"Expected {expected_sample} for {total_rows} rows, got {sample_size}")
            
            # Test that sample size never exceeds total rows
            for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, max_check=True):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertLessEqual(sample_size, total_rows,
                                       f"Sample size {sample_size} should not exceed total rows {total_rows}")
            
            # Test that sample size is always non-negative
            for total_rows in [0, 1, 1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, non_negative_check=True):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertGreaterEqual(sample_size, 0,
                                          f"Sample size {sample_size} should be non-negative for {total_rows} rows")
            
            # Test that sample size is always an integer
            for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, integer_check=True):
                    sample_size = profiler._calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertIsInstance(sample_size, int,
                                        f"Sample size {sample_size} should be an integer for {total_rows} rows")
            
        finally:
            # Robust cleanup with exception handling
            try:
                os.close(temp_fd)
            except OSError:
                pass  # File descriptor already closed
            try:
                os.unlink(temp_path)
            except OSError:
                pass  # File may not exist or be locked
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except OSError:
                pass  # Directory may not exist or be locked

    def test_profiler_properties(self):
        """Test profiler properties."""
        temp_dir = tempfile.mkdtemp()
        temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
        
        try:
            # Create test data
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            
            dsv_source = DsvSource(temp_path)
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=Path(temp_dir)
            )
            
            profiler = Profiler(data_lake=data_lake)
            
            # Test properties before profiling
            self.assertEqual(profiler.data_lake, data_lake)
            self.assertEqual(len(profiler.profiled_columns), 2)  # Always has columns with default TEXT type
            
            # Profile the data (reduced sample size for performance)
            profiler.profile(sample_size=5)
            
            # Test properties after profiling
            self.assertEqual(profiler.data_lake, data_lake)
            self.assertGreater(len(profiler.profiled_columns), 0)
            
        finally:
            # Robust cleanup with exception handling
            try:
                os.close(temp_fd)
            except OSError:
                pass  # File descriptor already closed
            try:
                os.unlink(temp_path)
            except OSError:
                pass  # File may not exist or be locked
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except OSError:
                pass  # Directory may not exist or be locked


if __name__ == '__main__':
    unittest.main() 
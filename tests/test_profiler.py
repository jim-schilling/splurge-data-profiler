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
from sqlalchemy import create_engine, inspect, text

from splurge_data_profiler.source import DataType, DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


class TestProfilerComprehensive(unittest.TestCase):
    """Comprehensive test cases for Profiler with all data types."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create temporary directory for data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)
        
        # Create temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        self.csv_path = Path(self.temp_path)
        
        # Generate comprehensive test data
        self._generate_comprehensive_csv()
        
        # Create DsvSource and DataLake
        self.dsv_source = DsvSource(self.csv_path, delimiter='|', bookend='"')
        self.data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=self.dsv_source,
            data_lake_path=self.data_lake_path
        )
        
        # Create Profiler
        self.profiler = Profiler(data_lake=self.data_lake)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Close and remove temporary CSV file
        try:
            os.close(self.temp_fd)
            os.unlink(self.temp_path)
        except (OSError, AttributeError):
            pass
        
        # Remove temporary directory and contents
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def _generate_comprehensive_csv(self) -> None:
        """Generate a comprehensive CSV file with all data types and 15000 rows."""
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
        delimiter = '|'
        bookend = '"'
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for i in range(15000):
                row_data = [str(generator_func(i)) for _, _, generator_func in column_configs]
                writer.writerow(row_data)
                if i == 0:
                    print(f"DEBUG: CSV first row data: {row_data}")
                    print(f"DEBUG: CSV first row header: {header}")
                    print(f"DEBUG: CSV first row zipped: {list(zip(header, row_data))}")

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
            "eu fugiat nulla pariatur"
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
        self.assertIsNotNone(self.profiler)
        self.assertEqual(len(self.profiler.profiled_columns), len(self.dsv_source.columns))
        
        # Check that profiled columns are copies
        for i, column in enumerate(self.profiler.profiled_columns):
            self.assertEqual(column.name, self.dsv_source.columns[i].name)
            self.assertIsNot(column, self.dsv_source.columns[i])

    def test_profiler_comprehensive_profiling(self) -> None:
        """Test comprehensive profiling with all data types."""
        # Run profiling
        self.profiler.profile(sample_size=5000)
        
        # Get profiled columns
        profiled_columns = self.profiler.profiled_columns
        
        # Debug output
        print(f"Expected columns: 24")
        print(f"Actual columns: {len(profiled_columns)}")
        print(f"Column names: {[col.name for col in profiled_columns]}")
        
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

        # Print all inferred types for debugging
        for column in profiled_columns:
            print(f"Column: {column.name}, Inferred Type: {column.inferred_type}")

    def test_profiler_sample_size_effectiveness(self) -> None:
        """Test that different sample sizes produce consistent results."""
        # Profile with different sample sizes
        self.profiler.profile(sample_size=1000)
        results_1000 = [col.inferred_type for col in self.profiler.profiled_columns]
        
        self.profiler.profile(sample_size=5000)
        results_5000 = [col.inferred_type for col in self.profiler.profiled_columns]
        
        self.profiler.profile(sample_size=10000)
        results_10000 = [col.inferred_type for col in self.profiler.profiled_columns]
        
        # Results should be consistent across sample sizes for well-defined data types
        # (Allow some variation for mixed columns)
        for i, (col_1000, col_5000, col_10000) in enumerate(zip(results_1000, results_5000, results_10000)):
            column_name = self.profiler.profiled_columns[i].name
            if "mixed" not in column_name:  # Skip mixed columns
                self.assertEqual(
                    col_1000, col_5000,
                    f"Sample size 1000 vs 5000 inconsistent for {column_name}"
                )
                self.assertEqual(
                    col_5000, col_10000,
                    f"Sample size 5000 vs 10000 inconsistent for {column_name}"
                )

    def test_profiler_original_data_unmodified(self) -> None:
        """Test that original DataLake and DbSource remain unmodified."""
        # Store original inferred types
        original_types = [col.inferred_type for col in self.data_lake.db_source.columns]
        
        # Run profiling
        self.profiler.profile(sample_size=5000)
        
        # Check that original types are unchanged
        current_types = [col.inferred_type for col in self.data_lake.db_source.columns]
        self.assertEqual(original_types, current_types)
        
        # Check that profiled columns have updated types
        profiled_types = [col.inferred_type for col in self.profiler.profiled_columns]
        self.assertNotEqual(original_types, profiled_types)

    def test_profiler_large_dataset_performance(self) -> None:
        """Test profiling performance with large dataset."""
        import time
        
        # Time the profiling operation
        start_time = time.time()
        self.profiler.profile(sample_size=10000)
        end_time = time.time()
        
        profiling_time = end_time - start_time
        
        # Profiling should complete within reasonable time (adjust threshold as needed)
        self.assertLess(profiling_time, 60.0, f"Profiling took {profiling_time:.2f} seconds, should be under 60 seconds")
        
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
        # First profile the data
        self.profiler.profile(sample_size=1000)
        
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
                self.assertEqual(row_count, 15000, "Table should have 15000 rows")
                
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


if __name__ == '__main__':
    unittest.main() 
"""
Basic performance tests for splurge-data-profiler.

This module contains basic performance tests that measure the efficiency
of data profiling operations without requiring external dependencies.
"""

import csv
import random
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

from sqlalchemy import create_engine, text

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource


class BasicPerformanceTests(unittest.TestCase):
    """Basic performance tests for data profiling operations."""

    # Sample data for generating realistic records
    FIRST_NAMES = [
        "Alice", "Bob", "Charlie", "Dana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
        "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ruby", "Sam", "Tara"
    ]

    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"
    ]

    CITIES = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
        "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus"
    ]

    DEPARTMENTS = [
        "Engineering", "Marketing", "Sales", "HR", "Legal", "Finance", "Operations",
        "Customer Support", "Product Management", "Research & Development"
    ]

    def setUp(self) -> None:
        """Set up test fixtures for each test method."""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = Path(self.temp_dir)
        
        # Create a single large DSV file for all tests
        self.dsv_path = self.test_dir / "performance_test_250k.dsv"
        self._generate_dsv(self.dsv_path, num_rows=250000, delimiter="|", bookend='"')

    def tearDown(self) -> None:
        """Clean up test fixtures after each test method."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass

    def _generate_dsv(
            self,
            file_path: Path,
            *,
            num_rows: int = 1000,
            delimiter: str = "|",
            bookend: str = '"'
    ) -> None:
        """
        Generate a DSV file with the specified number of rows.
        
        Args:
            file_path: Path where the DSV file will be created
            num_rows: Number of rows to generate (default: 1000)
            delimiter: Delimiter character (default: |)
            bookend: Bookend/quote character (default: ")
        """
        columns = [
            "id", "name", "email", "age", "city", "salary", "department", "hire_date",
            "score", "last_login", "shift_start", "is_active"
        ]
        start_date = datetime(2015, 1, 1)
        
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter, quotechar=bookend, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            
            for i in range(1, num_rows + 1):
                first_name = random.choice(self.FIRST_NAMES)
                last_name = random.choice(self.LAST_NAMES)
                name = f"{first_name} {last_name}"
                email = f"{first_name.lower()}.{last_name.lower()}@example.com"
                age = random.randint(21, 65)
                city = random.choice(self.CITIES)
                salary = random.randint(40000, 180000)
                department = random.choice(self.DEPARTMENTS)
                hire_date = (start_date + timedelta(days=random.randint(0, 365 * 8))).date().isoformat()
                score = round(random.uniform(0, 100), 2)
                last_login = (
                    start_date + timedelta(
                        days=random.randint(0, 365 * 8), 
                        hours=random.randint(0, 23), 
                        minutes=random.randint(0, 59)
                    )
                ).isoformat(sep="T", timespec="seconds")
                shift_start = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                is_active = random.choice(["true", "false"])
                row = [
                    str(i), name, email, str(age), city, str(salary), department, hire_date,
                    str(score), last_login, shift_start, is_active
                ]
                writer.writerow(row)

    def _truncate_dsv_file(self, num_rows: int) -> None:
        """
        Truncate the master DSV file to the specified number of rows.
        
        Args:
            num_rows: Number of rows to keep (including header)
        """
        self._truncate_dsv_file_copy(self.dsv_path, num_rows)

    def _truncate_dsv_file_copy(self, file_path: Path, num_rows: int) -> None:
        """
        Truncate a DSV file to the specified number of rows.
        
        Args:
            file_path: Path to the DSV file to truncate
            num_rows: Number of rows to keep (including header)
        """
        # Read all lines
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Keep header + specified number of data rows
        lines_to_keep = lines[:num_rows + 1]  # +1 for header
        
        # Write back truncated file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines_to_keep)

    def _print_performance_summary(
            self,
            test_name: str,
            results: Dict[str, float],
            *,
            num_rows: int,
            db_path: Path
    ) -> None:
        """
        Print a formatted performance summary.
        
        Args:
            test_name: Name of the test
            results: Dictionary containing timing results
            num_rows: Number of rows processed
            db_path: Path to the database file
        """
        # Get database file size
        db_size_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0
        
        print(f"\n{'='*60}")
        print(f"PERFORMANCE SUMMARY: {test_name}")
        print(f"{'='*60}")
        print(f"Dataset Size: {num_rows:,} rows")
        print(f"Database File: {db_path.name}")
        print(f"Database Size: {db_size_mb:.2f} MB")
        print(f"Database Creation: {results['db_creation_time']:.3f}s")
        print(f"Data Profiling: {results['profiling_time']:.3f}s")
        print(f"Table Creation: {results['table_creation_time']:.3f}s")
        print(f"Total Time: {results['total_time']:.3f}s")
        print(f"Rows per Second: {num_rows / results['total_time']:.0f}")
        print(f"Profiling Efficiency: {num_rows / results['profiling_time']:.0f} rows/s")
        print(f"Database I/O: {db_size_mb / results['total_time']:.2f} MB/s")
        print(f"{'='*60}")

    def _run_performance_test(self, num_rows: int) -> Tuple[Dict[str, float], Path]:
        """
        Run a complete performance test for the given number of rows.
        
        Args:
            num_rows: Number of rows to test
            
        Returns:
            Tuple of (timing results dictionary, actual database path)
        """
        # Create a unique database name for this test
        timestamp = int(time.time() * 1000) % 100000
        unique_db_name = f"performance_test_{num_rows}_{timestamp}"
        unique_dsv_path = self.test_dir / f"{unique_db_name}.dsv"
        
        # Copy the master DSV file and truncate the copy
        import shutil
        shutil.copy2(self.dsv_path, unique_dsv_path)
        
        # Truncate the copy to the desired size
        self._truncate_dsv_file_copy(unique_dsv_path, num_rows)
        
        # Create DsvSource with unique file
        dsv_source = DsvSource(
            file_path=str(unique_dsv_path),
            delimiter="|",
            bookend='"'
        )

        # Create data lake with file-based database
        start_time = time.time()
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.test_dir
        )
        db_creation_time = time.time() - start_time
        
        # Get the actual database path that was created
        actual_db_path = self.test_dir / f"{unique_db_name}.sqlite"

        # Measure profiling time
        profiler = Profiler(data_lake=data_lake)
        start_time = time.time()
        profiler.profile()  # Uses adaptive sampling by default
        profiling_time = time.time() - start_time

        # Measure inferred table creation time
        start_time = time.time()
        inferred_table_name = profiler.create_inferred_table()
        table_creation_time = time.time() - start_time

        # Verify row counts
        engine = create_engine(data_lake.db_url)
        try:
            with engine.connect() as connection:
                # Check original table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {data_lake.db_table}"))
                original_count = result.fetchone()[0]
                
                # Check inferred table
                result = connection.execute(text(f"SELECT COUNT(*) FROM {inferred_table_name}"))
                inferred_count = result.fetchone()[0]
        finally:
            engine.dispose()

        # Verify data integrity
        self.assertEqual(original_count, num_rows, f"Original table should have {num_rows} rows")
        self.assertEqual(inferred_count, num_rows, f"Inferred table should have {num_rows} rows")

        results = {
            'db_creation_time': db_creation_time,
            'profiling_time': profiling_time,
            'table_creation_time': table_creation_time,
            'total_time': db_creation_time + profiling_time + table_creation_time
        }
        
        return results, actual_db_path

    def test_performance_001k_rows(self) -> None:
        """Test performance with 1,000 rows."""
        num_rows = 1000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary(
            test_name="test_performance_001k_rows",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )

    def test_performance_005k_rows(self) -> None:
        """Test performance with 5,000 rows."""
        num_rows = 5000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary(
            test_name="test_performance_005k_rows",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )

    def test_performance_010k_rows(self) -> None:
        """Test performance with 10,000 rows."""
        num_rows = 10000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary(
            test_name="test_performance_010k_rows",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )

    def test_performance_025k_rows(self) -> None:
        """Test performance with 25,000 rows."""
        num_rows = 25000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary(
            test_name="test_performance_025k_rows",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )

    def test_adaptive_sampling_efficiency(self) -> None:
        """Test adaptive sampling performance across different dataset sizes."""
        dataset_sizes = [1000, 10000, 25000]
        
        for num_rows in dataset_sizes:
            results, actual_db_path = self._run_performance_test(num_rows=num_rows)
            
            # Check profiling efficiency (rows per second) - only fail if extremely slow
            profiling_efficiency = num_rows / results['profiling_time']
            min_efficiency = 100  # Very low minimum to only catch real failures
            self.assertGreater(
                profiling_efficiency,
                min_efficiency,
                f"Profiling efficiency {profiling_efficiency:.0f} rows/s is below minimum {min_efficiency}"
            )
            
            self._print_performance_summary(
                test_name=f"adaptive_sampling_{num_rows}",
                results=results,
                num_rows=num_rows,
                db_path=actual_db_path
            )

    def test_operation_breakdown(self) -> None:
        """Test performance breakdown of individual operations."""
        num_rows = 10000
        
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        
        # Check that profiling is the most time-consuming operation
        self.assertGreater(
            results['profiling_time'],
            results['db_creation_time'],
            "Profiling should take longer than database creation"
        )
        
        self.assertGreater(
            results['profiling_time'],
            results['table_creation_time'],
            "Profiling should take longer than table creation"
        )
        
        self._print_performance_summary(
            test_name="operation_breakdown",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )

    def test_repeated_operations(self) -> None:
        """Test consistency of repeated operations."""
        num_rows = 5000
        num_iterations = 2
        
        times = []
        
        for i in range(num_iterations):
            results, actual_db_path = self._run_performance_test(num_rows=num_rows)
            
            # Only measure profiling + table creation time
            core_operations_time = results['profiling_time'] + results['table_creation_time']
            times.append(core_operations_time)
            
            self._print_performance_summary(
                test_name=f"repeated_operation_{i+1}",
                results=results,
                num_rows=num_rows,
                db_path=actual_db_path
            )
        
        # Check consistency between runs - only fail if extremely inconsistent
        if len(times) > 1:
            mean_time = sum(times) / len(times)
            max_deviation = max(abs(t - mean_time) for t in times)
            
            # Only fail if deviation is more than 50% of mean time
            max_allowed_deviation = mean_time * 0.5
            self.assertLess(
                max_deviation,
                max_allowed_deviation,
                f"Maximum deviation {max_deviation:.3f}s exceeds tolerance {max_allowed_deviation:.3f}s. "
                f"Times: {[f'{t:.3f}s' for t in times]}"
            )
            
            print(f"\n{'='*60}")
            print("CONSISTENCY ANALYSIS")
            print(f"{'='*60}")
            print(f"Mean core operations time: {mean_time:.3f}s")
            print(f"Max deviation: {max_deviation:.3f}s")
            print(f"Variance: {max_deviation/mean_time:.2%}")
            print(f"Core operation times: {[f'{t:.3f}s' for t in times]}")
            print(f"{'='*60}")


if __name__ == '__main__':
    unittest.main() 
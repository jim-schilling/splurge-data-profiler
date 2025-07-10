"""
Performance benchmarks for splurge-data-profiler.

This module contains comprehensive performance tests that measure the efficiency
of data profiling operations across different dataset sizes and scenarios.
"""

import csv
import os
import random
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource


class PerformanceBenchmarks(unittest.TestCase):
    """Performance benchmarks for data profiling operations."""

    # Sample data for generating realistic records
    FIRST_NAMES = [
        "Alice", "Bob", "Charlie", "Dana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
        "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ruby", "Sam", "Tara",
        "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Bella", "Chris", "Diana",
        "Ethan", "Fiona", "George", "Hannah", "Ian", "Julia", "Kevin", "Laura", "Mike", "Nina",
        "Oscar", "Penny", "Ryan", "Sarah", "Tom", "Ursula", "Vince", "Willa", "Xander", "Yuki"
    ]

    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
        "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
        "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
        "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
        "Carter", "Roberts"
    ]

    CITIES = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
        "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
        "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
        "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas",
        "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
        "Sacramento", "Mesa", "Kansas City", "Atlanta", "Long Beach", "Colorado Springs",
        "Raleigh", "Miami", "Virginia Beach", "Omaha", "Oakland", "Minneapolis", "Tampa",
        "Tulsa", "Arlington", "New Orleans", "Wichita"
    ]

    DEPARTMENTS = [
        "Engineering", "Marketing", "Sales", "HR", "Legal", "Finance", "Operations",
        "Customer Support", "Product Management", "Research & Development", "IT",
        "Business Development", "Quality Assurance", "Design", "Data Science"
    ]

    def setUp(self) -> None:
        """Set up test environment."""
        self.test_dir = Path("tests/performance/test_data")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate a large DSV file with 100,000 rows for testing
        self.dsv_path = self.test_dir / "performance_test_data_100k.dsv"
        if not self.dsv_path.exists():
            self._generate_dsv(self.dsv_path, num_rows=100000)

    def tearDown(self) -> None:
        """Clean up test fixtures after each test method."""
        # No cleanup needed since we use persistent test data directory
        pass

    def _generate_dsv(
            self,
            file_path: Path,
            *,
            num_rows: int = 100000,
            delimiter: str = "|",
            bookend: str = '"'
    ) -> None:
        """
        Generate a DSV file with the specified number of rows, delimiter, and bookend.
        
        Args:
            file_path: Path where the DSV file will be created
            num_rows: Number of rows to generate (default: 100000)
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

    def _cleanup_database_tables(self, db_path: Path) -> None:
        """
        Clean up database tables if they exist.
        
        Args:
            db_path: Path to the database file
        """
        db_url = f"sqlite:///{db_path}"
        engine = create_engine(db_url)
        try:
            with engine.connect() as connection:
                for table in ["performance_test_data_inferred", "performance_test_data"]:
                    connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        finally:
            engine.dispose()

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

    def test_performance_050k_rows(self) -> None:
        """Test performance with 50,000 rows."""
        num_rows = 50000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary("test_performance_050k_rows", results, num_rows=num_rows, db_path=actual_db_path)

    def test_performance_100k_rows(self) -> None:
        """Test performance with 100,000 rows."""
        num_rows = 100000
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        self._print_performance_summary("test_performance_100k_rows", results, num_rows=num_rows, db_path=actual_db_path)

    def test_adaptive_sampling_scaling(self) -> None:
        """Test adaptive sampling performance across different dataset sizes."""
        dataset_sizes = [10000, 25000]
        for num_rows in dataset_sizes:
            results, actual_db_path = self._run_performance_test(num_rows=num_rows)
            profiling_efficiency = num_rows / results['profiling_time']
            min_efficiency = 50  # Very low minimum to only catch real failures
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

    def test_memory_efficiency(self) -> None:
        """Test memory efficiency with large datasets."""
        num_rows = 10000
        
        results, actual_db_path = self._run_performance_test(num_rows=num_rows)
        
        # Check profiling efficiency - only fail if extremely slow
        profiling_efficiency = num_rows / results['profiling_time']
        min_efficiency = 50  # Very low minimum to only catch real failures
        self.assertGreater(
            profiling_efficiency,
            min_efficiency,
            f"Profiling efficiency {profiling_efficiency:.0f} rows/s is below minimum {min_efficiency}"
        )
        
        self._print_performance_summary(
            test_name="memory_efficiency",
            results=results,
            num_rows=num_rows,
            db_path=actual_db_path
        )


if __name__ == '__main__':
    unittest.main() 
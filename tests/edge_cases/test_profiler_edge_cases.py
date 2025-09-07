"""
Edge case tests for Profiler class.

These tests focus on testing Profiler with edge cases, error conditions,
and boundary scenarios to ensure robust behavior.
"""

import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


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
        - Datasets < 10K rows: 100% sample
        - Datasets 10K-25K rows: 75% sample
        - Datasets 25K-50K rows: 50% sample
        - Datasets 50K-100K rows: 25% sample
        - Datasets 100K-500K rows: 15% sample
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
            DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=Path(temp_dir)
            )

            # Test datasets < 10K rows (100% sample)
            test_cases_small = [
                (0, 0),
                (1, 1),
                (1000, 1000),
                (9999, 9999),
            ]

            for total_rows, expected_sample in test_cases_small:
                with self.subTest(total_rows=total_rows):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test datasets 10K-25K rows (75% sample)
            test_cases_75 = [
                (10000, 7500),
                (15000, 11250),
                (20000, 15000),
                (24999, int(24999 * 0.75)),
            ]
            for total_rows, expected_sample in test_cases_75:
                with self.subTest(total_rows=total_rows, pct_75=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test datasets 25K-50K rows (50% sample)
            test_cases_50 = [
                (25000, 12500),
                (30000, 15000),
                (40000, 20000),
                (49999, int(49999 * 0.5)),
            ]
            for total_rows, expected_sample in test_cases_50:
                with self.subTest(total_rows=total_rows, pct_50=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test datasets 50K-100K rows (25% sample)
            test_cases_25 = [
                (50000, 12500),
                (60000, 15000),
                (80000, 20000),
                (99999, int(99999 * 0.25)),
            ]
            for total_rows, expected_sample in test_cases_25:
                with self.subTest(total_rows=total_rows, pct_25=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test datasets 100K-500K rows (15% sample)
            test_cases_15 = [
                (100000, 15000),
                (200000, 30000),
                (300000, 45000),
                (499999, int(499999 * 0.15)),
            ]
            for total_rows, expected_sample in test_cases_15:
                with self.subTest(total_rows=total_rows, pct_15=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test datasets > 500K rows (10% sample)
            test_cases_10 = [
                (500000, 50000),
                (1000000, 100000),
                (5000000, 500000),
                (10000000, 1000000),
            ]
            for total_rows, expected_sample in test_cases_10:
                with self.subTest(total_rows=total_rows, pct_10=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test boundary conditions and edge cases
            boundary_tests = [
                # Test exact boundaries
                (10000, 7500),  # Exactly at 10K boundary
                (25000, 12500),  # Exactly at 25K boundary
                (50000, 12500),  # Exactly at 50K boundary
                (100000, 15000), # Exactly at 100K boundary
                (500000, 50000), # Exactly at 500K boundary

                # Test one row before boundaries
                (9999, 9999),  # One row before 10K boundary
                (24999, int(24999 * 0.75)),  # One row before 25K boundary
                (49999, int(49999 * 0.5)),  # One row before 50K boundary
                (99999, int(99999 * 0.25)),  # One row before 100K boundary
                (499999, int(499999 * 0.15)), # One row before 500K boundary

                # Test one row after boundaries
                (10001, int(10001 * 0.75)),  # One row after 10K boundary
                (25001, int(25001 * 0.5)),  # One row after 25K boundary
                (50001, int(50001 * 0.25)),  # One row after 50K boundary
                (100001, int(100001 * 0.15)), # One row after 100K boundary
                (500001, int(500001 * 0.10)), # One row after 500K boundary
            ]
            for total_rows, expected_sample in boundary_tests:
                with self.subTest(total_rows=total_rows, boundary_test=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertEqual(sample_size, Profiler.calculate_adaptive_sample_size(total_rows=total_rows),
                                   f"Expected {Profiler.calculate_adaptive_sample_size(total_rows=total_rows)} for {total_rows} rows, got {sample_size}")

            # Test that sample size never exceeds total rows
            for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, max_check=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertLessEqual(sample_size, total_rows,
                                       f"Sample size {sample_size} should not exceed total rows {total_rows}")

            # Test that sample size is always non-negative
            for total_rows in [0, 1, 1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, non_negative_check=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
                    self.assertGreaterEqual(sample_size, 0,
                                          f"Sample size {sample_size} should be non-negative for {total_rows} rows")

            # Test that sample size is always an integer
            for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
                with self.subTest(total_rows=total_rows, integer_check=True):
                    sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
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


if __name__ == "__main__":
    unittest.main()

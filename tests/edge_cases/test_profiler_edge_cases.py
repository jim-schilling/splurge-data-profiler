"""
Edge case tests for Profiler class.

These tests focus on testing Profiler with edge cases, error conditions,
and boundary scenarios to ensure robust behavior.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler


def test_profiler_with_none_data_lake():
    """Test profiler initialization with None data lake."""
    with pytest.raises(ValueError):
        Profiler(data_lake=None)


def test_profiler_reprofile_same_data():
    """Test that reprofiling the same data produces consistent results."""
    # Create a simple test setup
    temp_dir = tempfile.mkdtemp()
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")

    try:
        # Create simple test data
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name,value\n1,Alice,10.5\n2,Bob,20.0\n")

        dsv_source = DsvSource(temp_path)
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


def test_profiler_large_sample_size():
    """Test profiler with sample size larger than available data."""
    temp_dir = tempfile.mkdtemp()
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")

    try:
        # Create small test data (only 5 rows)
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Diana\n5,Eve\n")

        dsv_source = DsvSource(temp_path)
        data_lake = DataLakeFactory.from_dsv_source(dsv_source=dsv_source, data_lake_path=Path(temp_dir))

        profiler = Profiler(data_lake=data_lake)

        # Try to profile with sample size larger than available data
        profiler.profile(sample_size=100)  # More than 5 rows

        # Should still work and profile all available data
        assert len(profiler.profiled_columns) > 0

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


def test_calculate_adaptive_sample_size():
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
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")

        dsv_source = DsvSource(temp_path)
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
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test datasets 5K-10K rows (80% sample)
        test_cases_80 = [
            (5000, 4000),
            (7500, 6000),
            (9999, 7999),
        ]
        for total_rows, expected_sample in test_cases_80:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test datasets 10K-25K rows (60% sample)
        test_cases_60 = [
            (10000, 6000),
            (15000, 9000),
            (20000, 12000),
            (24999, 14999),
        ]
        for total_rows, expected_sample in test_cases_60:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test datasets 25K-100K rows (40% sample)
        test_cases_40 = [
            (25000, 10000),
            (50000, 20000),
            (75000, 30000),
            (99999, 39999),
        ]
        for total_rows, expected_sample in test_cases_40:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test datasets 100K-500K rows (20% sample)
        test_cases_20 = [
            (100000, 20000),
            (200000, 40000),
            (300000, 60000),
            (499999, 99999),
        ]
        # Exercise and assert
        for total_rows, expected_sample in test_cases_20:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

    # Test datasets > 500K rows (10% sample)
        test_cases_10 = [
            (500000, 50000),
            (1000000, 100000),
            (5000000, 500000),
            (10000000, 1000000),
        ]
        for total_rows, expected_sample in test_cases_10:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test boundary conditions and edge cases (aligned with Profiler._SAMPLE_RULES)
        boundary_tests = [
            # Test exact boundaries (equality uses next rule's factor)
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
            assert sample_size == expected_sample, (
                f"Expected {expected_sample} for {total_rows} rows, got {sample_size}"
            )

        # Test that sample size never exceeds total rows
        for total_rows in [1000, 25000, 50000, 100000, 500000, 1000000]:
            sample_size = Profiler.calculate_adaptive_sample_size(total_rows=total_rows)
            assert sample_size <= total_rows, f"Sample size {sample_size} should not exceed total rows {total_rows}"
            assert sample_size >= 0, f"Sample size {sample_size} should be non-negative for {total_rows} rows"
            assert isinstance(sample_size, int), f"Sample size {sample_size} should be an integer for {total_rows} rows"

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


def test_profiler_properties():
    """Test profiler properties."""
    temp_dir = tempfile.mkdtemp()
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")

    try:
        # Create test data
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")

        dsv_source = DsvSource(temp_path)
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

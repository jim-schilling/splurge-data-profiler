# Performance Tests

This directory contains performance tests for the splurge-data-profiler package.

## Test Files

### `test_basic_performance.py`
Basic performance tests that don't require external dependencies. Tests:
- Performance with different dataset sizes (1K, 5K, 10K, 25K rows)
- Adaptive sampling efficiency (see below for current rules)
- Operation breakdown timing
- Repeated operation consistency

### `test_performance_benchmarks.py`
Comprehensive performance benchmarks with advanced features. Tests:
- Performance with larger datasets (10K, 25K, 50K, 100K, 250K, 500K rows)
- Adaptive sampling scaling analysis (see below for current rules)
- Memory efficiency (indirect testing)
- Concurrent processing capabilities

## Adaptive Sampling (Current Implementation)

The profiler uses adaptive sampling to determine how many rows to sample for profiling, based on the total number of rows in the dataset. The rules are:

| Total Rows         | Sample Fraction |
|--------------------|----------------|
| < 5,000            | 100%           |
| < 10,000           | 80%            |
| < 25,000           | 60%            |
| < 100,000          | 40%            |
| < 500,000          | 20%            |
| >= 500,000         | 10%            |

- The sample size is calculated as `int(total_rows * fraction)` for the first rule that matches.
- If a specific sample size is provided, it overrides adaptive sampling.
- Sampling is performed using random row selection (ORDER BY RANDOM() for SQLite, ORDER BY RAND() for others).

## Running Performance Tests

### Run all performance tests:
```bash
python tests/run_tests.py performance
```

### Run specific performance test file:
```bash
python -m pytest tests/performance/test_basic_performance.py
python -m pytest tests/performance/test_performance_benchmarks.py
```

### Run with verbose output:
```bash
python -m pytest tests/performance/ -v
```

## Test Data

Tests generate realistic test data including:
- Employee records with names, emails, ages
- Salary and department information
- Dates and timestamps
- Boolean values
- Various data types for comprehensive profiling

## What's Tested

1. **Database Creation**: Time to create SQLite database from DSV files
2. **Data Profiling**: Time to profile data types using adaptive sampling
3. **Inferred Table Creation**: Time to create tables with cast columns
4. **Data Integrity**: Verification that all rows are processed correctly
5. **Scaling Efficiency**: Ensuring performance scales sub-linearly with dataset size
6. **Consistency**: Repeated operations maintain similar performance

## Notes

- Tests use temporary directories that are automatically cleaned up
- Performance thresholds may need adjustment based on system capabilities
- The comprehensive benchmarks include larger datasets and may take longer to run
- Memory efficiency is tested indirectly through performance timing
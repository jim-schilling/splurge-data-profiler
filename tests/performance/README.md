# Performance Tests

This directory contains performance tests for the splurge-data-profiler package.

## Test Files

### `test_basic_performance.py`
Basic performance tests that don't require external dependencies. Tests:
- Performance with different dataset sizes (1K, 5K, 10K, 25K rows)
- Adaptive sampling efficiency
- Operation breakdown timing
- Repeated operation consistency

### `test_performance_benchmarks.py`
Comprehensive performance benchmarks with advanced features. Tests:
- Performance with larger datasets (10K, 25K, 50K, 100K, 250K, 500K rows)
- Adaptive sampling scaling analysis
- Memory efficiency (indirect testing)
- Concurrent processing capabilities

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

## Performance Thresholds

The tests use conservative performance thresholds based on typical system capabilities:

### Basic Performance Tests
- 1K rows: < 3 seconds
- 5K rows: < 8 seconds  
- 10K rows: < 15 seconds
- 25K rows: < 35 seconds

### Comprehensive Benchmarks
- 10K rows: < 10 seconds
- 25K rows: < 20 seconds
- 50K rows: < 40 seconds
- 100K rows: < 80 seconds
- 250K rows: < 200 seconds
- 500K rows: < 400 seconds

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
- Concurrent processing tests verify thread safety 
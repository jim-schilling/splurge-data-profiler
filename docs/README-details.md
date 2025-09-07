# Splurge Data Profiler - Detailed Documentation

This document provides comprehensive information about the Splurge Data Profiler, including detailed usage examples, configuration options, API documentation, and technical specifications.

## Table of Contents

- [Features](#features)
- [CLI Usage](#cli-usage)
- [Configuration](#configuration)
- [Examples](#examples)
- [Data Types](#data-types)
- [Adaptive Sampling](#adaptive-sampling)
- [Programmatic Usage](#programmatic-usage)
- [Testing and Quality Assurance](#testing-and-quality-assurance)
- [Requirements](#requirements)
- [Error Handling](#error-handling)
- [Dependencies](#dependencies)

## Features

### Core Features
- **DSV File Support**: Profile CSV, TSV, and other delimiter-separated value files
- **Automatic Type Inference**: Intelligently detect data types using adaptive sampling
- **Data Lake Creation**: Generate SQLite databases with optimized schemas
- **Inferred Tables**: Create tables with both original and type-cast columns
- **Flexible Configuration**: JSON-based configuration for customization
- **Command Line Interface**: Easy-to-use CLI for batch processing

### Technical Features
- **Adaptive Sampling**: Automatically adjusts sample size based on dataset size
- **Type Safety**: Strong type inference with fallback to TEXT type
- **Performance Optimized**: Efficient processing for large datasets
- **Memory Efficient**: Streaming processing for large files
- **Cross-Platform**: Works on Windows, macOS, and Linux

## CLI Usage

### Profile Command

Profile a DSV file and create a data lake:

```bash
python -m splurge_data_profiler profile <dsv_file> <config_file> [options]
```

**Options:**
- `--verbose`: Enable verbose output

**Examples:**
```bash
# Basic profiling
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json

# Verbose output
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json --verbose
```

### Create Config Command

Generate a sample configuration file:

```bash
python -m splurge_data_profiler create-config <output_file>
```

**Example:**
```bash
python -m splurge_data_profiler create-config examples/example_config.json
```

## Configuration

### Configuration File Structure

The configuration file is a JSON file that specifies how to process your DSV file:

```json
{
  "data_lake_path": "./data_lake",
  "dsv": {
    "delimiter": ",",
    "strip": true,
    "bookend": "\"",
    "bookend_strip": true,
    "encoding": "utf-8",
    "skip_header_rows": 0,
    "skip_footer_rows": 0,
    "header_rows": 1,
    "skip_empty_rows": true
  }
}
```

### Configuration Options

#### Required Fields
- `data_lake_path`: Directory where the SQLite database will be created

#### DSV Configuration (`dsv` object)
- `delimiter`: Character used to separate values (default: `","`)
- `strip`: Whether to strip whitespace from values (default: `true`)
- `bookend`: Character used to quote values (default: `"\"`)
- `bookend_strip`: Whether to strip bookend characters (default: `true`)
- `encoding`: File encoding (default: `"utf-8"`)
- `skip_header_rows`: Number of header rows to skip (default: `0`)
- `skip_footer_rows`: Number of footer rows to skip (default: `0`)
- `header_rows`: Number of header rows (default: `1`)
- `skip_empty_rows`: Whether to skip empty rows (default: `true`)

**Note**: The profiler always uses adaptive sampling and always creates an inferred table.

## Examples

### Example 1: Basic Profiling

1. Create a configuration:
```bash
python -m splurge_data_profiler create-config examples/example_config.json
```

2. Profile your data:
```bash
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json
```

Output:
```
=== PROFILING RESULTS ===
id: INTEGER
name: TEXT
age: INTEGER
salary: FLOAT
is_active: BOOLEAN
hire_date: DATE
last_login: DATETIME

Profiling completed successfully!
```

**Note**: Datetime values should be in ISO 8601 format (YYYY-MM-DDTHH:MM:SS) for proper type inference.

### Example 2: With Inferred Table

```bash
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json
```

This creates an additional table with:
- Original columns (preserving text values)
- Cast columns with inferred data types

### Example 3: Verbose Output

```bash
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json --verbose
```

## Data Types

The profiler can infer the following data types:

- **TEXT**: String values
- **INTEGER**: Whole numbers
- **FLOAT**: Decimal numbers
- **BOOLEAN**: True/false values (case-insensitive: true/false, yes/no, 1/0)
- **DATE**: Date values (YYYY-MM-DD)
- **TIME**: Time values (HH:MM:SS)
- **DATETIME**: Date and time values (ISO 8601 format: YYYY-MM-DDTHH:MM:SS)

### Type Inference Rules

1. **Empty/null values**: Ignored during type inference
2. **Mixed types**: Falls back to TEXT type
3. **Invalid formats**: Falls back to TEXT type
4. **Boolean detection**: Recognizes common boolean representations
5. **Date/time parsing**: Strict ISO 8601 format required for date/time types

## Adaptive Sampling

When no sample size is specified, the profiler uses adaptive sampling based on dataset size:

- **< 5K rows**: 100% sampling (all rows)
- **< 10K rows**: 80% sampling
- **< 25K rows**: 60% sampling
- **< 100K rows**: 40% sampling
- **< 500K rows**: 20% sampling
- **>= 500K rows**: 10% sampling

### Sampling Algorithm

The adaptive sampling uses a stratified sampling approach to ensure representative data across the entire dataset. The sampling is deterministic and reproducible for the same input file.

## Programmatic Usage

You can also use the profiler programmatically:

```python
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource

# Create DSV source
dsv_source = DsvSource(
    file_path="examples/example_data.csv",
    delimiter=",",
    encoding="utf-8"
)

# Create data lake
data_lake = DataLakeFactory.from_dsv_source(
    dsv_source=dsv_source,
    data_lake_path="./data_lake"
)

# Create profiler and run profiling
profiler = Profiler(data_lake=data_lake)
profiler.profile()

# Get results
for column in profiler.profiled_columns:
    print(f"{column.name}: {column.inferred_type}")
```

### Advanced Programmatic Usage

```python
from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource

# Create DSV source with custom configuration
dsv_source = DsvSource(
    file_path="data.csv",
    delimiter="|",
    encoding="utf-8",
    skip_header_rows=1,
    header_rows=1
)

# Create data lake
data_lake = DataLakeFactory.from_dsv_source(
    dsv_source=dsv_source,
    data_lake_path="./my_data_lake"
)

# Create profiler with custom sample size
profiler = Profiler(data_lake=data_lake)
profiler.profile(sample_size=1000)  # Custom sample size

# Access profiling results
print("Profiling Results:")
for column in profiler.profiled_columns:
    print(f"  {column.name}: {column.inferred_type}")

# Get inferred table name
inferred_table = profiler.create_inferred_table()
print(f"Inferred table created: {inferred_table}")
```

## Testing and Quality Assurance

The Splurge Data Profiler maintains enterprise-grade quality through a comprehensive testing framework organized into multiple test categories:

### Test Categories

#### Unit Tests (`tests/unit/`)
- **Component Isolation**: Test individual classes and methods in isolation
- **Core Functionality**: Validate core business logic and algorithms
- **Type Safety**: Ensure proper type handling and validation
- **Error Conditions**: Test error handling at the component level

#### Integration Tests (`tests/integration/`)
- **Component Interaction**: Test how components work together
- **Database Operations**: Validate real database interactions
- **File System Operations**: Test file I/O and path handling
- **Streaming Processing**: Validate large file processing

#### Edge Case Tests (`tests/edge_cases/`)
- **Boundary Conditions**: Test limits and edge values
- **Error Scenarios**: Validate error handling for unusual inputs
- **Resource Management**: Test memory and connection handling
- **Malformed Data**: Handle corrupted or invalid input files

#### End-to-End Tests (`tests/e2e/`)
- **Complete Workflows**: Test full user scenarios from start to finish
- **CLI Integration**: Validate command-line interface functionality
- **Performance Validation**: Test with realistic data volumes
- **Cross-Platform**: Ensure consistent behavior across environments

### Running Tests

#### Run All Tests
```bash
pytest
```

#### Run Specific Test Categories
```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Edge case tests only
pytest tests/edge_cases/

# E2E tests only
pytest tests/e2e/
```

#### Run with Coverage
```bash
pytest --cov=splurge_data_profiler --cov-report=html
```

#### Run Performance Tests
```bash
pytest tests/e2e/test_profiler_comprehensive_e2e.py::TestProfilerComprehensive::test_profiler_large_dataset_performance -v
```

### Test Coverage Areas

#### Core Components
- **Column Class**: Type inference, validation, and representation
- **DataType Enum**: Type definitions and conversions
- **DbSource Class**: Database connection and schema inspection
- **DsvSource Class**: File parsing and column detection

#### Data Processing
- **Type Inference**: Automatic data type detection algorithms
- **Adaptive Sampling**: Dynamic sample size calculation
- **Data Lake Creation**: SQLite database generation and optimization
- **Streaming Processing**: Memory-efficient large file handling

#### Error Handling
- **File Processing Errors**: Invalid files, encoding issues, permissions
- **Database Errors**: Connection failures, schema mismatches, constraints
- **Configuration Errors**: Invalid settings, missing required fields
- **Resource Errors**: Memory limits, disk space, connection timeouts

#### Performance Validation
- **Large Datasets**: Files with millions of rows
- **High Cardinality**: Columns with many unique values
- **Complex Schemas**: Tables with many columns
- **Concurrent Access**: Multiple simultaneous operations

### Quality Metrics

The project maintains the following quality standards:

- **Test Coverage**: >85% code coverage across all components
- **Performance Benchmarks**: Established performance baselines
- **Error Recovery**: Comprehensive error handling and recovery
- **Documentation**: Tests validate documentation accuracy
- **Cross-Platform**: Consistent behavior on Windows, macOS, Linux

### Contributing to Testing

When adding new features:

1. **Add Unit Tests**: Create unit tests for new components
2. **Add Integration Tests**: Validate component interactions
3. **Test Edge Cases**: Consider boundary conditions and error scenarios
4. **Update E2E Tests**: Ensure complete workflows still function
5. **Performance Testing**: Validate performance impact of changes

## Requirements

### System Requirements
- **Python**: 3.10 or higher
- **Operating System**: Windows, macOS, or Linux
- **Memory**: Minimum 512MB RAM (more recommended for large datasets)
- **Disk Space**: Sufficient space for SQLite databases (typically 2-3x input file size)

### Python Dependencies
- **SQLAlchemy**: >= 2.0.37 (database operations)
- **splurge-dsv**: >= 2025.1.5 (DSV file parsing)
- **splurge-typer**: >= 2025.0.1 (CLI framework)
- **splurge-tabular**: >= 2025.0.0 (tabular data processing)

## Error Handling

### Common Errors

#### File Not Found
```
Error: DSV file not found: examples/data.csv
```
**Solution**: Verify the file path and ensure the file exists.

#### Invalid Configuration
```
Error: Missing required configuration keys: ['data_lake_path']
```
**Solution**: Ensure the configuration file contains all required fields.

#### Encoding Issues
```
Error: Unable to decode file with encoding 'utf-8'
```
**Solution**: Specify the correct encoding in the configuration file.

#### Permission Errors
```
Error: Permission denied when creating data lake
```
**Solution**: Ensure write permissions for the data lake directory.

### Error Codes

- **Exit Code 0**: Success
- **Exit Code 1**: Generic error
- **Exit Code 2**: Invalid arguments
- **Exit Code 130**: Interrupted (Ctrl+C)

## Dependencies

### Core Dependencies

#### SQLAlchemy >= 2.0.37
- Database abstraction layer
- SQLite database operations
- Connection pooling and transaction management

#### splurge-dsv >= 2025.1.5
- High-performance DSV file parsing
- Support for various delimiters and encodings
- Memory-efficient streaming processing

#### splurge-typer >= 2025.0.1
- Modern CLI framework
- Type-safe command line interfaces
- Automatic help generation

#### splurge-tabular >= 2025.0.0
- Tabular data processing utilities
- Data type inference algorithms
- Schema generation and validation

### Development Dependencies

#### pytest >= 7.0.0
- Unit testing framework
- Test discovery and execution
- Assertion and fixture support

#### pytest-cov >= 4.0.0
- Code coverage reporting
- Coverage analysis and reporting

#### pytest-xdist >= 3.8.0
- Parallel test execution
- Distributed testing support

#### ruff >= 0.12.12
- Fast Python linter
- Code formatting and style checking

#### mypy >= 1.0.0
- Static type checking
- Type annotation validation

## API Reference

### Classes

#### `DsvSource`
Main class for DSV file processing.

**Methods:**
- `__init__(file_path, delimiter, encoding, **kwargs)`: Initialize DSV source
- `columns`: Property returning list of column definitions
- `row_count`: Property returning total number of rows

#### `DataLakeFactory`
Factory class for creating data lakes.

**Methods:**
- `from_dsv_source(dsv_source, data_lake_path)`: Create data lake from DSV source

#### `Profiler`
Main profiling class.

**Methods:**
- `__init__(data_lake)`: Initialize profiler
- `profile(sample_size=None)`: Run profiling with optional sample size
- `create_inferred_table()`: Create inferred table with type-cast columns

### Exceptions

#### `FileNotFoundError`
Raised when input file is not found.

#### `ValueError`
Raised for invalid configuration or data format issues.

#### `RuntimeError`
Raised for processing errors during profiling.

## Performance Considerations

### Memory Usage
- Streaming processing for large files
- Configurable sample sizes for memory control
- Efficient data type inference algorithms

### Processing Speed
- Optimized SQLite operations
- Parallel processing where applicable
- Adaptive sampling for faster processing of large datasets

### Scalability
- Handles files up to millions of rows
- Efficient for datasets with hundreds of columns
- Optimized for both small and large data lakes

## Troubleshooting

### Common Issues

#### Slow Processing
- Reduce sample size for large datasets
- Check available memory
- Verify file encoding

#### Type Inference Issues
- Ensure consistent data formats
- Check for mixed data types in columns
- Verify datetime formats (ISO 8601 required)

#### Database Errors
- Ensure write permissions for data lake directory
- Check available disk space
- Verify SQLite compatibility

### Getting Help

For additional support:
- Check the [GitHub Issues](https://github.com/jim-schilling/splurge-data-profiler/issues)
- Review the [Documentation](https://github.com/jim-schilling/splurge-data-profiler#readme)
- Consult the [Changelog](https://github.com/jim-schilling/splurge-data-profiler/blob/main/CHANGELOG.md)
# Splurge Data Profiler

A powerful data profiling tool for delimited and database sources that automatically infers data types and creates optimized data lakes (SQLite database).

## Features

- **DSV File Support**: Profile CSV, TSV, and other delimiter-separated value files
- **Automatic Type Inference**: Intelligently detect data types using adaptive sampling
- **Data Lake Creation**: Generate SQLite databases with optimized schemas
- **Inferred Tables**: Create tables with both original and type-cast columns
- **Flexible Configuration**: JSON-based configuration for customization
- **Command Line Interface**: Easy-to-use CLI for batch processing

## Installation

```bash
pip install splurge-data-profiler
```

## Quick Start

1. **Create a configuration file**:
```bash
python -m splurge_data_profiler create-config examples/example_config.json
```

2. **Profile your data**:
```bash
python -m splurge_data_profiler profile examples/example_data.csv examples/example_config.json
```

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

## Configuration File

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
- `bookend`: Character used to quote values (default: `"\""`)
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
- **BOOLEAN**: True/false values
- **DATE**: Date values (YYYY-MM-DD)
- **TIME**: Time values (HH:MM:SS)
- **DATETIME**: Date and time values (ISO 8601 format: YYYY-MM-DDTHH:MM:SS)

## Adaptive Sampling

When no sample size is specified, the profiler uses adaptive sampling:

- Datasets < 25K rows: 100% sample
- Datasets 25K-50K rows: 50% sample
- Datasets 50K-100K rows: 25% sample
- Datasets 100K-500K rows: 20% sample
- Datasets > 500K rows: 10% sample

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

## Requirements

- Python 3.10+
- SQLAlchemy >= 2.0.37
- splurge-tools == 0.2.4

## License

MIT License


## Changelog

### [0.1.0] 2025-07-06

- **Initial release** of Splurge Data Profiler
- **CLI implementation** with `profile` and `create-config` commands
- **DSV file support** for CSV, TSV, and other delimiter-separated value files
- **Automatic type inference** using adaptive sampling strategy
- **Data lake creation** with SQLite database generation
- **Inferred table creation** with both original and type-cast columns
- **JSON configuration** for DSV parsing options
- **ISO 8601 datetime support** for proper type inference
- **Adaptive sampling** based on dataset size (100% for <25K rows, 50% for 25K-50K, 25% for 50K-100K, 20% for 100K-500K, 10% for >500K)
- **Simplified workflow** - always profiles and always creates inferred tables

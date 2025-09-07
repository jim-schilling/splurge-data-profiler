# Splurge Data Profiler

[![Version](https://img.shields.io/badge/version-2025.2.0-blue.svg)](https://github.com/jim-schilling/splurge-data-profiler/releases)
[![Python Versions](https://img.shields.io/pypi/pyversions/splurge-data-profiler.svg)](https://pypi.org/project/splurge-data-profiler/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-detailed-blue.svg)](docs/README-details.md)
[![Coverage](https://img.shields.io/badge/coverage-94%25-brightgreen.svg)](https://github.com/jim-schilling/splurge-data-profiler)

A powerful data profiling tool for delimited and database sources that automatically infers data types and creates optimized data lakes (SQLite database).

## Features

- **DSV File Support**: Profile CSV, TSV, and other delimiter-separated value files
- **Automatic Type Inference**: Intelligently detect data types using adaptive sampling
- **Data Lake Creation**: Generate SQLite databases with optimized schemas
- **Inferred Tables**: Create tables with both original and type-cast columns
- **Flexible Configuration**: JSON-based configuration for customization
- **Command Line Interface**: Easy-to-use CLI for batch processing
- **Comprehensive Testing**: Extensive test coverage ensuring reliability and robustness
- **Production Ready**: Enterprise-grade error handling and performance optimization

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

### Create Config Command

Generate a sample configuration file:

```bash
python -m splurge_data_profiler create-config <output_file>
```

## Configuration

The configuration file is a JSON file that specifies how to process your DSV file:

```json
{
  "data_lake_path": "./data_lake",
  "dsv": {
    "delimiter": ",",
    "encoding": "utf-8"
  }
}
```

## Documentation

For detailed documentation, examples, and API reference, see:
- [Detailed Documentation](docs/README-details.md)
- [Changelog](CHANGELOG.md)

## Requirements

- Python 3.10+
- SQLAlchemy >= 2.0.37

## Quality Assurance

This project maintains high code quality through comprehensive testing:

- **Unit Tests**: Core component testing with 100% coverage of critical paths
- **Integration Tests**: End-to-end workflow validation
- **Edge Case Tests**: Error handling and boundary condition testing
- **E2E Tests**: Complete user scenario validation
- **Performance Tests**: Large dataset processing validation

Run tests with:
```bash
pytest
```

## License

MIT License
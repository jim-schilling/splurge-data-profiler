# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2025.2.0] - 2025-09-07

### Added
- **Comprehensive Test Suite**: Added extensive test coverage across all components
  - Unit tests for core classes (`Column`, `DataType`, `DbSource`, `DsvSource`)
  - Integration tests for end-to-end workflows and database operations
  - Edge case tests for error handling and boundary conditions
  - E2E tests for comprehensive profiling scenarios
  - CLI integration tests for command-line functionality
- **Test Organization**: Structured test suite with clear categorization:
  - `tests/unit/` - Unit tests for individual components
  - `tests/integration/` - Integration tests for component interactions
  - `tests/edge_cases/` - Tests for error conditions and edge cases
  - `tests/e2e/` - End-to-end tests for complete workflows
- **Quality Assurance**: Enhanced reliability through comprehensive testing
  - Error handling validation for malformed data and edge cases
  - Resource management testing for database connections
  - Performance testing for large datasets
  - Type casting validation for all supported data types
- **Documentation Testing**: Added tests to validate documentation structure and links

### Changed
- **Improved Code Reliability**: Enhanced error handling and edge case management
- **Better Resource Management**: Improved database connection handling and cleanup
- **Enhanced Type Safety**: Strengthened type annotations and validation

### Fixed
- **Memory Management**: Fixed potential memory leaks in streaming operations
- **Error Recovery**: Improved error recovery for malformed input files
- **Database Connection Handling**: Enhanced connection pooling and disposal

### Verified
- Test suite: 211 tests passed (run with pytest -n auto).
- Coverage: 92% overall (coverage report written to `htmlcov/` and `coverage.xml`).

Recent maintenance and test hygiene changes:

- Replaced stdlib `tempfile` usage in tests with pytest-managed fixtures (`tmp_path` and `tmp_path_factory`).
- Removed repository-level autouse fixture that monkeypatched `tempfile` and made tempfile handling explicit in tests.
- Fixed mypy issues by adding TYPE_CHECKING guards and performing runtime import checks for third-party helper packages.
- Replaced silent runtime `Any` fallbacks for third-party imports with fail-fast runtime imports so missing dependencies raise an ImportError instead of causing subtle runtime errors during profiling.
- Fixed several linter (ruff) issues and ensured the project lints cleanly.

## [0.1.1] - 2025-07-10

### Changed
- **Refactored adaptive sampling logic**: Sampling thresholds and factors are now defined as class-level rules using a dataclass, improving maintainability and clarity.
- **Public classmethod for adaptive sample size**: `calculate_adaptive_sample_size` is now a public classmethod, replacing the previous private method and magic numbers.
- **Test suite updated**: All tests now use the new classmethod for adaptive sample size, ensuring consistency and eliminating magic numbers.
- **Sampling rules updated**: New adaptive sampling rules:
  - < 5K rows: 100%
  - < 10K rows: 80%
  - < 25K rows: 60%
  - < 100K rows: 40%
  - < 500K rows: 20%
  - >= 500K rows: 10%
- **General code quality improvements**: Improved type annotations, error handling, and code organization per project standards.
- **Enhanced test coverage and reliability**: Test logic and assertions now reflect the updated adaptive sampling strategy.

## [0.1.0] - 2025-07-06

### Added
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
#!/usr/bin/env python3
"""
Test runner script for the splurge-data-profiler package.

This script provides convenient commands to run different types of tests
during the transition to the new test organization structure.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> int:
    """Run a command and return the exit code."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTest run interrupted by user.")
        return 1
    except Exception as e:
        print(f"Error running command: {e}")
        return 1


def main() -> int:
    """Main function to run tests based on command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run tests for splurge-data-profiler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py all                    # Run all tests
  python run_tests.py unit                   # Run only unit tests
  python run_tests.py integration            # Run only integration tests
  python run_tests.py edge                   # Run only edge case tests
  python run_tests.py performance            # Run only performance tests
  python run_tests.py core                   # Run core test files
  python run_tests.py source                 # Run all source-related tests
  python run_tests.py coverage               # Run all tests except performance benchmarks
  python run_tests.py --coverage unit        # Run unit tests with coverage
  python run_tests.py --verbose integration  # Run integration tests with verbose output
  python run_tests.py --fail-fast performance # Run performance tests, stop on first failure
  python run_tests.py -x -v all              # Run all tests with fail-fast and verbose output
        """
    )
    
    parser.add_argument(
        'test_type',
        choices=['all', 'unit', 'integration', 'edge', 'performance', 'core', 'source', 'coverage'],
        help='Type of tests to run'
    )
    
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage reporting'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Run tests with verbose output'
    )
    
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Run only fast tests (unit tests)'
    )
    
    parser.add_argument(
        '--fail-fast', '-x',
        action='store_true',
        help='Stop on first failure (pytest -x option)'
    )
    
    args = parser.parse_args()
    
    # Base pytest command
    pytest_cmd = [sys.executable, '-m', 'pytest']
    
    # Add coverage if requested
    if args.coverage:
        pytest_cmd.extend(['--cov=splurge_data_profiler', '--cov-report=term-missing'])
    
    # Add verbose output if requested
    if args.verbose:
        pytest_cmd.append('-v')
    
    # Add fast mode (short traceback)
    if args.fast:
        pytest_cmd.append('--tb=short')
    
    # Add fail-fast option
    if args.fail_fast:
        pytest_cmd.append('-x')
    
    # Automatically enable fail-fast for performance tests
    if args.test_type == 'performance' and not args.fail_fast:
        pytest_cmd.append('-x')
        print("Note: Fail-fast mode automatically enabled for performance tests")
    
    # Automatically enable output capture disable (-s) for performance tests
    if args.test_type == 'performance':
        pytest_cmd.append('-s')
        print("Note: Output capture disabled (-s) for performance tests to show summaries")
    
    # Determine test paths based on test type
    if args.test_type == 'all':
        # Run both new and legacy tests
        test_paths = ['tests']
        description = "All tests (new structure + legacy)"
        
    elif args.test_type == 'unit':
        test_paths = ['tests/unit/']
        description = "Unit tests only"
        
    elif args.test_type == 'integration':
        test_paths = ['tests/integration/']
        description = "Integration tests only"
        
    elif args.test_type == 'edge':
        test_paths = ['tests/edge_cases/']
        description = "Edge case tests only"
        
    elif args.test_type == 'performance':
        test_paths = ['tests/performance/']
        description = "Performance tests only"
        
    elif args.test_type == 'core':
        # Run core test files
        test_paths = [
            'tests/test_source.py',
            'tests/test_data_lake.py', 
            'tests/test_profiler.py',
            'tests/test_cli.py'
        ]
        description = "Core test files only"
        
    elif args.test_type == 'source':
        # Run all source-related tests
        test_paths = [
            'tests/unit/test_source_unit.py',
            'tests/integration/test_source_integration.py',
            'tests/edge_cases/test_source_edge.py'
        ]
        description = "All source-related tests"
        
    elif args.test_type == 'coverage':
        # Run all tests except for test_performance_benchmarks.py
        test_paths = [
            'tests/unit/',
            'tests/integration/',
            'tests/edge_cases/',
            'tests/test_source.py',
            'tests/test_data_lake.py',
            'tests/test_profiler.py',
            'tests/test_cli.py',
            'tests/performance/test_basic_performance.py'
        ]
        description = "Coverage tests (all except performance benchmarks)"
        
    else:
        print(f"Unknown test type: {args.test_type}")
        return 1
    
    # Check if test paths exist
    existing_paths = []
    for path in test_paths:
        if Path(path).exists():
            existing_paths.append(path)
        else:
            print(f"Warning: Test path '{path}' does not exist")
    
    if not existing_paths:
        print("No test paths found to run!")
        return 1
    
    # Run the tests
    cmd = pytest_cmd + existing_paths
    return run_command(cmd, description)


if __name__ == '__main__':
    sys.exit(main()) 
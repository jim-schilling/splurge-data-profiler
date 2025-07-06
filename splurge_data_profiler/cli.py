#!/usr/bin/env python3
"""
Command-line interface for the Splurge Data Profiler.

This module provides a CLI for profiling DSV files and creating data lakes
with inferred data types.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from splurge_data_profiler.data_lake import DataLakeFactory
from splurge_data_profiler.profiler import Profiler
from splurge_data_profiler.source import DsvSource, DataType


def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_path: Path to the configuration JSON file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
        ValueError: If required configuration is missing
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError(f"Invalid JSON in config file: {exc}", exc.doc, exc.pos) from exc
    
    # Validate required configuration (only data_lake_path is required now)
    required_keys = ['data_lake_path']
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    return config


def create_dsv_source_from_config(
        dsv_path: Path,
        config: Dict[str, Any]
) -> DsvSource:
    """
    Create a DsvSource from configuration.
    
    Args:
        dsv_path: Path to the DSV file
        config: Configuration dictionary
        
    Returns:
        DsvSource instance
    """
    # Extract DSV-specific configuration with defaults
    dsv_config = config.get('dsv', {})
    
    return DsvSource(
        file_path=dsv_path,
        delimiter=dsv_config.get('delimiter', ','),
        strip=dsv_config.get('strip', True),
        bookend=dsv_config.get('bookend', '"'),
        bookend_strip=dsv_config.get('bookend_strip', True),
        encoding=dsv_config.get('encoding', 'utf-8'),
        skip_header_rows=dsv_config.get('skip_header_rows', 0),
        skip_footer_rows=dsv_config.get('skip_footer_rows', 0),
        header_rows=dsv_config.get('header_rows', 1),
        skip_empty_rows=dsv_config.get('skip_empty_rows', True)
    )


def run_profiling(
        dsv_path: Path,
        config_path: Path,
        *,
        verbose: bool = False
) -> None:
    """
    Run the complete profiling workflow. Always profiles with adaptive sampling and recreates the inferred table.
    
    Args:
        dsv_path: Path to the DSV file
        config_path: Path to the configuration file
        verbose: Whether to print verbose output
        
    Raises:
        RuntimeError: If profiling fails
    """
    try:
        if verbose:
            print(f"Loading configuration from: {config_path}")
        
        # Load configuration
        config = load_config(config_path)
        
        if verbose:
            print(f"Creating DSV source from: {dsv_path}")
        
        # Create DSV source
        dsv_source = create_dsv_source_from_config(dsv_path, config)
        
        if verbose:
            print(f"DSV columns: {[col.name for col in dsv_source.columns]}")
        
        # Create data lake
        data_lake_path = Path(config['data_lake_path'])
        data_lake_path.mkdir(parents=True, exist_ok=True)
        
        if verbose:
            print(f"Creating data lake at: {data_lake_path}")
        
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=data_lake_path
        )
        
        if verbose:
            print(f"Data lake created successfully")
            print(f"Database: {data_lake.db_url}")
            print(f"Table: {data_lake.db_table}")
        
        # Create profiler
        profiler = Profiler(data_lake=data_lake)
        
        if verbose:
            print("Starting data profiling with adaptive sampling...")
        
        # Run profiling with adaptive sampling (sample_size=None)
        profiler.profile(sample_size=None)
        
        # Display results
        print("\n=== PROFILING RESULTS ===")
        for column in profiler.profiled_columns:
            print(f"{column.name}: {column.inferred_type.value}")
        
        # Always create inferred table
        if verbose:
            print("\nRecreating inferred table...")
        inferred_table_name = profiler.create_inferred_table()
        print(f"\nInferred table created: {inferred_table_name}")
        
        print("\nProfiling completed successfully!")
        
    except Exception as exc:
        print(f"Error during profiling: {exc}", file=sys.stderr)
        sys.exit(1)


def create_sample_config(output_path: Path) -> None:
    """
    Create a sample configuration file.
    
    Args:
        output_path: Path where to save the sample configuration
    """
    sample_config = {
        "data_lake_path": "./data_lake",
        "dsv": {
            "delimiter": ",",
            "strip": True,
            "bookend": "\"",
            "bookend_strip": True,
            "encoding": "utf-8",
            "skip_header_rows": 0,
            "skip_footer_rows": 0,
            "header_rows": 1,
            "skip_empty_rows": True
        }
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"Sample configuration created at: {output_path}")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Splurge Data Profiler - Profile DSV files and create data lakes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile a CSV file with default settings
  python -m splurge_data_profiler.cli profile data.csv config.json
  
  # Profile with custom sample size and create inferred table
  python -m splurge_data_profiler.cli profile data.csv config.json --sample-size 10000 --create-inferred-table
  
  # Create a sample configuration file
  python -m splurge_data_profiler.cli create-config sample_config.json
  
  # Verbose output
  python -m splurge_data_profiler.cli profile data.csv config.json --verbose
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Profile command
    profile_parser = subparsers.add_parser(
        'profile',
        help='Profile a DSV file'
    )
    profile_parser.add_argument(
        'dsv_file',
        type=Path,
        help='Path to the DSV file to profile'
    )
    profile_parser.add_argument(
        'config_file',
        type=Path,
        help='Path to the configuration JSON file'
    )
    profile_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    # Create config command
    config_parser = subparsers.add_parser(
        'create-config',
        help='Create a sample configuration file'
    )
    config_parser.add_argument(
        'output_file',
        type=Path,
        help='Path where to save the sample configuration file'
    )
    
    args = parser.parse_args()
    
    if args.command == 'profile':
        if not args.dsv_file.exists():
            print(f"Error: DSV file not found: {args.dsv_file}", file=sys.stderr)
            sys.exit(1)
        
        run_profiling(
            dsv_path=args.dsv_file,
            config_path=args.config_file,
            verbose=args.verbose
        )
    
    elif args.command == 'create-config':
        create_sample_config(args.output_file)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

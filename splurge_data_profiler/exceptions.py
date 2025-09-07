"""
Custom exceptions for the Splurge Data Profiler.

This module defines domain-specific exceptions used throughout the data profiler
to provide more meaningful error messages and better error handling.
"""

from typing import Optional


class DataProfilerError(Exception):
    """Base exception for all data profiler errors."""

    def __init__(
            self,
            message: str,
            *,
            details: Optional[str] = None
    ) -> None:
        """
        Initialize a DataProfilerError.

        Args:
            message: The error message
            details: Additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message


class ConfigurationError(DataProfilerError):
    """Exception raised for configuration-related errors."""
    pass


class DataSourceError(DataProfilerError):
    """Exception raised for data source-related errors."""
    pass


class ProfilingError(DataProfilerError):
    """Exception raised for profiling-related errors."""
    pass


class DatabaseError(DataProfilerError):
    """Exception raised for database-related errors."""
    pass


class FileProcessingError(DataProfilerError):
    """Exception raised for file processing errors."""
    pass

"""
Integration tests for DsvSource class.

These tests focus on testing DsvSource with real file systems and external dependencies,
validating end-to-end functionality without mocking.
"""

import os
import tempfile
import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing."""
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
        f.write('id,name\n1,Alice\n2,Bob\n')
    file_path = Path(temp_path)

    yield file_path

    # Cleanup
    try:
        os.remove(temp_path)
    except Exception:
        pass


def test_dsv_source_real_file(temp_csv_file):
    """Test DsvSource with a real file."""
    # This will use the real DsvHelper and TabularDataModel
    source = DsvSource(temp_csv_file)
    try:
        columns = source._initialize()
    except (ValueError, RuntimeError):
        raise
    except Exception as exc:
        raise RuntimeError(f"Unexpected error in test: {exc}")
    assert len(columns) >= 2
    assert columns[0].name == "id"
    assert columns[1].name == "name"

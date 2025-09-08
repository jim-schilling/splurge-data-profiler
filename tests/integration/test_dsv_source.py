"""
Integration tests for DsvSource class.

These tests focus on testing DsvSource with real file systems and external dependencies,
validating end-to-end functionality without mocking.
"""

import pytest
from pathlib import Path

from splurge_data_profiler.source import DsvSource


@pytest.fixture
def temp_csv_file(tmp_path: Path):
    """Create a temporary CSV file for testing using pytest tmp_path."""
    file_path = tmp_path / "temp.csv"
    file_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    yield file_path


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

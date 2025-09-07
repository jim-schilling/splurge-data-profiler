"""
Integration tests for DsvSource class.

These tests focus on testing DsvSource with real file systems and external dependencies,
validating end-to-end functionality without mocking.
"""

import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DsvSource


class TestDsvSourceIntegration(unittest.TestCase):
    """Integration test for DsvSource using a real CSV file (no mocking)."""

    def setUp(self) -> None:
        # Create a temporary CSV file
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        self.file_path = Path(self.temp_path)

    def tearDown(self) -> None:
        os.remove(self.temp_path)

    def test_dsv_source_real_file(self):
        # This will use the real DsvHelper and TabularDataModel
        source = DsvSource(self.file_path)
        try:
            columns = source._initialize()
        except (ValueError, RuntimeError):
            raise
        except Exception as exc:
            raise RuntimeError(f"Unexpected error in test: {exc}")
        self.assertTrue(len(columns) >= 2)
        self.assertEqual(columns[0].name, "id")
        self.assertEqual(columns[1].name, "name")


if __name__ == "__main__":
    unittest.main()

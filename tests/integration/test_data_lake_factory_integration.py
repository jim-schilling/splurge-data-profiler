"""
Integration tests for DataLakeFactory class.

These tests focus on testing DataLakeFactory with real file systems and database operations,
validating end-to-end functionality without mocking.
"""

import os
import tempfile
import unittest
from pathlib import Path

from splurge_data_profiler.source import DsvSource, DbSource
from splurge_data_profiler.data_lake import DataLakeFactory, DataLake


class TestDataLakeFactory(unittest.TestCase):
    """Test cases for DataLakeFactory class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a temporary CSV file for testing
        self.temp_fd, self.temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(self.temp_fd, 'w', encoding='utf-8') as f:
            f.write('id,name,value\n1,Alice,10.5\n2,Bob,20.0\n')
        self.test_file_path = Path(self.temp_path)

        # Create a temporary directory for the data lake
        self.temp_dir = tempfile.mkdtemp()
        self.data_lake_path = Path(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        try:
            os.remove(self.temp_path)
        except Exception:
            pass
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except Exception:
            pass

    def test_from_dsv_source_creates_sqlite_table(self) -> None:
        """Test that from_dsv_source creates a SQLite table correctly."""
        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake
        data_lake = DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=self.data_lake_path
        )

        # Verify the data lake was created
        self.assertIsInstance(data_lake, DataLake)
        self.assertIsInstance(data_lake.db_source, DbSource)

        # Verify the SQLite file was created
        expected_db_path = self.data_lake_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())

        # Verify the database URL is correct
        expected_db_url = f"sqlite:///{expected_db_path}"
        self.assertEqual(data_lake.db_url, expected_db_url)

        # Verify the table name is correct
        expected_table_name = self.test_file_path.stem
        self.assertEqual(data_lake.db_table, expected_table_name)

        # Verify the schema is None (SQLite doesn't use schemas)
        self.assertIsNone(data_lake.db_schema)

        # Verify the column names are preserved
        expected_columns = ["id", "name", "value"]
        self.assertEqual(data_lake.column_names, expected_columns)

        # Verify the columns in the database source
        db_columns = data_lake.db_source.columns
        self.assertEqual(len(db_columns), 3)
        self.assertEqual(db_columns[0].name, "id")
        self.assertEqual(db_columns[1].name, "name")
        self.assertEqual(db_columns[2].name, "value")

    def test_from_dsv_source_creates_directory_if_not_exists(self) -> None:
        """Test that from_dsv_source creates the data lake directory if it doesn't exist."""
        # Create a non-existent directory path
        non_existent_path = self.data_lake_path / "new_directory"

        # Create DSV source
        dsv_source = DsvSource(self.test_file_path)

        # Create data lake
        DataLakeFactory.from_dsv_source(
            dsv_source=dsv_source,
            data_lake_path=non_existent_path
        )

        # Verify the directory was created
        self.assertTrue(non_existent_path.exists())
        self.assertTrue(non_existent_path.is_dir())

        # Verify the SQLite file was created in the new directory
        expected_db_path = non_existent_path / f"{self.test_file_path.stem}.sqlite"
        self.assertTrue(expected_db_path.exists())

    def test_from_dsv_source_with_different_file_types(self) -> None:
        """Test that from_dsv_source works with different file extensions."""
        # Create a TSV file
        tsv_fd, tsv_path = tempfile.mkstemp(suffix=".tsv")
        try:
            with os.fdopen(tsv_fd, 'w', encoding='utf-8') as f:
                f.write('id\tname\tvalue\n1\tAlice\t10.5\n2\tBob\t20.0\n')

            tsv_file_path = Path(tsv_path)

            # Create DSV source with tab delimiter
            dsv_source = DsvSource(tsv_file_path, delimiter="\t")

            # Create data lake
            data_lake = DataLakeFactory.from_dsv_source(
                dsv_source=dsv_source,
                data_lake_path=self.data_lake_path
            )

            # Verify the SQLite file was created with the correct name
            expected_db_path = self.data_lake_path / f"{tsv_file_path.stem}.sqlite"
            self.assertTrue(expected_db_path.exists())

            # Verify the table name is correct (without .tsv extension)
            expected_table_name = tsv_file_path.stem
            self.assertEqual(data_lake.db_table, expected_table_name)

        finally:
            try:
                os.remove(tsv_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()

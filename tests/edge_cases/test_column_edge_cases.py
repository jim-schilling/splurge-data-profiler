"""
Edge case tests for Column class.

These tests focus on testing Column with edge cases, boundary conditions,
and unusual scenarios to ensure robust behavior.
"""

import unittest

from splurge_data_profiler.source import Column, DataType


class TestColumnEdgeCases(unittest.TestCase):
    """Test cases for Column class edge cases."""

    def test_column_inferred_type_setter_edge_cases(self) -> None:
        """Test Column.inferred_type setter with edge cases."""
        column = Column("test_column")

        # Test setting various data types
        for data_type in [DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN,
                         DataType.DATE, DataType.TIME, DataType.DATETIME, DataType.TEXT]:
            column.inferred_type = data_type
            self.assertEqual(column.inferred_type, data_type)

        # Test setting to same type multiple times
        original_type = column.inferred_type
        column.inferred_type = original_type
        self.assertEqual(column.inferred_type, original_type)

    def test_column_equality_edge_cases(self) -> None:
        """Test Column equality with edge cases."""
        column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column3 = Column("other", inferred_type=DataType.INTEGER, is_nullable=False)

        # Same columns should be equal
        self.assertEqual(column1, column2)

        # Different names should not be equal
        self.assertNotEqual(column1, column3)

        # Different inferred types should not be equal
        column4 = Column("test", inferred_type=DataType.FLOAT, is_nullable=False)
        self.assertNotEqual(column1, column4)

        # Different nullable should not be equal
        column5 = Column("test", inferred_type=DataType.INTEGER, is_nullable=True)
        self.assertNotEqual(column1, column5)

    def test_column_hash_consistency(self) -> None:
        """Test Column hash consistency."""
        column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
        column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)

        # Equal columns should have equal hashes
        self.assertEqual(hash(column1), hash(column2))

        # Hash should be consistent across calls
        hash1 = hash(column1)
        hash2 = hash(column1)
        self.assertEqual(hash1, hash2)

    def test_column_string_representations(self) -> None:
        """Test Column string representations with various data types."""
        test_cases = [
            (DataType.TEXT, "test_column (DataType.TEXT)"),
            (DataType.INTEGER, "test_column (DataType.INTEGER)"),
            (DataType.FLOAT, "test_column (DataType.FLOAT)"),
            (DataType.BOOLEAN, "test_column (DataType.BOOLEAN)"),
            (DataType.DATE, "test_column (DataType.DATE)"),
            (DataType.TIME, "test_column (DataType.TIME)"),
            (DataType.DATETIME, "test_column (DataType.DATETIME)"),
        ]

        for data_type, expected_str in test_cases:
            column = Column("test_column", inferred_type=data_type)
            self.assertEqual(str(column), expected_str)


if __name__ == "__main__":
    unittest.main()

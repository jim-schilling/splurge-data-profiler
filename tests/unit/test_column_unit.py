import unittest

from splurge_data_profiler.source import Column, DataType


class TestColumn(unittest.TestCase):
    """Test cases for Column class."""

    def test_column_initialization_defaults(self) -> None:
        """Test Column initialization with default values."""
        column = Column("test_column")

        self.assertEqual(column.name, "test_column")
        self.assertEqual(column.inferred_type, DataType.TEXT)
        self.assertEqual(column.raw_type, DataType.TEXT)
        self.assertTrue(column.is_nullable)

    def test_column_initialization_custom_values(self) -> None:
        """Test Column initialization with custom values."""
        column = Column(
            name="custom_column",
            inferred_type=DataType.INTEGER,
            is_nullable=False
        )

        self.assertEqual(column.name, "custom_column")
        self.assertEqual(column.inferred_type, DataType.INTEGER)
        self.assertEqual(column.raw_type, DataType.TEXT)
        self.assertFalse(column.is_nullable)

    def test_column_string_representation(self) -> None:
        """Test Column string representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT)

        expected_str = "test_column (DataType.FLOAT)"
        self.assertEqual(str(column), expected_str)

    def test_column_repr_representation(self) -> None:
        """Test Column repr representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT, is_nullable=False)

        expected_repr = "Column(name=test_column, inferred_type=DataType.FLOAT, raw_type=DataType.TEXT, is_nullable=False)"
        self.assertEqual(repr(column), expected_repr)


if __name__ == '__main__':
    unittest.main()

from splurge_data_profiler.source import Column, DataType


class TestColumn:
    """Test cases for Column class."""

    def test_column_initialization_defaults(self) -> None:
        """Test Column initialization with default values."""
        column = Column("test_column")

        assert column.name == "test_column"
        assert column.inferred_type == DataType.TEXT
        assert column.raw_type == DataType.TEXT
        assert column.is_nullable is True

    def test_column_initialization_custom_values(self) -> None:
        """Test Column initialization with custom values."""
        column = Column(name="custom_column", inferred_type=DataType.INTEGER, is_nullable=False)

        assert column.name == "custom_column"
        assert column.inferred_type == DataType.INTEGER
        assert column.raw_type == DataType.TEXT
        assert column.is_nullable is False

    def test_column_string_representation(self) -> None:
        """Test Column string representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT)

        expected_str = "test_column (DataType.FLOAT)"
        assert str(column) == expected_str

    def test_column_repr_representation(self) -> None:
        """Test Column repr representation."""
        column = Column("test_column", inferred_type=DataType.FLOAT, is_nullable=False)

        expected_repr = (
            "Column(name=test_column, inferred_type=DataType.FLOAT, raw_type=DataType.TEXT, is_nullable=False)"
        )
        assert repr(column) == expected_repr

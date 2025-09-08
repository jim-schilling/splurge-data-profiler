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


def test_column_inferred_type_setter_edge_cases():
    """Test Column.inferred_type setter with edge cases."""
    column = Column("test_column")

    # Test setting various data types
    for data_type in [
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.BOOLEAN,
        DataType.DATE,
        DataType.TIME,
        DataType.DATETIME,
        DataType.TEXT,
    ]:
        column.inferred_type = data_type
        assert column.inferred_type == data_type

    # Test setting to same type multiple times
    original_type = column.inferred_type
    column.inferred_type = original_type
    assert column.inferred_type == original_type


def test_column_equality_edge_cases():
    """Test Column equality with edge cases."""
    column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
    column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
    column3 = Column("other", inferred_type=DataType.INTEGER, is_nullable=False)

    # Same columns should be equal
    assert column1 == column2

    # Different names should not be equal
    assert column1 != column3

    # Different inferred types should not be equal
    column4 = Column("test", inferred_type=DataType.FLOAT, is_nullable=False)
    assert column1 != column4

    # Different nullable should not be equal
    column5 = Column("test", inferred_type=DataType.INTEGER, is_nullable=True)
    assert column1 != column5


def test_column_hash_consistency():
    """Test Column hash consistency."""
    column1 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)
    column2 = Column("test", inferred_type=DataType.INTEGER, is_nullable=False)

    # Equal columns should have equal hashes
    assert hash(column1) == hash(column2)

    # Hash should be consistent across calls
    hash1 = hash(column1)
    hash2 = hash(column1)
    assert hash1 == hash2


def test_column_string_representations():
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
        assert str(column) == expected_str

from splurge_data_profiler.source import DataType


class TestDataType:
    """Test cases for DataType enum."""

    def test_data_type_values(self) -> None:
        """Test that all DataType enum values are correct."""
        expected_values = {
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "DATETIME": "DATETIME",
            "FLOAT": "FLOAT",
            "INTEGER": "INTEGER",
            "TEXT": "TEXT",
            "TIME": "TIME",
        }

        for enum_name, expected_value in expected_values.items():
            enum_member = getattr(DataType, enum_name)
            assert enum_member.value == expected_value

    def test_data_type_membership(self) -> None:
        """Test that DataType enum contains expected members."""
        expected_members = {"BOOLEAN", "DATE", "DATETIME", "FLOAT", "INTEGER", "TEXT", "TIME"}
        actual_members = {member.name for member in DataType}
        assert actual_members == expected_members

"""
Edge case tests for Source abstract base class.

These tests focus on testing Source with edge cases, boundary conditions,
and unusual scenarios to ensure robust behavior.
"""

import unittest

from splurge_data_profiler.source import Column, Source


class TestSourceEdgeCases(unittest.TestCase):
    """Test cases for Source abstract base class edge cases."""

    def test_source_iteration_edge_cases(self) -> None:
        """Test Source iteration with edge cases."""

        class TestSource(Source):
            pass

        # Empty source
        empty_source = TestSource()
        self.assertEqual(len(empty_source), 0)
        self.assertEqual(list(empty_source), [])

        # Source with columns
        columns = [Column("col1"), Column("col2"), Column("col3")]
        source = TestSource(columns=columns)
        self.assertEqual(len(source), 3)
        self.assertEqual(list(source), columns)

    def test_source_indexing_edge_cases(self) -> None:
        """Test Source indexing with edge cases."""

        class TestSource(Source):
            pass

        columns = [Column("col1"), Column("col2")]
        source = TestSource(columns=columns)

        # Valid indexing
        self.assertEqual(source[0], columns[0])
        self.assertEqual(source[1], columns[1])

        # Negative indexing
        self.assertEqual(source[-1], columns[1])
        self.assertEqual(source[-2], columns[0])

        # Out of bounds indexing
        with self.assertRaises(IndexError):
            _ = source[2]
        with self.assertRaises(IndexError):
            _ = source[-3]

    def test_source_equality_edge_cases(self) -> None:
        """Test Source equality with edge cases."""

        class TestSource(Source):
            pass

        # Empty sources
        source1 = TestSource()
        source2 = TestSource()
        self.assertEqual(source1, source2)

        # Sources with same columns
        columns1 = [Column("col1"), Column("col2")]
        columns2 = [Column("col1"), Column("col2")]
        source3 = TestSource(columns=columns1)
        source4 = TestSource(columns=columns2)
        self.assertEqual(source3, source4)

        # Sources with different columns
        columns3 = [Column("col1"), Column("col3")]
        source5 = TestSource(columns=columns3)
        self.assertNotEqual(source3, source5)

        # Different lengths
        source6 = TestSource(columns=[Column("col1")])
        self.assertNotEqual(source3, source6)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for core module."""

import unittest
import unittest.mock as mock

from nopandas import (
    _QDataFrame,
    _Attribute,
)


class TestQDataFrame(unittest.TestCase):
    """Test bacis API of QDataFrame."""

    def setUp(self):
        """Create frame."""
        self.qdf = _QDataFrame(source=mock.Mock(), schema=mock.Mock())

    def test_slicing_with_non_existed_column(self):
        """Slice QDataFrame with non-existed columns."""
        with mock.patch.object(_QDataFrame, 'attributes'):
            self.qdf.attributes.return_value = {'key1': None, 'key2': None}
            with self.assertRaisesRegex(ValueError, 'WRONG_KEY'):
                self.qdf[['WRONG_KEY', 'key1']]


class TestQDataFrameMerging(unittest.TestCase):
    """Test merging pair of QDataFrame."""

    def setUp(self):
        """Create two frames."""
        self.left = _QDataFrame(source=mock.Mock(), schema=mock.Mock())
        self.right = _QDataFrame(source=mock.Mock(), schema=mock.Mock())

    def test_merge_on_non_existed_column(self):
        """Merge QDataFrame along wrong column name."""
        with mock.patch.object(_QDataFrame, 'attributes'):
            self.left.attributes.return_value = {'key1': None, 'key2': None}
            self.right.attributes.return_value = {'key1': None, 'key3': None}
            with self.assertRaisesRegex(ValueError, "is not in both frames"):
                self.left.merge(self.right, on='key')


class TestQSeriesExpressionCompilation(unittest.TestCase):
    """Test compilation of attribute expression."""

    def setUp(self):
        """Create pure attribute."""
        self.attr = _Attribute('attr', source='table')

    def test_equal_to_number(self):
        """Compare that attribute equal to fix number."""
        self.assertEqual(
            str(_Attribute((_Attribute.eq, self.attr, 10))),
            '(table.attr = 10)'
        )

    def test_equal_to_str(self):
        """Compare that attribute equal to fix string."""
        self.assertEqual(
            str(_Attribute((_Attribute.eq, self.attr, 'CAT'))),
            "(table.attr = 'CAT')"
        )

#
# Copyright 2024 Tabs Data Inc.
#

import unittest

import polars as pl

import tabsdata as td
from tabsdata.utils.tableframe import _constants, _helpers

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._helpers import SYSTEM_COLUMNS

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_simple_dataframe


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        self.data_frame, self.lazy_frame, self.table_frame = load_simple_dataframe()

    def test_columns(self):
        columns = self.table_frame.columns
        self.assertTrue("intColumn" in columns)
        self.assertTrue("stringColumn" in columns)

    def test_dtypes(self):
        expected_dtypes = [pl.Int64, pl.Utf8]
        for column, metadata in _helpers.SYSTEM_COLUMNS_METADATA.items():
            expected_dtypes.append(metadata[_constants.TD_COL_DTYPE])
        dtypes = self.table_frame.dtypes
        self.assertEqual(dtypes, expected_dtypes)

    def test_schema(self):
        schema = str(self.table_frame)
        self.assertTrue("intColumn" in schema)
        self.assertTrue("stringColumn" in schema)

    def test_width(self):
        width = self.table_frame.width
        self.assertEqual(width, 2 + len(SYSTEM_COLUMNS))

    def test_bool(self):
        with self.assertRaises(TypeError):
            self.table_frame.__bool__()

    def test_eq(self):
        assert self.table_frame.__eq__(self.table_frame)

    def test_ne(self):
        tf = td.TableFrame(self.table_frame)
        assert self.table_frame.__ne__(tf)

    def test_gt(self):
        with self.assertRaises(TypeError):
            self.table_frame.__gt__(self.table_frame)

    def test_lt(self):
        with self.assertRaises(TypeError):
            self.table_frame.__lt__(self.table_frame)

    def test_ge(self):
        with self.assertRaises(TypeError):
            self.table_frame.__ge__(self.table_frame)

    def test_le(self):
        with self.assertRaises(TypeError):
            self.table_frame.__le__(self.table_frame)

    def test_contains(self):
        assert self.table_frame.__contains__("intColumn")
        assert self.table_frame.__contains__("stringColumn")

    def test_getitem_index(self):
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            self.table_frame.__getitem__([2])

    def test_getitem_index_bracket(self):
        with self.assertRaises(TypeError):
            _ = self.table_frame[2]

    def test_getitem_list(self):
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            self.table_frame.__getitem__([2, 3])

    def test_getitem_list_bracket(self):
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            _ = self.table_frame[2, 3]

    def test_getitem_slice(self):
        ttf = self.table_frame.__getitem__(slice(2, 3))
        assert len(ttf._lf.collect().rows()) == 1
        assert len(ttf.columns) >= 2

    def test_getitem_slice_bracket(self):
        ttf = self.table_frame[2:3]
        assert len(ttf._lf.collect().rows()) == 1

    def test_str(self):
        string = self.table_frame.__str__()
        self.assertIsInstance(string, str)
        self.assertTrue("intColumn" in string)
        self.assertTrue("stringColumn" in string)

    def test_str_function(self):
        string = str(self.table_frame)
        self.assertIsInstance(string, str)
        self.assertTrue("intColumn" in string)
        self.assertTrue("stringColumn" in string)

    def test_repr(self):
        string = self.table_frame.__repr__()
        self.assertTrue(string.startswith("<TableFrame at"))

    def test_repr_function(self):
        string = repr(self.table_frame)
        self.assertTrue(string.startswith("<TableFrame at"))

    # ToDo
    def test_limit(self):
        limited = self.table_frame.limit(2)
        assert "intColumn" in limited.columns
        assert "stringColumn" in limited.columns
        assert len(limited._lf.collect().rows()) == 2

    # ToDo
    def test_head_tail(self):
        head = self.table_frame.head(2)
        tail = self.table_frame.tail(2)
        self.assertEqual(len(head._lf.collect().rows()), 2)
        self.assertEqual(len(tail._lf.collect().rows()), 2)

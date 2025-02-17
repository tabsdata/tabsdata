#
# Copyright 2024 Tabs Data Inc.
#

import unittest
from collections import Counter

import pytest_check as check

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        self.data_frame, self.lazy_frame, self.table_frame = load_complex_dataframe()

    def test_columns_all(self):
        expected_columns = self.data_frame.collect_schema().names()
        lf = self.table_frame.select("*")
        columns = lf.columns
        check.equal(Counter(columns), Counter(expected_columns))

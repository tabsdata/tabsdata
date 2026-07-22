#
# Copyright 2024 Tabsdata Inc.
#

import unittest
from collections import Counter

import pytest_check as check

# noinspection PyProtectedMemberº
from tabsdata._utils.tableframe._helpers import OPTIONAL_SYSTEM_COLUMNS, SYSTEM_COLUMNS

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        self.data_frame, self.lazy_frame, self.table_frame = load_complex_dataframe()

    def test_columns_all(self):
        expected_columns = self.data_frame.collect_schema().names()
        tf = self.table_frame.select("*")
        columns = tf.columns("all")
        system_optional = set(OPTIONAL_SYSTEM_COLUMNS)
        required_sys = [column for column in SYSTEM_COLUMNS if column not in system_optional]
        check.equal(Counter(columns), Counter(expected_columns + required_sys))

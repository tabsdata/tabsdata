#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import tempfile
import unittest

import polars as pl

from tabsdata._utils.tableframe._common import drop_inception_regenerate_system_columns
from tabsdata._utils.tableframe._constants import TD_COL_INCEPTION, Inception
from tabsdata._utils.tableframe._helpers import SYSTEM_COLUMNS_METADATA

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import (
    _unwrap_table_frame,
    _wrap_polars_frame,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import load_complex_dataframe, pretty_polars

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        pretty_polars()

        self.lazy_frame, self.data_frame, self.table_frame = load_complex_dataframe(
            token="l"
        )

    def test_system_columns_integrity(self):
        lf = self.lazy_frame
        tf = self.table_frame

        lf_row_count = lf.select(pl.len()).collect().item()
        tf_row_count = lf.select(pl.len()).collect().item()
        self.assertEqual(lf_row_count, tf_row_count, "Setup mismatch.")

        lf_len = lf.select(pl.len()).collect().item()
        lf_count = lf.select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, lf_len, "LazyFrame len (1) mismatch.")
        self.assertEqual(lf_row_count, lf_count, "LazyFrame count (1) mismatch.")

        tf_len = _unwrap_table_frame(tf).select(pl.len()).collect().item()
        tf_count = _unwrap_table_frame(tf).select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, tf_len, "TableFrame len (1.2) mismatch.")
        self.assertEqual(lf_row_count, tf_count, "TableFrame count (1) mismatch.")

        tf = tf.drop("island")
        lf_len = lf.select(pl.len()).collect().item()
        lf_count = lf.select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, lf_len, "LazyFrame len (2) mismatch.")
        self.assertEqual(lf_row_count, lf_count, "LazyFrame count (2) mismatch.")
        tf_len = tf._lf.select(pl.len()).collect(no_optimization=True).item()
        self.assertEqual(lf_row_count, tf_len, "TableFrame len (2.1) mismatch.")
        tf_len = _unwrap_table_frame(tf).select(pl.len()).collect().item()
        tf_count = _unwrap_table_frame(tf).select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, tf_len, "TableFrame len (2.2) mismatch.")
        self.assertEqual(lf_row_count, tf_count, "TableFrame count (2) mismatch.")

        parquet = os.path.join(tempfile.gettempdir(), "my.parquet")

        lf.sink_parquet(parquet)
        lf = pl.scan_parquet(parquet)
        tf = _wrap_polars_frame(lf)

        lf_len = lf.select(pl.len()).collect().item()
        lf_count = lf.select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, lf_len, "LazyFrame len (3.2) mismatch.")
        self.assertEqual(lf_row_count, lf_count, "LazyFrame count (3) mismatch.")
        tf_len = _unwrap_table_frame(tf).select(pl.len()).collect().item()
        tf_count = _unwrap_table_frame(tf).select(pl.count()).collect().item()
        self.assertEqual(lf_row_count, tf_len, "TableFrame len (3) mismatch.")
        self.assertEqual(lf_row_count, tf_count, "TableFrame count (3) mismatch.")

    def test_drop_inception_regenerate_system_columns(self):
        expected_drop = [
            column
            for column, metadata in SYSTEM_COLUMNS_METADATA.items()
            if metadata.get(TD_COL_INCEPTION) == Inception.REGENERATE
        ]

        expected_kept = [
            column
            for column, metadata in SYSTEM_COLUMNS_METADATA.items()
            if metadata.get(TD_COL_INCEPTION) != Inception.REGENERATE
        ]

        i_lf = self.table_frame._lf
        o_lf = drop_inception_regenerate_system_columns(lf=i_lf, ignore_missing=True)

        i_lf_columns = set(i_lf.collect_schema().names())
        o_lf_columns = set(o_lf.collect_schema().names())

        should_have_been_dropped = [
            column for column in expected_drop if column in o_lf_columns
        ]
        assert not should_have_been_dropped, (
            "These system columns should have been dropped but are still present:"
            f" {should_have_been_dropped}"
        )

        should_have_been_kept = [
            column for column in expected_kept if column in i_lf_columns
        ]
        mistakenly_dropped = [
            col for col in should_have_been_kept if col not in o_lf_columns
        ]
        assert not mistakenly_dropped, (
            "These system columns should not have been dropped but are missing:"
            f" {mistakenly_dropped}"
        )

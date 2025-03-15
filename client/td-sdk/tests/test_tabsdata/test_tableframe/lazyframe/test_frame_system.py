#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import tempfile
import unittest

import polars as pl

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import (
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
        tf_len = tf._lf.select(pl.len()).collect().item()
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

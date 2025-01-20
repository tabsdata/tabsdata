#
# Copyright 2025 Tabs Data Inc.
#

import logging
import unittest

import polars as pl

import tabsdata as td

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._translator import _wrap_polars_frame

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401
from ..common import (
    enrich_dataframe,
    load_complex_dataframe,
    load_simple_dataframe,
    pretty_polars,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestTableFrame(unittest.TestCase):

    def setUp(self):
        pretty_polars()

        self.data_frame_l, self.lazy_frame_l, self.table_frame_l = (
            load_complex_dataframe(token="l")
        )
        self.data_frame_r, self.lazy_frame_r, self.table_frame_r = (
            load_complex_dataframe(token="r")
        )

    def test_concat(self):
        _, _, tf_top = load_simple_dataframe(token="top-")
        _, _, tf_bottom = load_simple_dataframe(token="bottom-")
        tf = td.concat([tf_top, tf_bottom])
        assert tf is not None
        assert len(tf._lf.collect().rows()) == 6

    def test_lit(self):
        lf = enrich_dataframe(
            pl.LazyFrame(
                {
                    "letters": ["a", "b", "c"],
                    "numbers": [1, 2, 3],
                }
            )
        )
        tf = _wrap_polars_frame(lf)
        tf = tf.with_columns([pl.lit("こんにちは世界").alias("token")])

        rows = tf._lf.collect()
        column = rows["token"]
        assert all(
            value == "こんにちは世界" for value in column
        ), "Token column values do not match expected value!"

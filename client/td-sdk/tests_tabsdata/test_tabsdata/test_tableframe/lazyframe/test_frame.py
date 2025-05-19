#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import tempfile
import time
import unittest

import polars as pl

import tabsdata as td
from tabsdata.exceptions import TableFrameError

# noinspection PyProtectedMember
from tabsdata.tableframe.lazyframe.frame import _assemble_columns

# noinspection PyProtectedMember
from tabsdata.utils.tableframe._helpers import required_columns

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

        self.data_frame_l, self.lazy_frame_l, self.table_frame_l = (
            load_complex_dataframe(token="l")
        )
        self.data_frame_r, self.lazy_frame_r, self.table_frame_r = (
            load_complex_dataframe(token="r")
        )

    def test_drop_nulls(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        tf = tf.drop_nulls()
        assert tf is not None
        assert len(tf._lf.collect().rows()) == 1

    def test_rename_none(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TypeError) as context:
            _ = tf.rename(None)

    def test_rename_no_dict(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TypeError) as context:
            _ = tf.rename(("c1", "cc1"))

    def test_rename_empty_dict(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        _ = tf.rename({})

    def test_rename_no_string_old(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TypeError) as context:
            _ = tf.rename({1: "id"})

    def test_rename_no_string_new(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TypeError) as context:
            _ = tf.rename({"id": 1})

    def test_rename_old_name_system(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TableFrameError) as context:
            _ = tf.rename({"$td.id": "id"})
        assert context.exception.code == "TF-010"

    def test_rename_new_name_system(self):
        lf = pl.LazyFrame(
            {
                "c1": [11, 12, 13],
                "c2": ["2a", None, "2c"],
                "c3": ["3a", "3b", None],
            }
        )
        tf = _wrap_polars_frame(lf)
        with self.assertRaises(TableFrameError) as context:
            _ = tf.rename({"c1": "$td.c1"})
        assert context.exception.code == "TF-010"

    def test_join_common(self):
        _, _, tf = load_complex_dataframe(token="0-")
        # start_time = time.perf_counter()
        for i in range(32):
            _ = self.table_frame_l.join(self.table_frame_r, on="rowid", how="inner")
        # end_time = time.perf_counter()

    def test_join_weird(self):
        pl.Config.set_tbl_cols(1024)
        pl.Config.set_tbl_rows(1024)
        pl.Config.set_fmt_table_cell_list_len(1024)
        pl.Config.set_tbl_width_chars(4096)
        pl.Config.set_fmt_str_lengths(4096)

        lf = pl.LazyFrame(
            {
                "$td.id": ["id0.0", "id0.1", "id0.2"],
                "$td.id1": ["id1.0", "id1.1", "id1.2"],
                "$td.id2": ["id2.2", "id2.2", "id2.2"],
                "$td.id_x": ["id0.0", "id0.1", "id0.2"],
                "$td.id1_x": ["id1.0", "id1.1", "id1.2"],
                "$td.id2_x": ["id2.2", "id2.2", "id2.2"],
                "$td.src": [["src0.0"], ["src0.1"], ["src0.2"]],
                "$td.src1": [["src1.0"], ["src1.1"], ["src1.2"]],
                "$td.src2": [["src2.0"], ["src2.1"], ["src2.2"]],
                "$td.src_x": [["src0.0"], ["src0.1"], ["src0.2"]],
                "$td.src1_x": [["src1.0"], ["src1.1"], ["src1.2"]],
                "$td.src2_x": [["src2.0"], ["src2.1"], ["src2.2"]],
                "other": ["other0.0", "other0.1", "other0.2"],
                "other1": ["other1.0", "other1.1", "other1.2"],
                "other2": ["other2.0", "other2.1", "other2.2"],
            }
        )
        lf = pl.concat([lf] * 10)
        start_time = time.perf_counter()
        lf = _assemble_columns(lf)
        lf._lf.profile()
        end_time = time.perf_counter()
        logger.debug(f"Execution time: {end_time - start_time:.6f} seconds")

    def test_select_one(self):
        tf = _wrap_polars_frame(pl.LazyFrame({"letters": ["a", "b"]}))
        tf = tf.select("letters")
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_one_col(self):
        tf = _wrap_polars_frame(pl.LazyFrame({"letters": ["a", "b"]}))
        tf = tf.select(td.col("letters"))
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_all(self):
        tf = _wrap_polars_frame(pl.LazyFrame({"letters": ["a", "b"]}))
        tf = tf.select("*")
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_all_col(self):
        tf = _wrap_polars_frame(pl.LazyFrame({"letters": ["a", "b"]}))
        tf = tf.select(td.col("*"))
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_one_and_all(self):
        tf = _wrap_polars_frame(
            pl.LazyFrame({"letters": ["a", "b"], "numbers": [1, 2]})
        )
        tf = tf.select("letters").select("*")
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_one_and_all_col(self):
        tf = _wrap_polars_frame(
            pl.LazyFrame({"letters": ["a", "b"], "numbers": [1, 2]})
        )
        tf = tf.select(td.col("letters")).select(td.col("*"))
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_all_and_one(self):
        tf = _wrap_polars_frame(
            pl.LazyFrame({"letters": ["a", "b"], "numbers": [1, 2]})
        )
        tf = tf.select("*").select("letters")
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_select_all_and_one_col(self):
        tf = _wrap_polars_frame(
            pl.LazyFrame({"letters": ["a", "b"], "numbers": [1, 2]})
        )
        tf = tf.select(td.col("*")).select(td.col("letters"))
        columns = tf.columns("all")
        assert len(tf.columns("all")) == len(required_columns()) + 1
        for column in required_columns():
            assert column in columns

    def test_str(self):
        lf = pl.LazyFrame(
            {
                "letters": ["a", "b", "c"],
                "numbers": [1, 2, 3],
            }
        )
        tf = td.TableFrame.__build__(
            lf,
            None,
        )
        tf = tf.with_columns(
            td.col("letters").str.to_uppercase().alias("letters_uppercase")
        )
        columns = tf.columns("all")
        for column in required_columns():
            assert column in columns

        rows = tf._lf.collect()
        column = rows["letters_uppercase"]
        assert all(value.isupper() for value in column)

    def test_sink(self):
        lf = pl.LazyFrame(
            {
                "letters": ["a", "b", "c"],
                "numbers": [1, 2, 3],
            }
        )
        tf = td.TableFrame.__build__(
            lf,
            None,
        )
        tf._lf.sink_ndjson(os.path.join(tempfile.gettempdir(), "delete.sink_1.json"))
        _unwrap_table_frame(tf).sink_ndjson(
            os.path.join(tempfile.gettempdir(), "delete.sink_2.json")
        )

    def test_item_empty(self):
        lf = pl.LazyFrame(
            {
                "letters": [],
                "numbers": [],
            }
        )
        tf = td.TableFrame.__build__(
            lf,
            None,
        )

        item = tf.select(td.col("numbers").mean()).item()
        assert item is None

    def test_item(self):
        lf = pl.LazyFrame(
            {
                "letters": ["a", "b", "c"],
                "numbers": [1, 2, 3],
            }
        )
        tf = td.TableFrame.__build__(
            lf,
            None,
        )

        item = tf.select(td.col("numbers").mean()).item()
        assert item == 2.0

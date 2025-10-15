#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

import tabsdata as td
from tabsdata.tableframe.udf.function import UDF


class SimpleUDF(UDF):

    def on_element(self, values):
        return values


class TestUDFSchema:
    def test_init_success(self):
        udf = SimpleUDF([("a", pl.Int64), ("b", pl.String)])
        schema = udf._schema
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "a"
        assert schema.columns[0].dtype == pl.Int64
        assert schema.columns[1].name == "b"
        assert schema.columns[1].dtype == pl.String

    def test_init_with_single_tuple(self):
        udf = SimpleUDF(("a", pl.Int64))
        schema = udf._schema
        assert len(schema.columns) == 1
        assert schema.columns[0].name == "a"
        assert schema.columns[0].dtype == pl.Int64

    def test_init_empty_list_raises_error(self):
        with pytest.raises(
            ValueError,
            match="The columns list provided cannot be empty.",
        ):
            SimpleUDF([])

    def test_init_none_name_raises_error(self):
        with pytest.raises(
            ValueError,
            match="Column name at index 0 cannot be None",
        ):
            # noinspection PyTypeChecker
            SimpleUDF([(None, pl.Int64)])

    def test_init_none_dtype_raises_error(self):
        with pytest.raises(
            ValueError,
            match="Column data type at index 0 cannot be None",
        ):
            # noinspection PyTypeChecker
            SimpleUDF([("a", None)])

    def test_init_invalid_item_raises_error(self):
        with pytest.raises(
            TypeError,
            match="not a \(name, data type\) tuple",
        ):
            # noinspection PyTypeChecker
            SimpleUDF(["a", "b"])

    def test_output_columns_full_update(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        udf.with_columns([("c", td.Float32), ("d", td.Boolean)])
        schema = udf._schema
        assert schema.columns[0].name == "c"
        assert schema.columns[0].dtype == td.Float32
        assert schema.columns[1].name == "d"
        assert schema.columns[1].dtype == td.Boolean

    def test_output_columns_partial_update_preserves_rest(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        udf.with_columns([("c", td.Float32)])
        schema = udf._schema
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "c"
        assert schema.columns[0].dtype == td.Float32
        assert schema.columns[1].name == "b"
        assert schema.columns[1].dtype == td.String

    def test_output_columns_with_single_tuple(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        udf.with_columns(("c", td.Float32))
        schema = udf._schema
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "c"
        assert schema.columns[0].dtype == td.Float32
        assert schema.columns[1].name == "b"
        assert schema.columns[1].dtype == td.String

    def test_output_columns_list_preserve_name(self):
        udf = SimpleUDF([("a", td.Int64)])
        udf.with_columns([(None, td.Float32)])
        schema = udf._schema
        assert schema.columns[0].name == "a"
        assert schema.columns[0].dtype == td.Float32

    def test_output_columns_list_preserve_dtype(self):
        udf = SimpleUDF([("a", td.Int64)])
        udf.with_columns([("b", None)])
        schema = udf._schema
        assert schema.columns[0].name == "b"
        assert schema.columns[0].dtype == td.Int64

    def test_output_columns_too_long_raises_error(self):
        udf = SimpleUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="expects at most 1 columns, but 2 were provided",
        ):
            udf.with_columns([("b", td.Int32), ("c", td.String)])

    def test_output_columns_update_single(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        udf.with_columns({1: ("c", td.Float32)})
        schema = udf._schema
        assert schema.columns[0].name == "a"
        assert schema.columns[0].dtype == td.Int64
        assert schema.columns[1].name == "c"
        assert schema.columns[1].dtype == td.Float32

    def test_output_columns_dict_preserve_name(self):
        udf = SimpleUDF([("a", td.Int64)])
        udf.with_columns({0: (None, td.Float32)})
        schema = udf._schema
        assert schema.columns[0].name == "a"
        assert schema.columns[0].dtype == td.Float32

    def test_output_columns_dict_preserve_dtype(self):
        udf = SimpleUDF([("a", td.Int64)])
        udf.with_columns({0: ("b", None)})
        schema = udf._schema
        assert schema.columns[0].name == "b"
        assert schema.columns[0].dtype == td.Int64

    def test_output_columns_invalid_index_too_high_raises_error(self):
        udf = SimpleUDF([("a", td.Int64)])
        with pytest.raises(
            IndexError,
            match="Invalid index provided",
        ):
            udf.with_columns({1: ("b", td.Int32)})

    def test_output_columns_invalid_index_negative_raises_error(self):
        udf = SimpleUDF([("a", td.Int64)])
        with pytest.raises(
            IndexError,
            match="Invalid index provided",
        ):
            udf.with_columns({-1: ("b", td.Int32)})

    def test_internal_names_method_success(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        names = udf._names(width=2)
        assert names == ["a", "b"]

    def test_internal_names_method_raises_on_width_mismatch(self):
        udf = SimpleUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="UDF produced 2 output columns",
        ):
            udf._names(width=2)

    def test_internal_dtypes_method_success(self):
        udf = SimpleUDF([("a", td.Int64), ("b", td.String)])
        dtypes = udf._dtypes(width=2)
        assert dtypes == [td.Int64, td.String]

    def test_internal_dtypes_method_raises_on_width_mismatch(self):
        udf = SimpleUDF([("a", td.Int64)])
        with pytest.raises(
            ValueError,
            match="UDF produced 2 output columns",
        ):
            udf._dtypes(width=2)

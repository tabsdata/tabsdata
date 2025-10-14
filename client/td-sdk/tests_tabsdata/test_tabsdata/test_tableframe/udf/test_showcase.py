#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

import tabsdata as td
from tabsdata.tableframe.udf.function import UDF
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    load_normalized_complex_dataframe,
)


class TestUDFExperience:
    @pytest.mark.parametrize(
        "udf_mode, input_col, factor, out_col_name, expected_val",
        [
            ("element", "bill_length_mm", 10, "len_x10", 391.0),
            ("batch", "bill_length_mm", 10, "len_x10", 391.0),
            ("element", "bill_depth_mm", -2, "depth_x_neg_2", -37.4),
            ("batch", "bill_depth_mm", -2, "depth_x_neg_2", -37.4),
            ("element", "flipper_length_mm", 1.5, "flipper_x_1_5", 271.5),
            ("batch", "flipper_length_mm", 1.5, "flipper_x_1_5", 271.5),
            ("element", "body_mass_g", 0.5, "mass_half", 1875.0),
            ("batch", "body_mass_g", 0.5, "mass_half", 1875.0),
        ],
    )
    def test_scaler_udf_variations(
        self, udf_mode, input_col, factor, out_col_name, expected_val
    ):
        class ScalerUDF(UDF):
            def __init__(self, output_columns, i_factor: float):
                super().__init__(output_columns)
                self.factor = i_factor

            if udf_mode == "element":

                def on_element(self, values: list) -> list:
                    if values[0] is None:
                        return [None]
                    return [values[0] * self.factor]

            else:

                def on_batch(self, series: list) -> list:
                    return [series[0] * self.factor]

        _, _, tf = load_normalized_complex_dataframe()

        scaler = ScalerUDF([(out_col_name, pl.Float64)], i_factor=factor)

        result_tf = tf.udf(td.col(input_col), scaler)
        collected = result_tf._lf.collect()

        assert out_col_name in collected.columns
        assert collected[out_col_name][0] == pytest.approx(expected_val)

    @pytest.mark.parametrize(
        "udf_mode, input_col, threshold, low, high, out_name, expected_val",
        [
            (
                "element",
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "batch",
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "element",
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "batch",
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "element",
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "batch",
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "element",
                "year",
                2008,
                "early",
                "late",
                "year_cat",
                "early",
            ),
            (
                "batch",
                "year",
                2008,
                "early",
                "late",
                "year_cat",
                "early",
            ),
        ],
    )
    def test_categorization_udf_variations(
        self, udf_mode, input_col, threshold, low, high, out_name, expected_val
    ):
        class CategoryUDF(UDF):
            def __init__(
                self, output_columns, i_threshold: float, low_name: str, high_name: str
            ):
                super().__init__(output_columns)
                self.threshold = i_threshold
                self.low_name = low_name
                self.high_name = high_name

            if udf_mode == "element":

                def on_element(self, values: list) -> list:
                    if values[0] is None:
                        return [None]
                    return [
                        self.high_name if values[0] > self.threshold else self.low_name
                    ]

            else:

                def on_batch(self, series: list) -> list:
                    input_series = series[0]
                    return [
                        input_series.map_elements(
                            lambda x: (
                                self.high_name
                                if x is not None and x > self.threshold
                                else self.low_name if x is not None else None
                            )
                        )
                    ]

        _, _, tf = load_normalized_complex_dataframe()

        categorizer = CategoryUDF([(out_name, td.String)], threshold, low, high)

        result_tf = tf.udf(td.col(input_col), categorizer)
        collected = result_tf._lf.collect()

        assert out_name in collected.columns
        assert collected[out_name][0] == expected_val

    @pytest.mark.parametrize(
        "udf_mode, col_pair",
        [
            ("element", ("bill_length_mm", "bill_depth_mm")),
            ("batch", ("bill_length_mm", "bill_depth_mm")),
            ("element", ("flipper_length_mm", "body_mass_g")),
            ("batch", ("flipper_length_mm", "body_mass_g")),
        ],
    )
    def test_multi_feature_udf_variations(self, udf_mode, col_pair):
        class MultiFeatureUDF(UDF):
            if udf_mode == "element":

                def on_element(self, values: list) -> list:
                    val1, val2 = values[0], values[1]
                    if val1 is None or val2 is None:
                        return [None, None, None, None]
                    return [
                        val1 + val2,
                        val1 - val2,
                        val1 * val2,
                        val1 / val2 if val2 != 0 else None,
                    ]

            else:

                def on_batch(self, series: list) -> list:
                    s1, s2 = series[0], series[1]
                    div_series = (s1 / s2).replace([float("inf"), float("-inf")], None)
                    return [
                        s1 + s2,
                        s1 - s2,
                        s1 * s2,
                        div_series,
                    ]

        _, _, tf = load_normalized_complex_dataframe()
        c1, c2 = col_pair

        schema = [
            (f"{c1}_{c2}_sum", td.Float64),
            (f"{c1}_{c2}_diff", td.Float64),
            (f"{c1}_{c2}_prod", td.Float64),
            (f"{c1}_{c2}_div", td.Float64),
        ]

        feature_creator = MultiFeatureUDF(schema)

        result_tf = tf.udf(td.col(c1, c2), feature_creator)
        collected = result_tf._lf.collect()

        row0 = collected.row(0, named=True)
        v1, v2 = row0[c1], row0[c2]

        assert f"{c1}_{c2}_sum" in collected.columns
        assert f"{c1}_{c2}_diff" in collected.columns
        assert f"{c1}_{c2}_prod" in collected.columns
        assert f"{c1}_{c2}_div" in collected.columns
        assert row0[f"{c1}_{c2}_sum"] == pytest.approx(v1 + v2)
        assert row0[f"{c1}_{c2}_diff"] == pytest.approx(v1 - v2)

    @pytest.mark.parametrize(
        "start_name1, start_name2",
        [("feat1", "feat2"), ("a", "b"), ("x", "y"), ("tmp1", "tmp2")],
    )
    def test_schema_modification_variations(self, start_name1, start_name2):
        class PassthroughUDF(UDF):
            def on_batch(self, series: list) -> list:
                return series

        _, _, tf = load_normalized_complex_dataframe()

        passthrough = PassthroughUDF(
            [(start_name1, td.Float64), (start_name2, td.Float64)]
        )

        passthrough.output_columns(
            [(f"{start_name1}_renamed", td.String)]
        ).output_columns({1: (f"{start_name2}_final", None)})

        result_tf = tf.udf(td.col("bill_length_mm", "bill_depth_mm"), passthrough)
        collected = result_tf._lf.collect()

        assert f"{start_name1}_renamed" in collected.columns
        assert f"{start_name2}_final" in collected.columns
        assert collected[f"{start_name1}_renamed"].dtype == td.String
        assert collected[f"{start_name2}_final"].dtype == td.Float64

    @pytest.mark.parametrize(
        "udf1_factor, udf2_threshold",
        [(2.0, 8000), (0.5, 2000), (10, 40000), (-1, -3000)],
    )
    def test_pipeline_variations(self, udf1_factor, udf2_threshold):
        class PipelineScalerUDF(UDF):
            def __init__(self, output_columns, factor: float):
                super().__init__(output_columns)
                self.factor = factor

            def on_batch(self, series: list) -> list:
                return [series[0] * self.factor]

        class PipelineCategoryUDF(UDF):
            def __init__(self, output_columns, threshold: float):
                super().__init__(output_columns)
                self.threshold = threshold

            def on_element(self, values: list) -> list:
                if values[0] is None:
                    return [None]
                return ["high" if values[0] > self.threshold else "low"]

        _, _, tf = load_normalized_complex_dataframe()

        scaler = PipelineScalerUDF([("mass_scaled", td.Float64)], factor=udf1_factor)
        categorizer = PipelineCategoryUDF(
            [("mass_cat", td.String)], threshold=udf2_threshold
        )

        result_tf = (
            tf.udf(td.col("body_mass_g"), scaler)
            .with_columns(
                (td.col("mass_scaled") / td.col("bill_depth_mm")).alias(
                    "scaled_to_depth"
                )
            )
            .udf(td.col("mass_scaled"), categorizer)
        )

        collected = result_tf._lf.collect()
        assert "mass_scaled" in collected.columns
        assert "scaled_to_depth" in collected.columns
        assert "mass_cat" in collected.columns

        mass_g_0 = collected["body_mass_g"][0]
        mass_scaled_0 = collected["mass_scaled"][0]
        mass_cat_0 = collected["mass_cat"][0]

        if mass_g_0 is not None:
            assert mass_scaled_0 == pytest.approx(mass_g_0 * udf1_factor)
            assert mass_cat_0 == ("high" if mass_scaled_0 > udf2_threshold else "low")

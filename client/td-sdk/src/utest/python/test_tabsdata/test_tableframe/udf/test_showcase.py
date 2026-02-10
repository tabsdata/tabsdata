#
# Copyright 2025 Tabs Data Inc.
#

import polars as pl
import pytest

import tabsdata as td
from tabsdata.tableframe.udf.function import (
    SIGNATURE_LIST,
    SIGNATURE_UNPACKED,
    UDFList,
    UDFUnpacked,
)
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    load_normalized_complex_dataframe,
)


class TestUDFExperience:
    @pytest.mark.parametrize(
        "udf_mode, signature_type, input_col, factor, out_col_name, expected_val",
        [
            (
                "element",
                SIGNATURE_LIST,
                "bill_length_mm",
                10,
                "len_x10",
                391.0,
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "bill_length_mm",
                10,
                "len_x10",
                391.0,
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "bill_length_mm",
                10,
                "len_x10",
                391.0,
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "bill_length_mm",
                10,
                "len_x10",
                391.0,
            ),
            (
                "element",
                SIGNATURE_LIST,
                "bill_depth_mm",
                -2,
                "depth_x_neg_2",
                -37.4,
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "bill_depth_mm",
                -2,
                "depth_x_neg_2",
                -37.4,
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "bill_depth_mm",
                -2,
                "depth_x_neg_2",
                -37.4,
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "bill_depth_mm",
                -2,
                "depth_x_neg_2",
                -37.4,
            ),
            (
                "element",
                SIGNATURE_LIST,
                "flipper_length_mm",
                1.5,
                "flipper_x_1_5",
                271.5,
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "flipper_length_mm",
                1.5,
                "flipper_x_1_5",
                271.5,
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "flipper_length_mm",
                1.5,
                "flipper_x_1_5",
                271.5,
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "flipper_length_mm",
                1.5,
                "flipper_x_1_5",
                271.5,
            ),
            (
                "element",
                SIGNATURE_LIST,
                "body_mass_g",
                0.5,
                "mass_half",
                1875.0,
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "body_mass_g",
                0.5,
                "mass_half",
                1875.0,
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "body_mass_g",
                0.5,
                "mass_half",
                1875.0,
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "body_mass_g",
                0.5,
                "mass_half",
                1875.0,
            ),
        ],
    )
    def test_scaler_udf_variations(
        self, udf_mode, signature_type, input_col, factor, out_col_name, expected_val
    ):
        if udf_mode == "element":
            if signature_type == SIGNATURE_LIST:

                class ScalerUDFList(UDFList):
                    def __init__(self, output_columns, i_factor: float):
                        super().__init__(output_columns)
                        self.factor = i_factor

                    def on_element(self, values: list) -> list:
                        if values[0] is None:
                            return [None]
                        return [values[0] * self.factor]

                scaler_udf = ScalerUDFList
            else:

                class ScalerUDFUnpacked(UDFUnpacked):
                    def __init__(self, output_columns, i_factor: float):
                        super().__init__(output_columns)
                        self.factor = i_factor

                    def on_element(self, value) -> list:
                        if value is None:
                            return [None]
                        return [value * self.factor]

                scaler_udf = ScalerUDFUnpacked
        else:
            if signature_type == SIGNATURE_LIST:

                class ScalerUDFList(UDFList):
                    def __init__(self, output_columns, i_factor: float):
                        super().__init__(output_columns)
                        self.factor = i_factor

                    def on_batch(self, series: list) -> list:
                        return [series[0] * self.factor]

                scaler_udf = ScalerUDFList
            else:

                class ScalerUDFUnpacked(UDFUnpacked):
                    def __init__(self, output_columns, i_factor: float):
                        super().__init__(output_columns)
                        self.factor = i_factor

                    def on_batch(self, series) -> list:
                        return [series * self.factor]

                scaler_udf = ScalerUDFUnpacked

        _, _, tf = load_normalized_complex_dataframe()

        scaler = scaler_udf([(out_col_name, pl.Float64)], i_factor=factor)

        result_tf = tf.udf(td.col(input_col), scaler)
        collected = result_tf._lf.collect()

        assert out_col_name in collected.columns
        assert collected[out_col_name][0] == pytest.approx(expected_val)

    @pytest.mark.parametrize(
        "udf_mode, signature_type, input_col, threshold, low, high, out_name,"
        " expected_val",
        [
            (
                "element",
                SIGNATURE_LIST,
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "body_mass_g",
                4000,
                "light",
                "heavy",
                "mass_cat",
                "light",
            ),
            (
                "element",
                SIGNATURE_LIST,
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "flipper_length_mm",
                200,
                "short",
                "long",
                "flipper_cat",
                "short",
            ),
            (
                "element",
                SIGNATURE_LIST,
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                "bill_depth_mm",
                18,
                "narrow",
                "wide",
                "bill_cat",
                "wide",
            ),
            (
                "element",
                SIGNATURE_LIST,
                "year",
                2008,
                "early",
                "late",
                "year_cat",
                "early",
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                "year",
                2008,
                "early",
                "late",
                "year_cat",
                "early",
            ),
            (
                "batch",
                SIGNATURE_LIST,
                "year",
                2008,
                "early",
                "late",
                "year_cat",
                "early",
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
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
        self,
        udf_mode,
        signature_type,
        input_col,
        threshold,
        low,
        high,
        out_name,
        expected_val,
    ):
        if udf_mode == "element":
            if signature_type == SIGNATURE_LIST:

                class CategoryUDFList(UDFList):
                    def __init__(
                        self,
                        output_columns,
                        i_threshold: float,
                        low_name: str,
                        high_name: str,
                    ):
                        super().__init__(output_columns)
                        self.threshold = i_threshold
                        self.low_name = low_name
                        self.high_name = high_name

                    def on_element(self, values: list) -> list:
                        if values[0] is None:
                            return [None]
                        return [
                            (
                                self.high_name
                                if values[0] > self.threshold
                                else self.low_name
                            )
                        ]

                category_udf = CategoryUDFList
            else:

                class CategoryUDFUnpacked(UDFUnpacked):
                    def __init__(
                        self,
                        output_columns,
                        i_threshold: float,
                        low_name: str,
                        high_name: str,
                    ):
                        super().__init__(output_columns)
                        self.threshold = i_threshold
                        self.low_name = low_name
                        self.high_name = high_name

                    def on_element(self, value) -> list:
                        if value is None:
                            return [None]
                        return [
                            self.high_name if value > self.threshold else self.low_name
                        ]

                category_udf = CategoryUDFUnpacked
        else:
            if signature_type == SIGNATURE_LIST:

                class CategoryUDFList(UDFList):
                    def __init__(
                        self,
                        output_columns,
                        i_threshold: float,
                        low_name: str,
                        high_name: str,
                    ):
                        super().__init__(output_columns)
                        self.threshold = i_threshold
                        self.low_name = low_name
                        self.high_name = high_name

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

                category_udf = CategoryUDFList
            else:

                class CategoryUDFUnpacked(UDFUnpacked):
                    def __init__(
                        self,
                        output_columns,
                        i_threshold: float,
                        low_name: str,
                        high_name: str,
                    ):
                        super().__init__(output_columns)
                        self.threshold = i_threshold
                        self.low_name = low_name
                        self.high_name = high_name

                    def on_batch(self, series) -> list:
                        return [
                            series.map_elements(
                                lambda x: (
                                    self.high_name
                                    if x is not None and x > self.threshold
                                    else self.low_name if x is not None else None
                                )
                            )
                        ]

                category_udf = CategoryUDFUnpacked

        _, _, tf = load_normalized_complex_dataframe()

        categorizer = category_udf([(out_name, td.String)], threshold, low, high)

        result_tf = tf.udf(td.col(input_col), categorizer)
        collected = result_tf._lf.collect()

        assert out_name in collected.columns
        assert collected[out_name][0] == expected_val

    @pytest.mark.parametrize(
        "udf_mode, signature_type, col_pair",
        [
            (
                "element",
                SIGNATURE_LIST,
                (
                    "bill_length_mm",
                    "bill_depth_mm",
                ),
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                (
                    "bill_length_mm",
                    "bill_depth_mm",
                ),
            ),
            (
                "batch",
                SIGNATURE_LIST,
                (
                    "bill_length_mm",
                    "bill_depth_mm",
                ),
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                (
                    "bill_length_mm",
                    "bill_depth_mm",
                ),
            ),
            (
                "element",
                SIGNATURE_LIST,
                (
                    "flipper_length_mm",
                    "body_mass_g",
                ),
            ),
            (
                "element",
                SIGNATURE_UNPACKED,
                (
                    "flipper_length_mm",
                    "body_mass_g",
                ),
            ),
            (
                "batch",
                SIGNATURE_LIST,
                (
                    "flipper_length_mm",
                    "body_mass_g",
                ),
            ),
            (
                "batch",
                SIGNATURE_UNPACKED,
                (
                    "flipper_length_mm",
                    "body_mass_g",
                ),
            ),
        ],
    )
    def test_multi_feature_udf_variations(self, udf_mode, signature_type, col_pair):
        if udf_mode == "element":
            if signature_type == SIGNATURE_LIST:

                class MultiFeatureUDFList(UDFList):
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

                multi_feature_udf = MultiFeatureUDFList
            else:

                class MultiFeatureUDFUnpacked(UDFUnpacked):
                    def on_element(self, val1, val2) -> list:
                        if val1 is None or val2 is None:
                            return [None, None, None, None]
                        return [
                            val1 + val2,
                            val1 - val2,
                            val1 * val2,
                            val1 / val2 if val2 != 0 else None,
                        ]

                multi_feature_udf = MultiFeatureUDFUnpacked
        else:
            if signature_type == SIGNATURE_LIST:

                class MultiFeatureUDFList(UDFList):
                    def on_batch(self, series: list) -> list:
                        s1, s2 = series[0], series[1]
                        div_series = (s1 / s2).replace(
                            [float("inf"), float("-inf")], None
                        )
                        return [
                            s1 + s2,
                            s1 - s2,
                            s1 * s2,
                            div_series,
                        ]

                multi_feature_udf = MultiFeatureUDFList
            else:

                class MultiFeatureUDFUnpacked(UDFUnpacked):
                    def on_batch(self, s1, s2) -> list:
                        div_series = (s1 / s2).replace(
                            [float("inf"), float("-inf")], None
                        )
                        return [
                            s1 + s2,
                            s1 - s2,
                            s1 * s2,
                            div_series,
                        ]

                multi_feature_udf = MultiFeatureUDFUnpacked

        _, _, tf = load_normalized_complex_dataframe()
        c1, c2 = col_pair

        schema = [
            (f"{c1}_{c2}_sum", td.Float64),
            (f"{c1}_{c2}_diff", td.Float64),
            (f"{c1}_{c2}_prod", td.Float64),
            (f"{c1}_{c2}_div", td.Float64),
        ]

        feature_creator = multi_feature_udf(schema)

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
        "signature_type, start_name1, start_name2",
        [
            (SIGNATURE_LIST, "feat1", "feat2"),
            (SIGNATURE_UNPACKED, "feat1", "feat2"),
            (SIGNATURE_LIST, "a", "b"),
            (SIGNATURE_UNPACKED, "a", "b"),
            (SIGNATURE_LIST, "x", "y"),
            (SIGNATURE_UNPACKED, "x", "y"),
            (SIGNATURE_LIST, "tmp1", "tmp2"),
            (SIGNATURE_UNPACKED, "tmp1", "tmp2"),
        ],
    )
    def test_schema_modification_variations(
        self, signature_type, start_name1, start_name2
    ):
        if signature_type == SIGNATURE_LIST:

            class PassthroughUDFList(UDFList):
                def on_batch(self, series: list) -> list:
                    return series

            passthrough_udf = PassthroughUDFList
        else:

            class PassthroughUDFUnpacked(UDFUnpacked):
                def on_batch(self, s1, s2) -> list:
                    return [s1, s2]

            passthrough_udf = PassthroughUDFUnpacked

        _, _, tf = load_normalized_complex_dataframe()

        passthrough = passthrough_udf(
            [(start_name1, td.Float64), (start_name2, td.Float64)]
        )

        passthrough = passthrough.with_columns(
            [(f"{start_name1}_renamed", td.String)]
        ).with_columns({1: (f"{start_name2}_final", None)})

        result_tf = tf.udf(td.col("bill_length_mm", "bill_depth_mm"), passthrough)
        collected = result_tf._lf.collect()

        assert f"{start_name1}_renamed" in collected.columns
        assert f"{start_name2}_final" in collected.columns
        assert collected[f"{start_name1}_renamed"].dtype == td.String
        assert collected[f"{start_name2}_final"].dtype == td.Float64

    @pytest.mark.parametrize(
        "signature_type, udf1_factor, udf2_threshold",
        [
            (SIGNATURE_LIST, 2.0, 8000),
            (SIGNATURE_UNPACKED, 2.0, 8000),
            (SIGNATURE_LIST, 0.5, 2000),
            (SIGNATURE_UNPACKED, 0.5, 2000),
            (SIGNATURE_LIST, 10, 40000),
            (SIGNATURE_UNPACKED, 10, 40000),
            (SIGNATURE_LIST, -1, -3000),
            (SIGNATURE_UNPACKED, -1, -3000),
        ],
    )
    def test_pipeline_variations(self, signature_type, udf1_factor, udf2_threshold):
        if signature_type == SIGNATURE_LIST:

            class PipelineScalerUDFList(UDFList):
                def __init__(self, output_columns, factor: float):
                    super().__init__(output_columns)
                    self.factor = factor

                def on_batch(self, series: list) -> list:
                    return [series[0] * self.factor]

            class PipelineCategoryUDFList(UDFList):
                def __init__(self, output_columns, threshold: float):
                    super().__init__(output_columns)
                    self.threshold = threshold

                def on_element(self, values: list) -> list:
                    if values[0] is None:
                        return [None]
                    return ["high" if values[0] > self.threshold else "low"]

            pipeline_scaler_udf = PipelineScalerUDFList
            pipeline_category_udf = PipelineCategoryUDFList
        else:

            class PipelineScalerUDFUnpacked(UDFUnpacked):
                def __init__(self, output_columns, factor: float):
                    super().__init__(output_columns)
                    self.factor = factor

                def on_batch(self, series) -> list:
                    return [series * self.factor]

            class PipelineCategoryUDFUnpacked(UDFUnpacked):
                def __init__(self, output_columns, threshold: float):
                    super().__init__(output_columns)
                    self.threshold = threshold

                def on_element(self, value) -> list:
                    if value is None:
                        return [None]
                    return ["high" if value > self.threshold else "low"]

            pipeline_scaler_udf = PipelineScalerUDFUnpacked
            pipeline_category_udf = PipelineCategoryUDFUnpacked

        _, _, tf = load_normalized_complex_dataframe()

        scaler = pipeline_scaler_udf([("mass_scaled", td.Float64)], factor=udf1_factor)
        categorizer = pipeline_category_udf(
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

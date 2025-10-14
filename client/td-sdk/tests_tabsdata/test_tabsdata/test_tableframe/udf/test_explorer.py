#
# Copyright 2025 Tabs Data Inc.
#

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
import pytest
from polars._typing import PolarsDataType

import tabsdata as td
import tabsdata.tableframe.typing as td_typing
from tabsdata.tableframe.udf.function import UDF
from tests_tabsdata.test_tabsdata.test_tableframe.common import (
    load_normalized_complex_dataframe,
)


@dataclass(frozen=True)
class OperationSpec:
    factory: Callable[[], UDF]
    expr: Tuple[str, ...]
    base_names: Tuple[str, ...]
    base_dtypes: Tuple[PolarsDataType, ...]
    expected_row: Callable[[pl.DataFrame], List[Any]]


@dataclass(frozen=True)
class OverrideCase:
    case_id: str
    method: str
    payload: Any


def apply_override(
    base_names: Tuple[str, ...],
    base_dtypes: Tuple[PolarsDataType, ...],
    method: str,
    payload: Any,
) -> Tuple[List[str], List[PolarsDataType]]:
    names = list(base_names)
    dtypes = list(base_dtypes)
    if method == "all":
        columns_in = payload
        if isinstance(columns_in, tuple):
            columns_in = [columns_in]
        overrides = list(columns_in)
        updated: List[Tuple[str, PolarsDataType]] = []
        for idx, (alias, dtype) in enumerate(overrides):
            alias_val = names[idx] if alias is None else alias
            dtype_val = dtypes[idx] if dtype is None else dtype
            updated.append((alias_val, dtype_val))
        if len(overrides) < len(names):
            updated.extend(zip(names[len(overrides) :], dtypes[len(overrides) :]))
        names = [alias for alias, _ in updated]
        dtypes = [dtype for _, dtype in updated]
    elif method == "some":
        for idx, (alias, dtype) in payload.items():
            if alias is not None:
                names[idx] = alias
            if dtype is not None:
                dtypes[idx] = dtype
    else:
        raise ValueError(f"Unknown override method: {method}")
    return names, dtypes


def cast_values(values: List[Any], dtypes: List[PolarsDataType]) -> List[Any]:
    casted: List[Any] = []
    for value, dtype in zip(values, dtypes):
        if value is None:
            casted.append(None)
            continue
        series = pl.Series([value])
        casted.append(series.cast(dtype, strict=False)[0])
    return casted


def compare_values(actual: Any, expected: Any, dtype: PolarsDataType) -> None:
    if expected is None:
        assert actual is None
        return
    if isinstance(dtype, (pl.Float32, pl.Float64)):
        assert actual == pytest.approx(expected)
    elif isinstance(
        dtype,
        (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ),
    ):
        assert actual == expected
    else:
        assert actual == expected


def build_cases(override_map: Dict[str, List[OverrideCase]]):
    cases = []
    for op_key, overrides in override_map.items():
        for override in overrides:
            cases.append(
                pytest.param(
                    op_key,
                    override.method,
                    override.payload,
                    id=f"{op_key}__{override.case_id}",
                )
            )
    return cases


def cast_scalar_to_dtype(value: Any, dtype: PolarsDataType | None) -> Any:
    if value is None or dtype is None:
        return value
    series = td_typing.Series([value])
    return series.cast(dtype, strict=False)[0]


class MassCenteringBatch(UDF):
    def __init__(self) -> None:
        super().__init__([("mass_centered_default", td.Float64)])

    def on_batch(self, series: List[td_typing.Series]) -> List[td_typing.Series]:
        mass = series[0].cast(td.Float64)
        mean_value = mass.mean()
        centered = mass - mean_value
        return [centered]


class FlipperBucketElement(UDF):
    def __init__(self) -> None:
        super().__init__([("flipper_label_default", td.Utf8)])

    def on_element(self, values: List[Any]) -> List[Any]:
        length = values[0]
        if length is None:
            return [None]
        if length >= 215:
            return ["giant"]
        if length >= 195:
            return ["long"]
        return ["standard"]


class BillRatioElement(UDF):
    def __init__(self) -> None:
        super().__init__([("bill_ratio_default", td.Float64)])

    def on_element(self, values: List[Any]) -> List[Any]:
        length, depth = values
        if length is None or depth is None or depth == 0:
            return [None]
        ratio = length / depth
        dtype = self._schema.columns[0].dtype
        return [cast_scalar_to_dtype(ratio, dtype)]


class BillRatioBatch(UDF):
    def __init__(self) -> None:
        super().__init__([("bill_ratio_default_batch", td.Float64)])

    def on_batch(self, series: List[td_typing.Series]) -> List[td_typing.Series]:
        length = series[0].cast(td.Float64)
        depth = series[1].cast(td.Float64)
        ratio = length / depth
        return [ratio]


class BillSumDiffElement(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("bill_sum_default", td.Float64),
                ("bill_gap_default", td.Float64),
            ]
        )

    def on_element(self, values: List[Any]) -> List[Any]:
        length, depth = values
        if length is None or depth is None:
            return [None, None]
        schema_columns = list(self._schema)
        bill_sum = cast_scalar_to_dtype(length + depth, schema_columns[0].dtype)
        bill_gap = cast_scalar_to_dtype(length - depth, schema_columns[1].dtype)
        return [bill_sum, bill_gap]


class BillSumDiffBatch(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("bill_sum_default", td.Float64),
                ("bill_gap_default", td.Float64),
            ]
        )

    def on_batch(self, series: List[td_typing.Series]) -> List[td_typing.Series]:
        length = series[0].cast(td.Float64)
        depth = series[1].cast(td.Float64)
        bill_sum = length + depth
        bill_gap = length - depth
        return [bill_sum, bill_gap]


class IslandYearElement(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("island_year_default", td.Utf8),
                ("is_recent_default", td.Boolean),
            ]
        )

    def on_element(self, values: List[Any]) -> List[Any]:
        island, year = values
        if island is None or year is None:
            return [None, None]
        tag = f"{island.lower()}_{year}"
        return [tag, year >= 2008]


class MassFlipperBatch(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("mass_per_flipper_default", td.Float64),
                ("is_massive_default", td.Boolean),
            ]
        )

    def on_batch(self, series: List[td_typing.Series]) -> List[td_typing.Series]:
        mass = series[0].cast(td.Float64)
        flipper = series[1].cast(td.Float64)
        ratio = mass / flipper
        mean_mass = mass.mean()
        heavy = mass > mean_mass
        return [ratio, heavy]


class BodyCompositeBatch(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("mass_density_default", td.Float64),
                ("bill_sum_default", td.Float64),
                ("density_flag_default", td.Boolean),
            ]
        )

    def on_batch(self, series: List[td_typing.Series]) -> List[td_typing.Series]:
        mass = series[0].cast(td.Float64)
        length = series[1].cast(td.Float64)
        depth = series[2].cast(td.Float64)
        density = mass / (length * depth)
        bill_sum = length + depth
        density_mean = density.mean()
        density_flag = density > density_mean
        return [density, bill_sum, density_flag]


class SpeciesInsightsElement(UDF):
    def __init__(self) -> None:
        super().__init__(
            [
                ("species_tag_default", td.Utf8),
                ("is_gentoo_default", td.Boolean),
                ("species_name_length_default", td.Int64),
            ]
        )

    def on_element(self, values: List[Any]) -> List[Any]:
        species, island, sex = values
        if species is None:
            return [None, None, None]
        parts = [species.lower()]
        if island is not None:
            parts.append(str(island).lower())
        parts.append(str(sex).lower() if sex is not None else "unknown")
        tag = "-".join(parts)
        is_gentoo = species.lower() == "gentoo"
        name_length = len(species)
        schema_columns = list(self._schema)
        tag = cast_scalar_to_dtype(tag, schema_columns[0].dtype)
        is_gentoo = cast_scalar_to_dtype(is_gentoo, schema_columns[1].dtype)
        name_length = cast_scalar_to_dtype(name_length, schema_columns[2].dtype)
        return [tag, is_gentoo, name_length]


def expected_mass_center_batch(df: pl.DataFrame) -> List[Any]:
    mass = df["body_mass_g"].cast(td.Float64)
    centered = mass - mass.mean()
    return [centered[0]]


def expected_flipper_bucket_element(df: pl.DataFrame) -> List[Any]:
    length = df["flipper_length_mm"][0]
    if length is None:
        return [None]
    if length >= 215:
        return ["giant"]
    if length >= 195:
        return ["long"]
    return ["standard"]


def expected_bill_ratio(df: pl.DataFrame) -> List[Any]:
    length_series = df["bill_length_mm"].cast(td.Float64)
    depth_series = df["bill_depth_mm"].cast(td.Float64)
    ratio = length_series / depth_series
    return [ratio[0]]


def expected_bill_sum_diff(df: pl.DataFrame) -> List[Any]:
    length_series = df["bill_length_mm"].cast(td.Float64)
    depth_series = df["bill_depth_mm"].cast(td.Float64)
    bill_sum = length_series + depth_series
    bill_gap = length_series - depth_series
    return [bill_sum[0], bill_gap[0]]


def expected_island_year_element(df: pl.DataFrame) -> List[Any]:
    island = df["island"][0]
    year = df["year"][0]
    if island is None or year is None:
        return [None, None]
    tag = f"{island.lower()}_{year}"
    return [tag, year >= 2008]


def expected_mass_flipper_batch(df: pl.DataFrame) -> List[Any]:
    mass = df["body_mass_g"].cast(td.Float64)
    flipper = df["flipper_length_mm"].cast(td.Float64)
    ratio = mass / flipper
    heavy = mass > mass.mean()
    return [ratio[0], heavy[0]]


def expected_body_composite_batch(df: pl.DataFrame) -> List[Any]:
    mass = df["body_mass_g"].cast(td.Float64)
    length = df["bill_length_mm"].cast(td.Float64)
    depth = df["bill_depth_mm"].cast(td.Float64)
    density = mass / (length * depth)
    bill_sum = length + depth
    density_flag = density > density.mean()
    return [density[0], bill_sum[0], density_flag[0]]


def expected_species_insights_element(df: pl.DataFrame) -> List[Any]:
    species = df["species"][0]
    island = df["island"][0]
    sex = df["sex"][0]
    if species is None:
        return [None, None, None]
    parts = [species.lower()]
    if island is not None:
        parts.append(str(island).lower())
    parts.append(str(sex).lower() if sex is not None else "unknown")
    tag = "-".join(parts)
    is_gentoo = species.lower() == "gentoo"
    name_length = len(species)
    return [tag, is_gentoo, name_length]


OPERATION_SPECS: Dict[str, OperationSpec] = {
    "mass_center_batch": OperationSpec(
        factory=MassCenteringBatch,
        expr=("body_mass_g",),
        base_names=("mass_centered_default",),
        base_dtypes=(td.Float64,),
        expected_row=expected_mass_center_batch,
    ),
    "flipper_bucket_element": OperationSpec(
        factory=FlipperBucketElement,
        expr=("flipper_length_mm",),
        base_names=("flipper_label_default",),
        base_dtypes=(td.Utf8,),
        expected_row=expected_flipper_bucket_element,
    ),
    "bill_ratio_element": OperationSpec(
        factory=BillRatioElement,
        expr=("bill_length_mm", "bill_depth_mm"),
        base_names=("bill_ratio_default",),
        base_dtypes=(td.Float64,),
        expected_row=expected_bill_ratio,
    ),
    "bill_ratio_batch": OperationSpec(
        factory=BillRatioBatch,
        expr=("bill_length_mm", "bill_depth_mm"),
        base_names=("bill_ratio_default_batch",),
        base_dtypes=(td.Float64,),
        expected_row=expected_bill_ratio,
    ),
    "bill_sum_diff_element": OperationSpec(
        factory=BillSumDiffElement,
        expr=("bill_length_mm", "bill_depth_mm"),
        base_names=("bill_sum_default", "bill_gap_default"),
        base_dtypes=(td.Float64, td.Float64),
        expected_row=expected_bill_sum_diff,
    ),
    "bill_sum_diff_batch": OperationSpec(
        factory=BillSumDiffBatch,
        expr=("bill_length_mm", "bill_depth_mm"),
        base_names=("bill_sum_default", "bill_gap_default"),
        base_dtypes=(td.Float64, td.Float64),
        expected_row=expected_bill_sum_diff,
    ),
    "island_year_element": OperationSpec(
        factory=IslandYearElement,
        expr=("island", "year"),
        base_names=("island_year_default", "is_recent_default"),
        base_dtypes=(td.Utf8, td.Boolean),
        expected_row=expected_island_year_element,
    ),
    "mass_flipper_batch": OperationSpec(
        factory=MassFlipperBatch,
        expr=("body_mass_g", "flipper_length_mm"),
        base_names=("mass_per_flipper_default", "is_massive_default"),
        base_dtypes=(td.Float64, td.Boolean),
        expected_row=expected_mass_flipper_batch,
    ),
    "body_composite_batch": OperationSpec(
        factory=BodyCompositeBatch,
        expr=("body_mass_g", "bill_length_mm", "bill_depth_mm"),
        base_names=(
            "mass_density_default",
            "bill_sum_default",
            "density_flag_default",
        ),
        base_dtypes=(td.Float64, td.Float64, td.Boolean),
        expected_row=expected_body_composite_batch,
    ),
    "species_insights_element": OperationSpec(
        factory=SpeciesInsightsElement,
        expr=("species", "island", "sex"),
        base_names=(
            "species_tag_default",
            "is_gentoo_default",
            "species_name_length_default",
        ),
        base_dtypes=(td.Utf8, td.Boolean, td.Int64),
        expected_row=expected_species_insights_element,
    ),
}


SINGLE_COLUMN_OVERRIDES: Dict[str, List[OverrideCase]] = {
    "mass_center_batch": [
        OverrideCase("all_alias", "all", [("body_mass_centered", None)]),
        OverrideCase("all_dtype_float32", "all", [(None, td.Float32)]),
        OverrideCase("all_alias_dtype_int", "all", [("mass_centered_i64", td.Int64)]),
        OverrideCase("some_alias", "some", {0: ("mass_center_alias", None)}),
        OverrideCase("some_dtype_utf8", "some", {0: (None, td.Utf8)}),
    ],
    "flipper_bucket_element": [
        OverrideCase("all_alias", "all", [("flipper_bucket", None)]),
        OverrideCase("all_dtype_categorical", "all", [(None, td.Categorical)]),
        OverrideCase(
            "all_alias_dtype_categorical",
            "all",
            [("flipper_bucket_cat", td.Categorical)],
        ),
        OverrideCase("some_alias", "some", {0: ("bucket_alias_some", None)}),
        OverrideCase("some_dtype_categorical", "some", {0: (None, td.Categorical)}),
    ],
    "bill_ratio_element": [
        OverrideCase(
            "all_alias",
            "all",
            [("bill_ratio_length_depth", None)],
        ),
        OverrideCase("all_dtype_float32", "all", [(None, td.Float32)]),
        OverrideCase("all_alias_dtype_string", "all", [("bill_ratio_text", td.Utf8)]),
        OverrideCase("some_alias", "some", {0: ("bill_ratio_some", None)}),
        OverrideCase("some_dtype_int", "some", {0: (None, td.Int32)}),
    ],
    "bill_ratio_batch": [
        OverrideCase("all_alias", "all", [("bill_ratio_batch_alias", None)]),
        OverrideCase("all_tuple_alias", "all", ("bill_ratio_from_tuple", None)),
        OverrideCase("all_dtype_float64", "all", [(None, td.Float64)]),
        OverrideCase("some_alias", "some", {0: ("bill_ratio_batch_some", None)}),
        OverrideCase("some_dtype_string", "some", {0: (None, td.Utf8)}),
    ],
}


TWO_COLUMN_OVERRIDES: Dict[str, List[OverrideCase]] = {
    "bill_sum_diff_element": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("bill_total", None),
                ("bill_gap", None),
            ],
        ),
        OverrideCase(
            "all_dtype_float32",
            "all",
            [
                (None, td.Float32),
                (None, td.Float32),
            ],
        ),
        OverrideCase(
            "all_partial_alias",
            "all",
            [("bill_total_alias", td.Float64)],
        ),
        OverrideCase(
            "some_second_alias",
            "some",
            {1: ("difference_alias", None)},
        ),
        OverrideCase(
            "some_both_int",
            "some",
            {0: (None, td.Int64), 1: ("diff_i64", td.Int64)},
        ),
    ],
    "bill_sum_diff_batch": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("bill_total_batch", None),
                ("bill_gap_batch", None),
            ],
        ),
        OverrideCase(
            "all_dtype_mix",
            "all",
            [
                (None, td.Float32),
                ("bill_gap_alias", None),
            ],
        ),
        OverrideCase("all_partial_dtype", "all", [(None, td.Float64)]),
        OverrideCase(
            "some_first_alias_dtype",
            "some",
            {0: ("bill_total_cast", td.Float32)},
        ),
        OverrideCase(
            "some_second_string",
            "some",
            {1: ("bill_gap_text", td.Utf8)},
        ),
    ],
    "island_year_element": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("island_year", None),
                ("recent_flag", None),
            ],
        ),
        OverrideCase(
            "all_dtype_mix",
            "all",
            [
                (None, td.Categorical),
                ("recent_flag_bool", td.Boolean),
            ],
        ),
        OverrideCase("all_partial_alias", "all", [("island_year_alias", None)]),
        OverrideCase(
            "some_second_int",
            "some",
            {1: ("recent_flag_int", td.Int8)},
        ),
        OverrideCase(
            "some_first_alias",
            "some",
            {0: ("island_year_some", None)},
        ),
    ],
    "mass_flipper_batch": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("mass_flipper_ratio", None),
                ("mass_above_mean", None),
            ],
        ),
        OverrideCase(
            "all_dtype_mix",
            "all",
            [
                (None, td.Float32),
                ("mass_above_mean_flag", td.Boolean),
            ],
        ),
        OverrideCase(
            "all_partial_alias",
            "all",
            [("mass_flipper_ratio_alias", None)],
        ),
        OverrideCase("some_second_int", "some", {1: (None, td.Int8)}),
        OverrideCase(
            "some_first_string",
            "some",
            {0: ("mass_flipper_string", td.Utf8)},
        ),
    ],
}


THREE_COLUMN_OVERRIDES: Dict[str, List[OverrideCase]] = {
    "body_composite_batch": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("mass_density", None),
                ("bill_sum", None),
                ("density_flag", None),
            ],
        ),
        OverrideCase(
            "all_dtype_mix",
            "all",
            [
                (None, td.Float32),
                ("bill_sum_alias", td.Float64),
                (None, td.Boolean),
            ],
        ),
        OverrideCase(
            "all_partial",
            "all",
            [
                (None, td.Float32),
                ("bill_sum_partial", None),
            ],
        ),
        OverrideCase(
            "some_third_int",
            "some",
            {2: ("density_flag_int", td.Int8)},
        ),
        OverrideCase(
            "some_second_string",
            "some",
            {1: ("bill_sum_string", td.Utf8)},
        ),
        OverrideCase(
            "some_multi",
            "some",
            {0: ("mass_density_custom", None), 2: (None, td.Boolean)},
        ),
    ],
    "species_insights_element": [
        OverrideCase(
            "all_alias",
            "all",
            [
                ("species_tag", None),
                ("gentoo_flag", None),
                ("species_name_length", None),
            ],
        ),
        OverrideCase(
            "all_dtype_mix",
            "all",
            [
                (None, td.Categorical),
                ("gentoo_flag_int", td.Int8),
                (None, td.Int32),
            ],
        ),
        OverrideCase(
            "all_partial",
            "all",
            [
                ("species_tag_alias", None),
                (None, None),
            ],
        ),
        OverrideCase(
            "some_first_alias_third_uint",
            "some",
            {0: ("species_tag_some", None), 2: (None, td.UInt16)},
        ),
        OverrideCase(
            "some_second_string",
            "some",
            {
                1: ("gentoo_flag_text", td.Utf8),
                2: ("species_length_text", td.Utf8),
            },
        ),
        OverrideCase(
            "some_all_strings",
            "some",
            {
                0: (None, td.Utf8),
                1: (None, td.Utf8),
                2: (None, td.Utf8),
            },
        ),
    ],
}


SINGLE_COLUMN_CASES = build_cases(SINGLE_COLUMN_OVERRIDES)
TWO_COLUMN_CASES = build_cases(TWO_COLUMN_OVERRIDES)
THREE_COLUMN_CASES = build_cases(THREE_COLUMN_OVERRIDES)


@pytest.fixture(scope="module")
def penguin_data_frame() -> pl.DataFrame:
    _, data_frame, _ = load_normalized_complex_dataframe()
    return data_frame


@pytest.fixture
def penguin_table_frame(
    penguin_data_frame: pl.DataFrame,
) -> Tuple[pl.DataFrame, td.TableFrame]:
    table_frame = td.TableFrame.__build__(
        df=penguin_data_frame.lazy(),
        mode="raw",
        idx=0,
    )
    return penguin_data_frame, table_frame


def execute_udf_test(
    penguin_table_frame: Tuple[pl.DataFrame, td.TableFrame],
    operation_key: str,
    method: str,
    payload: Any,
) -> None:
    data_frame, table_frame = penguin_table_frame
    baseline_df = table_frame
    baseline_columns = set(baseline_df._lf.columns)
    spec = OPERATION_SPECS[operation_key]
    udf_instance = spec.factory()
    udf_function = (
        udf_instance.output_columns(payload)
        if method == "all"
        else udf_instance.output_columns(payload)
    )
    column_expr = td.col(*spec.expr)
    result_tf = table_frame.udf(column_expr, udf_function)
    result_df = result_tf._lf.collect()

    expected_names, expected_dtypes = apply_override(
        spec.base_names, spec.base_dtypes, method, payload
    )

    new_columns = []
    for name in result_df.columns:
        if name in baseline_columns:
            continue
        if name.startswith("$td."):
            continue
        new_columns.append(name)

    assert new_columns == expected_names

    schema = result_df.schema
    for name, dtype in zip(expected_names, expected_dtypes):
        assert schema[name] == dtype

    base_values = spec.expected_row(data_frame)
    casted_values = cast_values(base_values, expected_dtypes)
    actual_values = [result_df[name][0] for name in expected_names]

    for actual, expected, dtype in zip(actual_values, casted_values, expected_dtypes):
        compare_values(actual, expected, dtype)


@pytest.mark.parametrize("operation_key, method, payload", SINGLE_COLUMN_CASES)
def test_single_column_udf_schema_variants(
    penguin_table_frame: Tuple[pl.DataFrame, td.TableFrame],
    operation_key: str,
    method: str,
    payload: Any,
) -> None:
    execute_udf_test(penguin_table_frame, operation_key, method, payload)


@pytest.mark.parametrize("operation_key, method, payload", TWO_COLUMN_CASES)
def test_two_column_udf_schema_variants(
    penguin_table_frame: Tuple[pl.DataFrame, td.TableFrame],
    operation_key: str,
    method: str,
    payload: Any,
) -> None:
    execute_udf_test(penguin_table_frame, operation_key, method, payload)


@pytest.mark.parametrize("operation_key, method, payload", THREE_COLUMN_CASES)
def test_three_column_udf_schema_variants(
    penguin_table_frame: Tuple[pl.DataFrame, td.TableFrame],
    operation_key: str,
    method: str,
    payload: Any,
) -> None:
    execute_udf_test(penguin_table_frame, operation_key, method, payload)

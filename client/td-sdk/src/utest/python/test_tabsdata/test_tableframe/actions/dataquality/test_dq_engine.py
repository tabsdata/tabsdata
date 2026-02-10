#
# Copyright 2025 Tabs Data Inc.
#

import math
from decimal import Decimal

import polars as pl
import pytest

import tabsdata as td
import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata.dataquality import (
    ExponentialScale,
    IdentityScale,
    IsBetween,
    IsIn,
    IsNotBetween,
    IsNotIn,
    LinearScale,
    LogarithmicScale,
    MonomialScale,
    ScaleCategorizer,
)
from tabsdata.exceptions import DataQualityProcessingError, ErrorCode
from tabsdata.tableframe._actions.dataquality.engine import (
    ByteSentinel,
    DataQualityEngine,
)
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import (
    CLASSIFIERS,
    DATASET_ROWS,
    NaN,
    boolean_dataset,
)
from test_tabsdata.test_tableframe.common import (
    pretty_pandas,
    pretty_polars,
)

pretty_polars()
pretty_pandas()
pl.Config.set_verbose(False)


def _convert_classifier_values(classifier, dtype):
    if not isinstance(classifier, (IsIn, IsNotIn, IsBetween, IsNotBetween)):
        return classifier
    if dtype not in DATASET_ROWS:
        return classifier

    converter = DATASET_ROWS[dtype].converter

    is_uint = dtype in (
        td_dtypes.UInt8,
        td_dtypes.UInt16,
        td_dtypes.UInt32,
        td_dtypes.UInt64,
    )

    if isinstance(classifier, (IsIn, IsNotIn)):
        converted_values = set()
        for value in classifier.values:
            if value is None:
                converted_values.add(None)
            elif isinstance(value, float) and math.isnan(value):
                converted_values.add(value)
            elif is_uint and value == NaN:
                converted_values.add(None)
            elif is_uint and isinstance(value, (int, float)):
                converted_values.add(int(value))
            else:
                converted_values.add(converter(value))

        if isinstance(classifier, IsIn):
            return IsIn(
                column_names=classifier.column_names,
                tags=classifier.tags,
                values=frozenset(converted_values),
            )
        else:
            return IsNotIn(
                column_names=classifier.column_names,
                tags=classifier.tags,
                values=frozenset(converted_values),
            )
    elif isinstance(classifier, (IsBetween, IsNotBetween)):
        if is_uint:
            if classifier.min is not None:
                if classifier.min == NaN:
                    converted_min = None
                elif isinstance(classifier.min, (int, float)):
                    converted_min = int(classifier.min)
                else:
                    converted_min = converter(classifier.min)
            else:
                converted_min = None

            if classifier.max is not None:
                if classifier.max == NaN:
                    converted_max = None
                elif isinstance(classifier.max, (int, float)):
                    converted_max = int(classifier.max)
                else:
                    converted_max = converter(classifier.max)
            else:
                converted_max = None
        else:
            converted_min = (
                converter(classifier.min) if classifier.min is not None else None
            )
            converted_max = (
                converter(classifier.max) if classifier.max is not None else None
            )

        if isinstance(classifier, IsBetween):
            return IsBetween(
                column_names=classifier.column_names,
                tags=classifier.tags,
                min_val=converted_min,
                max_val=converted_max,
                closed_on=classifier.closed_on,
            )
        else:
            return IsNotBetween(
                column_names=classifier.column_names,
                tags=classifier.tags,
                min_val=converted_min,
                max_val=converted_max,
                closed_on=classifier.closed_on,
            )
    return None


def dq_engine_parameters():
    parameters = []
    for classifier, configs in CLASSIFIERS.items():
        for config in configs:
            for dtype in sorted(config.dtypes, key=lambda t: t.__name__):
                if config.name is not None:
                    config_id = f"{classifier.__name__}_{dtype.__name__}_{config.name}"
                else:
                    config_id = f"{classifier.__name__}_{dtype.__name__}"
                parameters.append(
                    pytest.param(
                        classifier,
                        dtype,
                        config,
                        id=config_id,
                    )
                )
    return parameters


@pytest.mark.dq
@pytest.mark.parametrize("classifier,dtype,configuration", dq_engine_parameters())
def test_dq_engine(classifier, dtype, configuration):
    dataset = (
        configuration.dataset if configuration.dataset is not None else boolean_dataset
    )
    tf = dataset(dtype)
    qrows = len(tf._lf.collect())
    dq_engine = tf._dq
    for classifier in configuration.classifiers:
        converted_classifier = _convert_classifier_values(classifier, dtype)
        dq_engine = configuration.method(dq_engine, converted_classifier)
    assert isinstance(dq_engine, DataQualityEngine)
    df = dq_engine.tf()._lf.collect()
    configuration.checker(df, dq_engine._tags, qrows)
    assert "value" in df.columns
    assert "id" in df.columns


def dq_negative_parameters():
    supported_dtypes = list(ScaleCategorizer.supported_dtypes())
    signed_dtypes = [
        dtype
        for dtype in supported_dtypes
        if dtype
        not in [
            td_dtypes.UInt8,
            td_dtypes.UInt16,
            td_dtypes.UInt32,
            td_dtypes.UInt64,
        ]
    ]
    signed_dtypes = sorted(signed_dtypes, key=lambda t: t.__name__)

    test_cases = []

    for dtype in signed_dtypes:
        test_cases.append(
            pytest.param(
                MonomialScale(
                    min_val=-4,
                    max_val=16,
                    power=2,
                    bins=5,
                    use_bin_zero=False,
                ),
                dtype,
                id=f"monomial_quadratic_neg_min_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                MonomialScale(
                    min_val=-4,
                    max_val=16,
                    power=2,
                    bins=5,
                    use_bin_zero=True,
                ),
                dtype,
                id=f"monomial_quadratic_neg_min_with_bin_zero_{dtype.__name__}",
            )
        )

    for dtype in signed_dtypes:
        test_cases.append(
            pytest.param(
                ExponentialScale(
                    min_val=-5,
                    max_val=10,
                    base=math.e,
                    bins=5,
                    use_bin_zero=False,
                ),
                dtype,
                id=f"exponential_neg_min_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                ExponentialScale(
                    min_val=-5,
                    max_val=10,
                    base=math.e,
                    bins=5,
                    use_bin_zero=True,
                ),
                dtype,
                id=f"exponential_neg_min_with_bin_zero_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                IdentityScale(
                    min_val=1,
                    max_val=100,
                ),
                dtype,
                id=f"identity_neg_min_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                LinearScale(
                    min_val=-5,
                    max_val=10,
                    bins=5,
                    use_bin_zero=False,
                ),
                dtype,
                id=f"lineal_neg_min_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                LinearScale(
                    min_val=-5,
                    max_val=10,
                    bins=5,
                    use_bin_zero=True,
                ),
                dtype,
                id=f"lineal_neg_min_with_bin_zero_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                LogarithmicScale(
                    min_val=-5,
                    max_val=10,
                    base=math.e,
                    bins=5,
                    use_bin_zero=False,
                ),
                dtype,
                id=f"logarithmic_neg_min_{dtype.__name__}",
            )
        )
        test_cases.append(
            pytest.param(
                LogarithmicScale(
                    min_val=-5,
                    max_val=10,
                    base=math.e,
                    bins=5,
                    use_bin_zero=True,
                ),
                dtype,
                id=f"logarithmic_neg_min_with_bin_zero_{dtype.__name__}",
            )
        )

    return test_cases


@pytest.mark.dq
@pytest.mark.parametrize("scale,dtype", dq_negative_parameters())
def test_categorizer_with_negative_values(scale, dtype):
    minimum, maximum = scale.scale_range

    values = []

    is_int_dtype = dtype in [
        td_dtypes.Int8,
        td_dtypes.Int16,
        td_dtypes.Int32,
        td_dtypes.Int64,
    ]
    is_decimal_dtype = dtype == td_dtypes.Decimal

    def normalize(value):
        if value is None:
            return None
        if is_int_dtype:
            return int(round(value))
        if is_decimal_dtype:
            return Decimal(str(value)).quantize(Decimal("0.01"))
        return value

    values.extend([normalize(minimum - 10), normalize(maximum + 10)])
    values.extend([normalize(minimum), normalize(maximum)])
    values.extend(
        [
            normalize(minimum + (maximum - minimum) * 0.1),
            normalize(minimum + (maximum - minimum) * 0.25),
            normalize(minimum + (maximum - minimum) * 0.5),
            normalize(minimum + (maximum - minimum) * 0.75),
            normalize(minimum + (maximum - minimum) * 0.9),
        ]
    )
    values.append(None)

    schema = {"id": pl.Int64, "value": dtype}

    data = {"id": list(range(len(values))), "value": values}
    df = pl.DataFrame(data, schema=schema, strict=False)
    if dtype in [td_dtypes.Float32, td_dtypes.Float64]:
        nan_data = {"id": [len(values)], "value": [float("nan")]}
        nan_df = pl.DataFrame(nan_data, schema=schema, strict=False)
        df = df.vstack(nan_df)
    tf = td.TableFrame.from_polars(df)

    categorizer = ScaleCategorizer(
        column_names=("value", "value_category"),
        scale=scale,
    )

    dq_engine = tf._dq.categorizer(categorizer)
    df = dq_engine.tf()._lf.collect()

    assert "value_category" in df.columns
    assert df.schema["value_category"] == pl.UInt8

    expected_count = len(values) + (
        1 if dtype in [td_dtypes.Float32, td_dtypes.Float64] else 0
    )
    assert len(df) == expected_count

    null_mask = df["value"].is_null()
    null_categories = df.filter(null_mask)["value_category"]
    assert null_categories.to_list() == [ByteSentinel.NULL]

    if dtype in [td_dtypes.Float32, td_dtypes.Float64]:
        nan_mask = df["value"].is_nan()
        nan_categories = df.filter(nan_mask)["value_category"]
        assert nan_categories.to_list() == [ByteSentinel.NAN]

    underflow_mask = (df["value"].is_not_null()) & (df["value"] < minimum)
    if dtype in [td_dtypes.Float32, td_dtypes.Float64]:
        underflow_mask = underflow_mask & df["value"].is_not_nan()
    underflow_categories = df.filter(underflow_mask)["value_category"]
    assert all(c == ByteSentinel.UNDERFLOW for c in underflow_categories.to_list())

    overflow_mask = (df["value"].is_not_null()) & (df["value"] > maximum)
    if dtype in [td_dtypes.Float32, td_dtypes.Float64]:
        overflow_mask = overflow_mask & df["value"].is_not_nan()
    overflow_categories = df.filter(overflow_mask)["value_category"]
    assert all(c == ByteSentinel.OVERFLOW for c in overflow_categories.to_list())

    valid_mask = (
        df["value"].is_not_null() & (df["value"] >= minimum) & (df["value"] <= maximum)
    )
    if dtype in [td_dtypes.Float32, td_dtypes.Float64]:
        valid_mask = valid_mask & df["value"].is_not_nan()

    valid_categories = df.filter(valid_mask)["value_category"]

    if hasattr(scale, "use_bin_zero") and scale.use_bin_zero:
        expected_range = range(0, scale.bins + 1)
    else:
        expected_range = range(1, scale.bins + 1)

    for cat in valid_categories.to_list():
        assert (
            cat in expected_range
        ), f"Category {cat} not in expected range {list(expected_range)}"

    if hasattr(scale, "use_bin_zero") and scale.use_bin_zero:
        min_value_mask = df["value"] == minimum
        min_categories = df.filter(min_value_mask)["value_category"]
        assert all(
            category == 0 for category in min_categories.to_list()
        ), "Minimum value should be in bin 0 when use_bin_zero=True"


def wrong_value_dataset() -> td.TableFrame:
    schema = {
        "id": pl.Int64,
        "int_col": td_dtypes.Int32,
        "float_col": td_dtypes.Float64,
        "str_col": td_dtypes.String,
    }
    data = {
        "id": list(range(10)),
        "int_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        "str_col": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    }
    df = pl.DataFrame(data, schema=schema, strict=False)
    lf = df.lazy()
    tf = td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=0,
        properties=TableFramePropertiesBuilder.empty(),
    )
    return tf


@pytest.mark.dq
def test_on_wrong_value_ignore_is_between():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsBetween(
        column_names=["int_col"],
        min_val="invalid_string",
        max_val="valid_string_larger",
        on_wrong_value="ignore",
    )
    dq = dq_engine.is_between(classifier)
    df = dq.tf()._lf.collect()
    assert "int_col_is_between" not in df.columns
    assert "int_col" in df.columns


@pytest.mark.dq
def test_on_wrong_value_fail_is_between_min():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsBetween(
        column_names=["int_col"],
        min_val="invalid_string",
        max_val="valid_string_larger",
        on_wrong_value="fail",
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        dq_engine.is_between(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "int_col" in exc_info.value.message
    assert "min_val" in exc_info.value.message
    assert "invalid_string" in exc_info.value.message
    assert "Int32" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_fail_is_between_max():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsBetween(
        column_names=["float_col"],
        min_val="not_a_number",
        max_val="really_not_a_number",
        on_wrong_value="fail",
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        dq_engine.is_between(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "float_col" in exc_info.value.message
    assert "min_val" in exc_info.value.message
    assert "not_a_number" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_ignore_is_not_between():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsNotBetween(
        column_names=["int_col"],
        min_val="bad_value",
        max_val="good_value_larger",
        on_wrong_value="ignore",
    )
    dq = dq_engine.is_not_between(classifier)
    df = dq.tf()._lf.collect()
    assert "int_col_is_not_between" not in df.columns


@pytest.mark.dq
def test_on_wrong_value_fail_is_not_between():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsNotBetween(
        column_names=["int_col"],
        min_val="invalid_max",
        max_val="valid_max_larger",
        on_wrong_value="fail",
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        dq_engine.is_not_between(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "int_col" in exc_info.value.message
    assert "min_val" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_ignore_is_in():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsIn(
        column_names=["int_col"],
        values=frozenset(["abc", "def", "ghi"]),
        on_wrong_value="ignore",
    )
    dq = dq_engine.is_in(classifier)
    df = dq.tf()._lf.collect()
    assert "int_col_is_in" not in df.columns


@pytest.mark.dq
def test_on_wrong_value_fail_is_in():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsIn(
        column_names=["int_col"],
        values=frozenset(["not", "numbers"]),
        on_wrong_value="fail",
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        dq_engine.is_in(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "int_col" in exc_info.value.message
    assert "values" in exc_info.value.message


@pytest.mark.dq
def test_on_wrong_value_ignore_is_not_in():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsNotIn(
        column_names=["float_col"],
        values=frozenset(["invalid", "float", "values"]),
        on_wrong_value="ignore",
    )
    result_dq = dq_engine.is_not_in(classifier)
    result_df = result_dq.tf()._lf.collect()
    assert "float_col_is_not_in" not in result_df.columns


@pytest.mark.dq
def test_on_wrong_value_fail_is_not_in():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsNotIn(
        column_names=["int_col"],
        values=frozenset(["x", "y", "z"]),
        on_wrong_value="fail",
    )
    with pytest.raises(DataQualityProcessingError) as exc_info:
        dq_engine.is_not_in(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()


@pytest.mark.dq
def test_on_wrong_value_ignore_multiple_columns():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsBetween(
        column_names=["int_col", "float_col", "str_col"],
        min_val="a",
        max_val="b",
        on_wrong_value="ignore",
    )
    dq = dq_engine.is_between(classifier)
    df = dq.tf()._lf.collect()
    assert "int_col_is_between" not in df.columns
    assert "float_col_is_between" not in df.columns
    assert "str_col_is_between" in df.columns


@pytest.mark.dq
def test_on_wrong_value_with_valid_values():
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    classifier = IsBetween(
        column_names=["int_col"],
        min_val=0,
        max_val=100,
        on_wrong_value="fail",
    )
    result_dq = dq_engine.is_between(classifier)
    result_df = result_dq.tf()._lf.collect()
    assert "int_col_is_between" in result_df.columns
    assert all(
        value == ByteSentinel.TRUE
        for value in result_df["int_col_is_between"].to_list()
    )


@pytest.mark.dq
@pytest.mark.parametrize(
    "classifier_type,param_values",
    [
        (IsBetween, {"min_val": "invalid", "max_val": "invalid"}),
        (IsNotBetween, {"min_val": "bad", "max_val": "bad"}),
        (IsIn, {"values": frozenset(["a", "b", "c"])}),
        (IsNotIn, {"values": frozenset(["x", "y", "z"])}),
    ],
    ids=[
        "is_between_bad_types",
        "is_not_between_bad_types",
        "is_in_bad_values",
        "is_not_in_bad_values",
    ],
)
def test_on_wrong_value_fail_parametrized(classifier_type, param_values):
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    parameters = {"column_names": ["int_col"], "on_wrong_value": "fail"}
    parameters.update(param_values)
    classifier = classifier_type(**parameters)
    method_name = (
        classifier_type.__name__.lower().replace("is", "is_").replace("not", "not_")
    )
    method = getattr(dq_engine, method_name)
    with pytest.raises(DataQualityProcessingError) as exc_info:
        method(classifier)
    assert exc_info.value.code == ErrorCode.DQPE16.code()
    assert "int_col" in exc_info.value.message


@pytest.mark.dq
@pytest.mark.parametrize(
    "classifier_type,param_values",
    [
        (IsBetween, {"min_val": "invalid", "max_val": "invalid"}),
        (IsNotBetween, {"min_val": "bad", "max_val": "bad"}),
        (IsIn, {"values": frozenset(["a", "b", "c"])}),
        (IsNotIn, {"values": frozenset(["x", "y", "z"])}),
    ],
    ids=[
        "is_between_bad_min",
        "is_not_between_bad_min",
        "is_in_bad_values",
        "is_not_in_bad_values",
    ],
)
def test_on_wrong_value_ignore_parametrized(classifier_type, param_values):
    tf = wrong_value_dataset()
    dq_engine = tf._dq
    parameters = {"column_names": ["int_col"], "on_wrong_value": "ignore"}
    parameters.update(param_values)
    classifier = classifier_type(**parameters)
    method_name = (
        classifier_type.__name__.lower().replace("is", "is_").replace("not", "not_")
    )
    method = getattr(dq_engine, method_name)
    result_dq = method(classifier)
    result_df = result_dq.tf()._lf.collect()
    expected_col_suffix = method_name
    assert f"int_col_{expected_col_suffix}" not in result_df.columns

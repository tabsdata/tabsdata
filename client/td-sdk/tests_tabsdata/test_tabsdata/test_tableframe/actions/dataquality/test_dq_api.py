#
#  Copyright 2026. Tabs Data Inc.
#

import datetime
from decimal import Decimal
from typing import Any, Callable, Optional

import polars as pl
import pytest
from fntest import tdfnrun
from polars import DataFrame

import tabsdata as td
import tabsdata.dataquality as dq
from tabsdata.dataquality import (
    Classifier,
    Criteria,
    IdentityScale,
    LinearScale,
    LogarithmicScale,
    MonomialScale,
    PercentThreshold,
    RowCountThreshold,
    Threshold,
)
from tabsdata.dataquality._operator import AllOk, AnyFailed
from tabsdata.dataquality._typing import OnInvalidColumnSpec


def input_table() -> pl.DataFrame:
    data = {
        "a": [True, False, None, None, None],
        "b": [-1, 0, 2, None, None],
        "c": [-1.1, 0, 2.2, float("nan"), None],
        "d": ["a", "aa", "c", "d", None],
    }
    return pl.DataFrame(data)


def input_table_for_scales() -> pl.DataFrame:
    data = {
        "identity": [-1, 0, 2, None, None],
        "linear": [-1, 0, 2, None, None],
        "monomial": [-1, 1, 3, None, None],
        "logarithmic": [-1, 1, 7, None, None],
        "exponential": [-1, 1, 7, None, None],
    }
    return pl.DataFrame(data)


def dq_fn(
    classifiers: list[dq.Classifier], operators: list[dq.Operator]
) -> Callable[[td.TableFrame], td.TableFrame]:
    @td.transformer(
        input_tables=["ti"],
        output_tables=["to"],
        on_output_tables=[
            dq.DataQuality(
                table="to",
                classifiers=classifiers,
                operators=operators,
            )
        ],
    )
    def tfr(tf: td.TableFrame) -> td.TableFrame:
        return tf

    return tfr


## Testing Classifiers and Operators Tags


def dq_enrich_with_tags_parameters():
    return [
        (
            [],
            {
                "a",
                "b",
                "c",
                "d",
                "a_is_null",
                "a_is_not_null",
            },
        ),
        (
            None,
            {
                "a",
                "b",
                "c",
                "d",
                "a_is_null",
                "a_is_not_null",
                "b_is_positive",
                "b_is_negative",
                "c_is_nan",
                "c_is_not_nan",
            },
        ),
        (
            ["foo"],
            {
                "a",
                "b",
                "c",
                "d",
                "b_is_positive",
                "b_is_negative",
                "c_is_nan",
                "c_is_not_nan",
            },
        ),
        (["bar"], {"a", "b", "c", "d", "c_is_nan", "c_is_not_nan"}),
        (
            ["foo", "bar"],
            {
                "a",
                "b",
                "c",
                "d",
                "b_is_positive",
                "b_is_negative",
                "c_is_nan",
                "c_is_not_nan",
            },
        ),
    ]


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize("tags,expected_columns", dq_enrich_with_tags_parameters())
def test_dq_enrich_with_tags(tags, expected_columns):
    input_df = input_table()

    classifiers = [
        dq.IsNull("a"),
        dq.IsNotNull("a"),
        dq.IsPositive("b", tags=["foo"]),
        dq.IsNegative("b", tags=["foo"]),
        dq.IsNan("c", tags=["foo", "bar"]),
        dq.IsNotNan("c", tags=["foo", "bar"]),
    ]

    fn = dq_fn(classifiers, [dq.Enrich(tags=tags)])
    res = tdfnrun(input_tables=input_df, function=fn)
    output = res.output.as_pl_df(False)[0]
    got_columns = set(output.schema.names())
    assert got_columns == expected_columns


## Testing on missing columns


def dq_enrich_on_missing_column_parameters():
    return [
        (["a"], "ignore", True, {"a", "b", "c", "d", "a_is_null"}),
        (["a"], "fail", True, {"a", "b", "c", "d", "a_is_null"}),
        (["x"], "ignore", True, {"a", "b", "c", "d"}),
        (["x"], "fail", False, None),
    ]


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "columns,on_missing_column,expected_ok,expected_columns",
    dq_enrich_on_missing_column_parameters(),
)
def test_dq_enrich_on_missing_column(
    columns, on_missing_column: OnInvalidColumnSpec, expected_ok, expected_columns
):
    input_df = input_table()

    classifiers = [
        dq.IsNull(columns, on_missing_column=on_missing_column),
    ]

    err = None
    res = None
    # noinspection PyBroadException
    try:
        fn = dq_fn(classifiers, [dq.Enrich()])
        res = tdfnrun(input_tables=input_df, function=fn)
    except Exception:
        ok = False
    else:
        ok = True

    if expected_ok and not ok:
        raise err
    elif not expected_ok and ok:
        raise Exception("Expected function failure, run OK")
    elif expected_ok and ok:
        output = res.output.as_pl_df(False)[0]
        got_columns = set(output.schema.names())
        assert got_columns == expected_columns
    else:
        pass  # not expected_ok and not ok


## Testing Classifiers on_wrong_types


def input_data_for_wrong_types_tests():
    data = {
        "bool": [True],
        "int": [int(0)],
        "float": [float(0)],
        "decimal": [Decimal(0)],
        "str": ["str"],
        "datetime": [datetime.datetime.now()],
        "time": [datetime.datetime.now().time()],
        "date": [datetime.datetime.now().date()],
        "duration": [datetime.timedelta(days=1)],
    }
    return data


def dq_operators_on_wrong_type_params(on_wrong_type: OnInvalidColumnSpec):
    return [
        #  (classifier, invalid_columns_for_classifier, column_suffix)
        (dq.IsNull(None), [], "_is_null"),
        (dq.IsNotNull(None), [], "_is_not_null"),
        (
            dq.IsNan(on_wrong_type=on_wrong_type),
            ["bool", "int", "decimal", "str", "datetime", "date", "time", "duration"],
            "_is_nan",
        ),
        (
            dq.IsNotNan(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "decimal", "str", "datetime", "date", "time", "duration"],
            "_is_not_nan",
        ),
        (
            dq.IsNullOrNan(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "decimal", "str", "datetime", "date", "time", "duration"],
            "_is_null_or_nan",
        ),
        (
            dq.IsNotNullNorNan(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "decimal", "str", "datetime", "date", "time", "duration"],
            "_is_not_null_nor_nan",
        ),
        (
            dq.IsZero(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_zero",
        ),
        (
            dq.IsNotZero(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_not_zero",
        ),
        (
            dq.IsPositive(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_positive",
        ),
        (
            dq.IsPositiveOrZero(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_positive_or_zero",
        ),
        (
            dq.IsNegative(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_negative",
        ),
        (
            dq.IsNegativeOrZero(
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "str", "datetime", "date", "time", "duration"],
            "_is_negative_or_zero",
        ),
        (
            dq.IsBetween(
                None,
                0,
                2,
                on_wrong_type=on_wrong_type,
            ),
            ["bool"],
            "_is_between",
        ),
        (
            dq.IsNotBetween(
                None,
                0,
                2,
                on_wrong_type=on_wrong_type,
            ),
            ["bool"],
            "_is_not_between",
        ),
        (dq.IsIn(None, values=[]), [], "_is_in"),
        (dq.IsNotIn(None, values=[]), [], "_is_not_in"),
        (
            dq.Matches(
                None,
                pattern="a.*",
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "float", "decimal", "datetime", "date", "time", "duration"],
            "_matches",
        ),
        (
            dq.DoesNotMatch(
                None,
                pattern="a.*",
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "float", "decimal", "datetime", "date", "time", "duration"],
            "_does_not_match",
        ),
        (
            dq.HasLength(
                None,
                min_len=0,
                max_len=1,
                on_wrong_type=on_wrong_type,
            ),
            ["bool", "int", "float", "decimal", "datetime", "date", "time", "duration"],
            "_has_length",
        ),
    ]


def dq_operators_on_wrong_type_params_ignore():
    return dq_operators_on_wrong_type_params("ignore")


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifier,invalid_columns_for_classifier, column_suffix",
    dq_operators_on_wrong_type_params_ignore(),
)
def test_dq_operators_on_wrong_type_ignore(
    classifier: Classifier, invalid_columns_for_classifier, column_suffix: str
):
    data = input_data_for_wrong_types_tests()
    input_df = pl.DataFrame(data)

    df_columns = data.keys()
    df_valid_dq_columns = data.keys() - invalid_columns_for_classifier
    dq_column_names = [f"{col}{column_suffix}" for col in df_valid_dq_columns]
    expected_columns = df_columns | dq_column_names

    fn = dq_fn([classifier], [dq.Enrich()])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)[0]
    got_columns = set(output.schema.names())

    assert got_columns == expected_columns


def dq_operators_on_wrong_type_params_fail():
    return dq_operators_on_wrong_type_params("fail")


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifier,invalid_columns_for_classifier, column_suffix",
    dq_operators_on_wrong_type_params_fail(),
)
def test_dq_operators_on_wrong_type_fail_using_ok_types(
    classifier: Classifier, invalid_columns_for_classifier, column_suffix: str
):
    data = input_data_for_wrong_types_tests()

    valid_data = {
        k: v for k, v in data.items() if k not in invalid_columns_for_classifier
    }
    data = valid_data

    input_df = pl.DataFrame(data)

    df_columns = data.keys()
    df_valid_dq_columns = data.keys()

    dq_column_names = [f"{col}{column_suffix}" for col in df_valid_dq_columns]

    expected_columns = df_columns | dq_column_names

    fn = dq_fn([classifier], [dq.Enrich()])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)[0]
    got_columns = set(output.schema.names())

    assert got_columns == expected_columns


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifier,invalid_columns_for_classifier, column_suffix",
    dq_operators_on_wrong_type_params_fail(),
)
def test_dq_operators_on_wrong_type_fail_using_wrong_types(
    classifier: Classifier, invalid_columns_for_classifier, column_suffix: str
):
    data = input_data_for_wrong_types_tests()

    invalid_data = {
        k: v for k, v in data.items() if k in invalid_columns_for_classifier
    }

    for k, v in invalid_data.items():
        data = {k: v}
        input_df = pl.DataFrame(data)

        fn = dq_fn([classifier], [dq.Enrich()])

        # noinspection PyBroadException
        try:
            tdfnrun(input_tables=input_df, function=fn)
        except Exception:
            pass
        else:
            assert False


def dq_summary_tags_params() -> list[
    tuple[
        list[Classifier],
        Optional[list[str]],
        bool,
        Optional[str],
        Optional[dict[str, Any]],
    ],
]:
    # (classifiers, summary_tags, has_summary_info, summary_row, summary row info)
    return [
        # classifier no tags, summary no tags, in summary
        (
            [dq.HasLength("d", max_len=1)],
            None,
            True,
            "d_has_length",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        # classifier tag, summary no tags, in summary
        (
            [dq.HasLength("d", max_len=1, tags="t")],
            None,
            True,
            "d_has_length",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        # classifier no tag, summary tag, not in summary
        ([dq.HasLength("d", max_len=1)], ["t"], False, None, None),
        # classifier tag, summary tag, in summary
        (
            [dq.HasLength("d", max_len=1, tags="t")],
            ["t"],
            True,
            "d_has_length",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        # classifier tag, summary other tag, not in summary
        ([dq.HasLength("d", max_len=1, tags="t")], ["tt"], False, None, None),
    ]


def dq_summary_bool_classifiers_params() -> list[
    tuple[
        list[Classifier],
        Optional[list[str]],
        bool,
        Optional[str],
        Optional[dict[str, Any]],
    ],
]:
    # (classifiers, summary_tags, has_summary_info, summary_row, summary row info)
    return [
        (
            [dq.IsTrue("a")],
            None,
            True,
            "a_is_true",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 3,
                "dq.nan": 0,
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsFalse("a")],
            None,
            True,
            "a_is_false",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 3,
                "dq.nan": 0,
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNull("a")],
            None,
            True,
            "a_is_null",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 0,
                "dq.nan": 0,
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotNull("a")],
            None,
            True,
            "a_is_not_null",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 0,
                "dq.nan": 0,
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNan("a")],
            None,
            False,
            False,
            False,
        ),
        (
            [dq.IsNotNan("a")],
            None,
            False,
            False,
            False,
        ),
        (
            [dq.IsNan("c")],
            None,
            True,
            "c_is_nan",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotNan("c")],
            None,
            True,
            "c_is_not_nan",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNullOrNan("a")],
            None,
            False,
            False,
            False,
        ),
        (
            [dq.IsNotNullNorNan("a")],
            None,
            False,
            False,
            False,
        ),
        (
            [dq.IsNullOrNan("c")],
            None,
            True,
            "c_is_null_or_nan",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 0,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotNullNorNan("c")],
            None,
            True,
            "c_is_not_null_nor_nan",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 0,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsZero("c")],
            None,
            True,
            "c_is_zero",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotZero("c")],
            None,
            True,
            "c_is_not_zero",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsPositive("c")],
            None,
            True,
            "c_is_positive",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsPositiveOrZero("c")],
            None,
            True,
            "c_is_positive_or_zero",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNegative("c")],
            None,
            True,
            "c_is_negative",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNegativeOrZero("c")],
            None,
            True,
            "c_is_negative_or_zero",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 1,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsBetween("b", 0, 2, closed_on="both")],
            None,
            True,
            "b_is_between",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotBetween("b", 0, 2, closed_on="both")],
            None,
            True,
            "b_is_not_between",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsBetween("b", 0, 2, closed_on="none")],
            None,
            True,
            "b_is_between",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 0,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotBetween("b", 0, 2, closed_on="none")],
            None,
            True,
            "b_is_not_between",
            {
                "dq.type": "boolean",
                "dq.false": 0,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsBetween("b", 0, 2, closed_on="lower")],
            None,
            True,
            "b_is_between",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotBetween("b", 0, 2, closed_on="lower")],
            None,
            True,
            "b_is_not_between",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsBetween("b", 0, 2, closed_on="upper")],
            None,
            True,
            "b_is_between",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotBetween("b", 0, 2, closed_on="upper")],
            None,
            True,
            "b_is_not_between",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsIn("b", values=[0, None])],
            None,
            True,
            "b_is_in",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 0,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotIn("b", values=[0, None])],
            None,
            True,
            "b_is_not_in",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 0,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsIn("b", values=[0])],
            None,
            True,
            "b_is_in",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.IsNotIn("b", values=[0])],
            None,
            True,
            "b_is_not_in",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.Matches("d", pattern="^aa?$")],
            None,
            True,
            "d_matches",
            {
                "dq.type": "boolean",
                "dq.false": 2,
                "dq.true": 2,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.DoesNotMatch("d", pattern="^a$")],
            None,
            True,
            "d_does_not_match",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.HasLength("d", min_len=2)],
            None,
            True,
            "d_has_length",
            {
                "dq.type": "boolean",
                "dq.false": 3,
                "dq.true": 1,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
        (
            [dq.HasLength("d", max_len=1)],
            None,
            True,
            "d_has_length",
            {
                "dq.type": "boolean",
                "dq.false": 1,
                "dq.true": 3,
                "dq.bins": None,
                "dq.none": 1,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": None,
                "dq.over": None,
                "dq.p000": None,
                "dq.p001": None,
            },
        ),
    ]


def dq_summary_categorizer_classifiers_params() -> list[
    tuple[
        list[Classifier],
        Optional[list[str]],
        bool,
        Optional[str],
        Optional[dict[str, Any]],
    ],
]:
    # (classifiers, summary_tags, has_summary_info, summary_row, summary row info)
    return [
        (
            [dq.ScaleCategorizer(column_names="identity", scale=IdentityScale(-2, 0))],
            None,
            True,
            "identity_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 2,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 0,
                "dq.over": 1,
                "dq.p000": None,
                "dq.p001": 1,
                "dq.p002": 1,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="identity",
                    scale=IdentityScale(0, 2, use_bin_zero=True),
                )
            ],
            None,
            True,
            "identity_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": 1,
                "dq.p001": 0,
                "dq.p002": 1,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="linear", scale=LinearScale(0, 2, bins=2)
                )
            ],
            None,
            True,
            "linear_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 2,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 1,
                "dq.p002": 1,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="linear", scale=LinearScale(-1, 1, bins=2)
                )
            ],
            None,
            True,
            "linear_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 2,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 0,
                "dq.over": 1,
                "dq.p000": None,
                "dq.p001": 2,
                "dq.p002": 0,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="linear",
                    scale=LinearScale(0, 2, bins=2, use_bin_zero=True),
                )
            ],
            None,
            True,
            "linear_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": 1,
                "dq.p001": 0,
                "dq.p002": 1,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="linear",
                    scale=LinearScale(0, 5, bins=2),
                )
            ],
            None,
            True,
            "linear_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 2,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 2,
                "dq.p002": 0,
                "dq.p003": None,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="monomial", scale=MonomialScale(0, 9, power=2, bins=3)
                )
            ],
            None,
            True,
            "monomial_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 1,
                "dq.p002": 1,
                "dq.p003": 0,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="monomial",
                    scale=MonomialScale(-1, 8, power=2, bins=3, use_bin_zero=True),
                )
            ],
            None,
            True,
            "monomial_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 4,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 0,
                "dq.over": 0,
                "dq.p000": 1,
                "dq.p001": 0,
                "dq.p002": 2,
                "dq.p003": 0,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="monomial", scale=MonomialScale(-3, 6, power=2, bins=3)
                )
            ],
            None,
            True,
            "monomial_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 0,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 0,
                "dq.p002": 2,
                "dq.p003": 1,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="logarithmic",
                    scale=LogarithmicScale(0, 8, base=2, bins=3),
                )
            ],
            None,
            True,
            "logarithmic_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 1,
                "dq.p002": 1,
                "dq.p003": 0,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="logarithmic",
                    scale=LogarithmicScale(1, 9, base=2, bins=3, use_bin_zero=True),
                )
            ],
            None,
            True,
            "logarithmic_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 4,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": 1,
                "dq.p001": 1,
                "dq.p002": 0,
                "dq.p003": 0,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="exponential",
                    scale=LogarithmicScale(0, 8, base=2, bins=3),
                )
            ],
            None,
            True,
            "exponential_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 3,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": None,
                "dq.p001": 1,
                "dq.p002": 1,
                "dq.p003": 0,
            },
        ),
        (
            [
                dq.ScaleCategorizer(
                    column_names="exponential",
                    scale=LogarithmicScale(1, 9, base=2, bins=3, use_bin_zero=True),
                )
            ],
            None,
            True,
            "exponential_category",
            {
                "dq.type": "categorical",
                "dq.false": None,
                "dq.true": None,
                "dq.bins": 4,
                "dq.none": 2,  # DOC
                "dq.nan": 0,  # DOC
                "dq.under": 1,
                "dq.over": 0,
                "dq.p000": 1,
                "dq.p001": 1,
                "dq.p002": 0,
                "dq.p003": 0,
            },
        ),
    ]


def _test_dq_summary(
    input_df: DataFrame,
    classifiers: list[Classifier],
    summary_tags: Optional[list[str]],
    has_summary_info: bool,
    summary_row: Optional[str],
    summary_info: dict[str, Any],
):

    fn = dq_fn(classifiers, [dq.Summary(tags=summary_tags)])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)[1]
    filtered = output.filter(pl.col("dq.classifier") == summary_row)
    if has_summary_info:
        assert len(filtered) == 1
        got_info = filtered.row(0, named=True)
        got_info_subset = {k: got_info[k] for k in summary_info.keys()}
        assert got_info_subset == summary_info
    else:
        assert len(filtered) == 0


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifiers,summary_tags,has_summary_info,summary_row,summary_info",
    dq_summary_tags_params(),
)
def test_dq_summary_tags(
    classifiers: list[Classifier],
    summary_tags: Optional[list[str]],
    has_summary_info: bool,
    summary_row: Optional[str],
    summary_info: dict[str, Any],
):
    _test_dq_summary(
        input_table(),
        classifiers,
        summary_tags,
        has_summary_info,
        summary_row,
        summary_info,
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifiers,summary_tags,has_summary_info,summary_row,summary_info",
    dq_summary_bool_classifiers_params(),
)
def test_dq_summary_with_bool_classifiers(
    classifiers: list[Classifier],
    summary_tags: Optional[list[str]],
    has_summary_info: bool,
    summary_row: Optional[str],
    summary_info: dict[str, Any],
):
    _test_dq_summary(
        input_table(),
        classifiers,
        summary_tags,
        has_summary_info,
        summary_row,
        summary_info,
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "classifiers,summary_tags,has_summary_info,summary_row,summary_info",
    dq_summary_categorizer_classifiers_params(),
)
def test_dq_summary_with_categorizer_classifiers(
    classifiers: list[Classifier],
    summary_tags: Optional[list[str]],
    has_summary_info: bool,
    summary_row: Optional[str],
    summary_info: dict[str, Any],
):
    _test_dq_summary(
        input_table_for_scales(),
        classifiers,
        summary_tags,
        has_summary_info,
        summary_row,
        summary_info,
    )


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize("to_table", [None, "t"])
def test_dq_enrich(to_table):
    input_df = input_table()

    fn = dq_fn([dq.IsNull(["a"])], [dq.Enrich(to_table=to_table)])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)

    if to_table:
        assert len(output) == 2
        output_df = output[0]
        assert "a_is_null" not in output_df.schema.names()
        output_df = output[1]
        assert "a_is_null" in output_df.schema.names()
    else:
        assert len(output) == 1
        output_df = output[0]
        assert "a_is_null" in output_df.schema.names()


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize("to_table", [None, "t"])
def test_dq_filter(to_table):
    input_df = input_table()

    # input table has 2 rows with the 'a' column non-NULL and 3 rows with NULL

    fn = dq_fn([dq.IsNotNull(["a"])], [dq.Filter(to_table=to_table, criteria=AllOk())])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)

    if to_table:
        assert len(output) == 2

    output_df = output[0]
    assert "a_is_not_null" not in output_df.schema.names()
    assert len(output_df) == 2
    assert len(output_df.filter(pl.col("a") is not None)) == 2

    if to_table:
        output_df = output[1]
        assert "a_is_not_null" not in output_df.schema.names()
        assert len(output_df) == 3
        assert len(output_df.filter(pl.col("a").is_null())) == 3


@pytest.mark.dq
@pytest.mark.tdfnrun
def test_dq_select():
    input_df = input_table()

    # input table has 2 rows with the 'a' column non-NULL and 3 rows with NULL

    fn = dq_fn([dq.IsNotNull(["a"])], [dq.Select(to_table="t", criteria=AllOk())])

    res = tdfnrun(input_tables=input_df, function=fn)

    output = res.output.as_pl_df(False)

    assert len(output) == 2

    output_df = output[0]
    assert "a_is_not_null" not in output_df.schema.names()
    assert len(output_df) == 5

    output_df = output[1]
    assert "a_is_not_null" not in output_df.schema.names()
    assert len(output_df) == 2
    assert len(output_df.filter(pl.col("a").is_not_null())) == 2


def dq_fail_all_ok_parameters():
    return [
        # input has:
        #   'a': [True, False, None, None, None],
        #   'b': [  -1,     0,    2, None, None],
        # (criteria, threshold, success)
        # With ANY there are 3 matches.
        (AnyFailed(), RowCountThreshold(2), False),
        (AnyFailed(), RowCountThreshold(3), True),
        (AnyFailed(), PercentThreshold(59), False),
        (AnyFailed(), PercentThreshold(60), True),
        # With ANY there are 2 matches.
        (AllOk(), RowCountThreshold(2), True),
        (AllOk(), RowCountThreshold(3), False),
        (AllOk(), PercentThreshold(40), True),
        (AllOk(), PercentThreshold(41), False),
    ]


@pytest.mark.dq
@pytest.mark.tdfnrun
@pytest.mark.parametrize(
    "criteria, threshold, expected_ok", dq_fail_all_ok_parameters()
)
def test_dq_fail_all_ok(
    criteria: Criteria, threshold: Optional[Threshold], expected_ok: bool
):
    input_df = input_table()

    fn = dq_fn(
        [dq.IsNotNull(["a", "b"])], [dq.Fail(threshold=threshold, criteria=criteria)]
    )

    res = None
    err = None
    # noinspection PyBroadException
    try:
        res = tdfnrun(input_tables=input_df, function=fn)
    except Exception:
        ok = False
    else:
        ok = True

    if expected_ok and not ok:
        raise err
    elif not expected_ok and ok:
        raise Exception("Expected function failure, run OK")
    elif expected_ok and ok:
        output = res.output.as_pl_df(False)[0]
        assert len(output) == 5
    else:
        pass  # not expected_ok and not ok

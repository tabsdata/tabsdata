#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import polars as pl
import pytest

ID = "id"
NAME = "name"
SCORE = "score"
EXTRA = "extra"
OP = "op"

STRICT = "strict"
UPDATE = "update"
APPEND = "append"
MERGE = "merge"
REPLACE = "replace"

BEYOND_INT32 = 2**40

AMOUNT = "amount"
NARROW_PRECISION = 10
WIDE_PRECISION = 38
NARROW_AMOUNT = "10.25"
FITTING_AMOUNT = "99999999.99"

REFUSAL = "SQL_COMMON_10"


@dataclass(frozen=True)
class Policy:
    write: Callable[[str, pl.LazyFrame, str, str], None]
    read: Callable[[str], pl.DataFrame]
    drop: Callable[[str], None]

    def rows(self, table: str, columns: list[str]) -> list[tuple]:
        frame = self.read(table)
        frame = frame.rename({name: name.lower() for name in frame.columns})
        return sorted(frame.select(columns).rows(), key=repr)

    def columns(self, table: str) -> set[str]:
        return {name.lower() for name in self.read(table).columns}


def _base() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [1], NAME: ["one"], SCORE: [10]},
        schema={ID: pl.Int64, NAME: pl.String, SCORE: pl.Int64},
    )


def _added() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [2], NAME: ["two"], SCORE: [20], EXTRA: ["x"]},
        schema={ID: pl.Int64, NAME: pl.String, SCORE: pl.Int64, EXTRA: pl.String},
    )


def _lacking() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [2], NAME: ["two"]}, schema={ID: pl.Int64, NAME: pl.String}
    )


def _narrow() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [1], NAME: ["one"], SCORE: [10]},
        schema={ID: pl.Int64, NAME: pl.String, SCORE: pl.Int32},
    )


def _wide() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [2], NAME: ["two"], SCORE: [BEYOND_INT32]},
        schema={ID: pl.Int64, NAME: pl.String, SCORE: pl.Int64},
    )


def _fitting() -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [2], NAME: ["two"], SCORE: [20]},
        schema={ID: pl.Int64, NAME: pl.String, SCORE: pl.Int64},
    )


def _amounts(amount: str, precision: int) -> pl.LazyFrame:
    return pl.LazyFrame(
        {ID: [1 if precision == NARROW_PRECISION else 2], AMOUNT: [Decimal(amount)]},
        schema={ID: pl.Int64, AMOUNT: pl.Decimal(precision, 2)},
    )


def _seed(policy: Policy, table: str, frame: pl.LazyFrame) -> None:
    policy.drop(table)
    policy.write(table, frame, REPLACE, STRICT)


def created_table_has_only_the_batch_columns(policy: Policy, table: str) -> None:
    _seed(policy, table, _base())
    _expect(policy.columns(table), {ID, NAME, SCORE})


def strict_refuses_a_new_column(policy: Policy, table: str) -> None:
    _seed(policy, table, _base())
    _refused(policy, table, lambda: policy.write(table, _added(), APPEND, STRICT))
    _expect(policy.columns(table), {ID, NAME, SCORE})
    _expect(policy.rows(table, [ID]), [(1,)])


def update_adds_a_new_column(policy: Policy, table: str) -> None:
    _seed(policy, table, _base())
    policy.write(table, _added(), APPEND, UPDATE)
    _expect(policy.columns(table), {ID, NAME, SCORE, EXTRA})
    _expect(policy.rows(table, [ID, EXTRA]), [(1, None), (2, "x")])


def a_missing_column_is_silent(policy: Policy, table: str, evolution: str) -> None:
    _seed(policy, table, _base())
    policy.write(table, _lacking(), APPEND, evolution)
    _expect(policy.columns(table), {ID, NAME, SCORE})
    _expect(policy.rows(table, [ID, NAME, SCORE]), [(1, "one", 10), (2, "two", None)])


def strict_refuses_a_wider_type(policy: Policy, table: str) -> None:
    _seed(policy, table, _narrow())
    try:
        policy.write(table, _wide(), APPEND, STRICT)
    except Exception:
        _expect(policy.rows(table, [ID, SCORE]), [(1, 10)])
    else:
        _expect(policy.rows(table, [ID, SCORE]), [(1, 10), (2, BEYOND_INT32)])


def strict_takes_a_wider_type_whose_values_fit(policy: Policy, table: str) -> None:
    _seed(policy, table, _narrow())
    policy.write(table, _fitting(), APPEND, STRICT)
    _expect(policy.rows(table, [ID, SCORE]), [(1, 10), (2, 20)])


def _amount_rows(policy: Policy, table: str) -> list[tuple]:
    return [(i, Decimal(str(a))) for i, a in policy.rows(table, [ID, AMOUNT])]


def strict_takes_a_wider_decimal_whose_values_fit(policy: Policy, table: str) -> None:
    _seed(policy, table, _amounts(NARROW_AMOUNT, NARROW_PRECISION))
    policy.write(table, _amounts(FITTING_AMOUNT, WIDE_PRECISION), APPEND, STRICT)
    _expect(
        _amount_rows(policy, table),
        [(1, Decimal(NARROW_AMOUNT)), (2, Decimal(FITTING_AMOUNT))],
    )


def update_widens_a_type(policy: Policy, table: str) -> None:
    _seed(policy, table, _narrow())
    policy.write(table, _wide(), APPEND, UPDATE)
    _expect(policy.rows(table, [ID, SCORE]), [(1, 10), (2, BEYOND_INT32)])


def _cdc(frame: pl.LazyFrame, op: str) -> pl.LazyFrame:
    return frame.with_columns(pl.lit(op).alias(OP))


def merge_keeps_what_the_batch_lacks(
    policy: Policy, table: str, evolution: str
) -> None:
    policy.drop(table)
    policy.write(table, _cdc(_base(), "i"), MERGE, evolution)
    batch = pl.LazyFrame(
        {ID: [1, 2], NAME: ["uno", "two"], OP: ["u", "i"]},
        schema={ID: pl.Int64, NAME: pl.String, OP: pl.String},
    )
    policy.write(table, batch, MERGE, evolution)
    _expect(policy.columns(table), {ID, NAME, SCORE})
    _expect(policy.rows(table, [ID, NAME, SCORE]), [(1, "uno", 10), (2, "two", None)])


def merge_strict_refuses_a_new_column(policy: Policy, table: str) -> None:
    policy.drop(table)
    policy.write(table, _cdc(_base(), "i"), MERGE, STRICT)
    _refused(
        policy, table, lambda: policy.write(table, _cdc(_added(), "i"), MERGE, STRICT)
    )
    _expect(policy.columns(table), {ID, NAME, SCORE})


def merge_strict_takes_a_wider_type_whose_values_fit(
    policy: Policy, table: str
) -> None:
    policy.drop(table)
    policy.write(table, _cdc(_narrow(), "i"), MERGE, STRICT)
    policy.write(table, _cdc(_fitting(), "i"), MERGE, STRICT)
    _expect(policy.rows(table, [ID, SCORE]), [(1, 10), (2, 20)])


def merge_strict_takes_a_wider_decimal_whose_values_fit(
    policy: Policy, table: str
) -> None:
    policy.drop(table)
    policy.write(
        table, _cdc(_amounts(NARROW_AMOUNT, NARROW_PRECISION), "i"), MERGE, STRICT
    )
    policy.write(
        table, _cdc(_amounts(FITTING_AMOUNT, WIDE_PRECISION), "i"), MERGE, STRICT
    )
    _expect(
        _amount_rows(policy, table),
        [(1, Decimal(NARROW_AMOUNT)), (2, Decimal(FITTING_AMOUNT))],
    )


def merge_update_adds_a_new_column(policy: Policy, table: str) -> None:
    policy.drop(table)
    policy.write(table, _cdc(_base(), "i"), MERGE, UPDATE)
    policy.write(table, _cdc(_added(), "i"), MERGE, UPDATE)
    _expect(policy.columns(table), {ID, NAME, SCORE, EXTRA})
    _expect(policy.rows(table, [ID, EXTRA]), [(1, None), (2, "x")])


APPEND_CASES = [
    pytest.param(created_table_has_only_the_batch_columns, id="created-exact"),
    pytest.param(strict_refuses_a_new_column, id="strict-new-column"),
    pytest.param(update_adds_a_new_column, id="update-new-column"),
    pytest.param(
        lambda policy, table: a_missing_column_is_silent(policy, table, STRICT),
        id="strict-missing-column",
    ),
    pytest.param(
        lambda policy, table: a_missing_column_is_silent(policy, table, UPDATE),
        id="update-missing-column",
    ),
    pytest.param(strict_refuses_a_wider_type, id="strict-wider-type"),
    pytest.param(strict_takes_a_wider_type_whose_values_fit, id="strict-fitting-type"),
    pytest.param(
        strict_takes_a_wider_decimal_whose_values_fit, id="strict-fitting-decimal"
    ),
    pytest.param(update_widens_a_type, id="update-wider-type"),
]

SCHEMALESS_CASES = [
    pytest.param(created_table_has_only_the_batch_columns, id="created-exact"),
    pytest.param(update_adds_a_new_column, id="new-column"),
    pytest.param(
        lambda policy, table: a_missing_column_is_silent(policy, table, UPDATE),
        id="missing-column",
    ),
]

MERGE_CASES = [
    pytest.param(
        lambda policy, table: merge_keeps_what_the_batch_lacks(policy, table, STRICT),
        id="merge-strict-missing-column",
    ),
    pytest.param(
        lambda policy, table: merge_keeps_what_the_batch_lacks(policy, table, UPDATE),
        id="merge-update-missing-column",
    ),
    pytest.param(merge_strict_refuses_a_new_column, id="merge-strict-new-column"),
    pytest.param(
        merge_strict_takes_a_wider_type_whose_values_fit, id="merge-strict-fitting-type"
    ),
    pytest.param(
        merge_strict_takes_a_wider_decimal_whose_values_fit,
        id="merge-strict-fitting-decimal",
    ),
    pytest.param(merge_update_adds_a_new_column, id="merge-update-new-column"),
]


def run(case, policy: Policy, table: str) -> None:
    try:
        case(policy, table)
    finally:
        policy.drop(table)


def table_for(request) -> str:
    return "td_policy_" + re.sub(r"\W", "_", request.node.callspec.id).lower()


def _expect(actual, expected) -> None:
    assert actual == expected, f"{actual!r} != {expected!r}"


def _refused(policy: Policy, table: str, write: Callable[[], None]) -> None:
    try:
        write()
    except Exception as e:
        assert str(e).startswith(REFUSAL), f"refused with another error: {e}"
        return
    pytest.fail(
        f"strict accepted the batch; {table} has {sorted(policy.columns(table))}"
    )

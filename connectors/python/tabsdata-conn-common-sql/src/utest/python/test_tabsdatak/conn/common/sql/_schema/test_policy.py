#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import (
    Action,
    ColumnDef,
    TableSchema,
    compute_delta,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import STRICT, UPDATE, governed
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonRuntimeException,
)

pytestmark = pytest.mark.sql_common

WIDENING = {"BIGINT": {"INT"}}


def _delta(current: list[tuple[str, str]] | None, batch: list[tuple[str, str]]):
    def schema(columns):
        return TableSchema(
            table_name="t",
            columns=[ColumnDef(name=name, type=type_) for name, type_ in columns],
        )

    return compute_delta(
        None if current is None else schema(current), schema(batch), WIDENING
    )


TABLE = [("id", "BIGINT"), ("name", "STRING"), ("score", "INT")]


@pytest.mark.parametrize("evolution", [STRICT, UPDATE])
def test_a_new_table_takes_the_batch_columns(evolution):
    delta = _delta(None, [("id", "BIGINT"), ("extra", "STRING")])
    assert governed(delta, evolution) is delta


@pytest.mark.parametrize("evolution", [STRICT, UPDATE])
def test_a_missing_column_is_not_a_change(evolution):
    assert governed(_delta(TABLE, TABLE[:2]), evolution).changes == []


@pytest.mark.parametrize("evolution", [STRICT, UPDATE])
def test_a_matching_batch_changes_nothing(evolution):
    assert governed(_delta(TABLE, TABLE), evolution).changes == []


def test_strict_refuses_a_new_column():
    with pytest.raises(SqlCommonRuntimeException) as raised:
        governed(_delta(TABLE, [*TABLE, ("extra", "STRING")]), STRICT)
    assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_10
    assert "new column 'extra'" in str(raised.value)


def test_update_adds_a_new_column():
    [change] = governed(_delta(TABLE, [*TABLE, ("extra", "STRING")]), UPDATE).changes
    assert (change.action, change.column) == (Action.ADD_COLUMN, "extra")


def test_strict_refuses_a_safe_widening():
    batch = [("id", "BIGINT"), ("name", "STRING"), ("score", "BIGINT")]
    with pytest.raises(SqlCommonRuntimeException) as raised:
        governed(_delta(TABLE, batch), STRICT)
    assert "column 'score' from INT to BIGINT" in str(raised.value)


def test_update_widens_safely():
    batch = [("id", "BIGINT"), ("name", "STRING"), ("score", "BIGINT")]
    [change] = governed(_delta(TABLE, batch), UPDATE).changes
    assert (change.action, change.column) == (Action.ALTER_TYPE, "score")


def test_update_refuses_an_unsafe_type_change():
    batch = [("id", "BIGINT"), ("name", "STRING"), ("score", "STRING")]
    with pytest.raises(SqlCommonRuntimeException) as raised:
        governed(_delta(TABLE, batch), UPDATE)
    assert "column 'score' from INT to STRING" in str(raised.value)


def test_strict_names_every_refused_change():
    batch = [("id", "BIGINT"), ("name", "STRING"), ("score", "BIGINT"), ("x", "INT")]
    with pytest.raises(SqlCommonRuntimeException) as raised:
        governed(_delta(TABLE, batch), STRICT)
    message = str(raised.value)
    assert "new column 'x'" in message and "column 'score'" in message

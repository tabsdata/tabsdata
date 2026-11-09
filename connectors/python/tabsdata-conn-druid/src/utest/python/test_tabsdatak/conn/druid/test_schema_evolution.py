#
# Copyright 2026 Tabsdata Inc.
#

"""`DruidDest` against the schema evolution matrix: every disposition, policy
and engine a user can write either registers with the resolution the matrix
gives, or is refused."""

from __future__ import annotations

import inspect

import pytest

from tabsdatak.conn.common.error import ConnInitException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema import matrix
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)
from tabsdatak.conn.druid import DruidDest

pytestmark = pytest.mark.druid

_KEY = "druid.schema_evolution_engine"
# What this destination's own types admit; anything else is refused by the
# field itself, before the matrix is asked.
_TYPED_DISPOSITIONS = ("append", "replace")
_TYPED_POLICIES = ("update",)
_TYPED_ENGINE = False


def _dest(disposition: str, policy: str | None, engine: str | None) -> DruidDest:
    kwargs: dict = dict(tables=["events"], if_table_exists=disposition)
    if policy is not None:
        kwargs["schema_evolution"] = policy
    if engine is not None:
        kwargs["dest_cfg"] = {_KEY: engine}
    return DruidDest(**kwargs)


_COMBINATIONS = [
    (disposition, policy, engine)
    for disposition in matrix.DISPOSITIONS
    for policy in (None, "strict", "update")
    for engine in (None, "native", "iceberg")
]


@pytest.mark.parametrize("disposition,policy,engine", _COMBINATIONS)
def test_a_written_combination_registers_as_the_matrix_resolves_it(
    disposition, policy, engine
):
    try:
        expected = matrix.resolve("druid", disposition, policy, engine)
    except SqlCommonValidateException:
        expected = None
    if expected is not None:
        assert _dest(disposition, policy, engine)._materialized() == (
            expected.options()
        )
        return
    typed = (
        disposition in _TYPED_DISPOSITIONS
        and (policy is None or policy in _TYPED_POLICIES)
        and (engine is None or _TYPED_ENGINE)
    )
    with pytest.raises((SqlCommonValidateException, ConnInitException)) as raised:
        _dest(disposition, policy, engine)
    if typed:
        assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_11


def test_a_left_out_policy_resolves_to_the_matrix_default():
    for disposition in _TYPED_DISPOSITIONS:
        row = matrix.MATRIX["druid"].rows[disposition]
        if row.status != "offered":
            continue
        resolved = _dest(disposition, None, None)._evolution()
        assert resolved.schema_evolution == row.default
        assert resolved.engine == row.engine


def test_a_round_trip_keeps_the_resolution():
    for disposition in _TYPED_DISPOSITIONS:
        try:
            dest = _dest(disposition, None, None)
        except (SqlCommonValidateException, ConnInitException):
            continue
        again = DruidDest.from_json(dest.to_json())
        assert again._materialized() == dest._materialized()


def test_the_docstring_carries_the_matrix_of_this_destination():
    docstring = inspect.cleandoc(DruidDest.__doc__)
    assert matrix.docstring_section("druid") in docstring

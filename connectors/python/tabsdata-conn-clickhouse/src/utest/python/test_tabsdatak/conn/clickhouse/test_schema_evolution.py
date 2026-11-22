#
# Copyright 2026 Tabsdata Inc.
#

"""`ClickHouseDest` against the schema evolution matrix: every disposition, policy
and engine a user can write either registers with the resolution the matrix
gives, or is refused."""

from __future__ import annotations

import inspect

import pytest

from tabsdatak.conn.clickhouse import ClickHouseDest
from tabsdatak.conn.common.error import ConnInitException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema import matrix
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)

pytestmark = pytest.mark.clickhouse

_KEY = "clickhouse.schema_evolution_engine"
# What this destination's own types admit; anything else is refused by the
# field itself, before the matrix is asked.
_TYPED_DISPOSITIONS = ("append", "replace")
_TYPED_POLICIES = ("strict", "update")
_TYPED_ENGINE = False


def _dest(disposition: str, policy: str | None, engine: str | None) -> ClickHouseDest:
    kwargs: dict = dict(tables=["t"], if_table_exists=disposition)
    if policy is not None:
        kwargs["schema_evolution"] = policy
    if engine is not None:
        kwargs["dest_cfg"] = {_KEY: engine}
    return ClickHouseDest(**kwargs)


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
        expected = matrix.resolve("clickhouse", disposition, policy, engine)
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
        row = matrix.MATRIX["clickhouse"].rows[disposition]
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
        again = ClickHouseDest.from_json(dest.to_json())
        assert again._materialized() == dest._materialized()


def test_the_docstring_carries_the_matrix_of_this_destination():
    docstring = inspect.cleandoc(ClickHouseDest.__doc__)
    assert matrix.docstring_section("clickhouse") in docstring


def _written_with(dest: ClickHouseDest) -> str:
    """The policy the plugin hands its writer for ``dest``."""
    from unittest.mock import MagicMock, patch

    import polars as pl

    from tabsdatak.conn.clickhouse._plugin import _ClickHouseDestPlugin

    captured: dict = {}

    def _capture(cfg, *args, **kwargs):
        captured["cfg"] = cfg

    with patch("tabsdatak.conn.clickhouse._impl.write_via_bulk_loader", _capture):
        _ClickHouseDestPlugin().write_out(
            MagicMock(), MagicMock(), dest, [pl.LazyFrame({"id": [1]})]
        )
    return captured["cfg"].schema_evolution


@pytest.mark.parametrize("disposition", ["append", "replace"])
@pytest.mark.parametrize("policy", [None, "strict", "update"])
def test_the_plugin_writes_with_the_resolved_policy(disposition, policy):
    expected = matrix.resolve("clickhouse", disposition, policy).schema_evolution
    assert _written_with(_dest(disposition, policy, None)) == expected

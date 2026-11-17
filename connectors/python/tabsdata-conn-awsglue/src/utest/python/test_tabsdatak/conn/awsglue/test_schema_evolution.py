#
# Copyright 2026 Tabsdata Inc.
#

"""`AwsGlueDest` against the schema evolution matrix: every disposition and
policy a user can write either registers with the resolution the matrix gives,
or is refused."""

from __future__ import annotations

import inspect

import pytest

from tabsdatak.conn.awsglue import AwsGlueDest
from tabsdatak.conn.common.error import ConnInitException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema import matrix
from tabsdatak.conn.common.sql.error import SqlCommonValidateException

pytestmark = pytest.mark.conn_awsglue


def _dest(disposition: str, policy: str | None) -> AwsGlueDest:
    kwargs: dict = dict(tables=["t"], if_table_exists=disposition)
    if policy is not None:
        kwargs["schema_evolution"] = policy
    return AwsGlueDest(**kwargs)


_COMBINATIONS = [
    (disposition, policy)
    for disposition in matrix.DISPOSITIONS
    for policy in (None, "strict", "update")
]


@pytest.mark.parametrize("disposition,policy", _COMBINATIONS)
def test_a_written_combination_registers_as_the_matrix_resolves_it(disposition, policy):
    try:
        expected = matrix.resolve("awsglue", disposition, policy)
    except SqlCommonValidateException:
        expected = None
    if expected is not None:
        assert _dest(disposition, policy)._materialized() == expected.options()
        return
    with pytest.raises(ConnInitException):
        _dest(disposition, policy)


def test_the_matrix_offers_no_disposition_the_destination_cannot_be_written_with():
    offered = {
        disposition
        for disposition, row in matrix.MATRIX["awsglue"].rows.items()
        if row.status == "offered"
    }
    assert offered == {"append", "replace"}


def test_a_left_out_policy_resolves_to_the_matrix_default():
    for disposition in ("append", "replace"):
        row = matrix.MATRIX["awsglue"].rows[disposition]
        resolved = _dest(disposition, None)._evolution()
        assert resolved.schema_evolution == row.default
        assert resolved.engine == row.engine


def test_a_round_trip_keeps_the_resolution():
    for disposition in ("append", "replace"):
        dest = _dest(disposition, "update")
        again = AwsGlueDest.from_json(dest.to_json())
        assert again._materialized() == dest._materialized()


def test_the_docstring_carries_the_matrix_of_this_destination():
    docstring = inspect.cleandoc(AwsGlueDest.__doc__)
    assert matrix.docstring_section("awsglue") in docstring

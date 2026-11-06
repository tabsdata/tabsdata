#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.databricks._types import can_widen, same_base

pytestmark = pytest.mark.databricks


@pytest.mark.parametrize(
    "old,new,widens",
    [
        ("decimal(10,2)", "decimal(12,2)", True),
        ("decimal(10,2)", "decimal(12,4)", True),
        ("decimal(10,2)", "decimal(10,4)", False),
        ("decimal(12,2)", "decimal(10,2)", False),
        ("int", "bigint", False),
    ],
)
def test_a_decimal_widens_when_it_keeps_every_digit(old, new, widens):
    assert can_widen(old, new) is widens


@pytest.mark.parametrize(
    "old,new,same",
    [
        ("decimal(10,2)", "decimal(20,4)", True),
        ("DECIMAL(10,2)", "decimal(10,2)", True),
        ("int", "bigint", False),
        ("array<int>", "array<bigint>", True),
    ],
)
def test_same_base_compares_the_type_without_its_parameters(old, new, same):
    assert same_base(old, new) is same

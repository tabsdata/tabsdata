#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.databricks._types import can_widen

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


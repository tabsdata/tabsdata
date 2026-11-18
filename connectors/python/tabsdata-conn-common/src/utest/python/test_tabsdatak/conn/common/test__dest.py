#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common._dest import present

pytestmark = [pytest.mark.unit]


def test_present_keeps_the_positions_of_the_tables_that_are_not_none():
    assert present(["a", None, "c"], ["t0", "t1", "t2"], [0, 1, 2]) == (
        ["a", "c"],
        ["t0", "t2"],
        [0, 2],
    )


def test_present_keeps_nothing_when_every_table_is_none():
    assert present([None, None], ["t0", "t1"]) == ([], [])


def test_present_keeps_every_table_when_none_is_none():
    assert present(["a", "b"]) == (["a", "b"],)


def test_present_refuses_values_not_aligned_with_the_tables():
    with pytest.raises(ValueError):
        present(["a", None], ["t0"])

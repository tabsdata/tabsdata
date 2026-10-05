#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._size import size_of_values


def test_nothing_weighs_nothing():
    assert size_of_values(None) == 0


def test_an_empty_mapping_weighs_its_container():
    assert size_of_values({}) == sys.getsizeof({})


def test_the_values_are_counted_one_level_deep():
    small = size_of_values({"a": "x"})
    large = size_of_values({"a": "x" * 1000})
    assert large - small == sys.getsizeof("x" * 1000) - sys.getsizeof("x")

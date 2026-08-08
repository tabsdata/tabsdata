#
# Copyright 2026 Tabsdata Inc.
#

"""Grok parsing for `TableFrame`: patterns that turn text into columns.

`TableFrame.grok` matches a Grok pattern against a string column and adds
one column per named capture in it. The `schema` it takes maps each
capture name to the `ColumnDefinition` describing the column that capture
produces -- its name and its `ColumnDataType`.
"""

from __future__ import annotations

from tabsdatak.tableframe.grok._schema import ColumnDataType, ColumnDefinition

__all__ = [
    "ColumnDataType",
    "ColumnDefinition",
]

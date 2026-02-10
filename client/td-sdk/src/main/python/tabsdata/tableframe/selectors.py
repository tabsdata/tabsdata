#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence
from datetime import timezone
from typing import Union

import polars as pl

# noinspection PyProtectedMember
from polars._typing import PythonDataType, TimeUnit

import tabsdata.tableframe.expr.expr as td_expr
from tabsdata._utils.annotations import pydoc

# noinspection PyProtectedMember
from tabsdata._utils.tableframe import _constants as td_constants
from tabsdata.extensions._tableframe.extension import SystemColumns

# noinspection PyProtectedMember
from tabsdata.tableframe import typing as td_typing


# noinspection PyPep8Naming
class SelectorProxy(td_expr.Expr):
    # noinspection PyProtectedMember
    def __init__(self, expr: Union[pl.Expr, td_expr.Expr]):
        super().__init__(expr)


def _exclude_system_columns() -> pl.Expr:
    selector = pl.selectors.exclude(
        pl.selectors.starts_with(td_constants.TD_COLUMN_PREFIX)
    )
    for prefix in td_constants.TD_NAMESPACED_VIRTUAL_COLUMN_PREFIXES:
        selector = selector | pl.selectors.starts_with(prefix)
    return selector


"""
Selectors by position.
"""


# noinspection PyShadowingBuiltins
@pydoc(categories="projection")
def all() -> SelectorProxy:
    """
    Select all columns in a TableFrame.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Name": ["a", "b", "c", "d", "e", "f", "g", "h"]
        ... })

        Original:
        ┌─────┬──────┐
        │ Id  ┆ Name │
        │ --- ┆ ---  │
        │ i64 ┆ str  │
        ╞═════╪══════╡
        │ 1   ┆ "a"  │
        │ 2   ┆ "b"  │
        │ 3   ┆ "c"  │
        │ 4   ┆ "d"  │
        │ 5   ┆ "e"  │
        │ 6   ┆ "f"  │
        │ 7   ┆ "g"  │
        │ 8   ┆ "h"  │
        └─────┴──────┘

        >>> tf.select(td_tf.selectors.all())

        Selected:
        ┌─────┬──────┐
        │ Id  ┆ Name │
        │ --- ┆ ---  │
        │ i64 ┆ str  │
        ╞═════╪══════╡
        │ 1   ┆ "a"  │
        │ 2   ┆ "b"  │
        │ 3   ┆ "c"  │
        │ 4   ┆ "d"  │
        │ 5   ┆ "e"  │
        │ 6   ┆ "f"  │
        │ 7   ┆ "g"  │
        │ 8   ┆ "h"  │
        └─────┴──────┘
    """
    return SelectorProxy(pl.selectors.all() & _exclude_system_columns())


@pydoc(categories="projection")
def first() -> SelectorProxy:
    """
    Select the first column in the TableFrame.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [101, 102, 103, 104, 105, 106, 107, 108],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "Score": [90.5, 84.0, 87.0, 91.5, 86.5, 88.0, 85.5, 89.0]
        ... })

        Original:
        ┌─────┬──────┬───────┐
        │ Id  ┆ Name ┆ Score │
        │ --- ┆ ---  ┆ ---   │
        │ i64 ┆ str  ┆ f64   │
        ╞═════╪══════╪═══════╡
        │ 101 ┆ "A"  ┆ 90.5  │
        │ 102 ┆ "B"  ┆ 84.0  │
        │ 103 ┆ "C"  ┆ 87.0  │
        │ 104 ┆ "D"  ┆ 91.5  │
        │ 105 ┆ "E"  ┆ 86.5  │
        │ 106 ┆ "F"  ┆ 88.0  │
        │ 107 ┆ "G"  ┆ 85.5  │
        │ 108 ┆ "H"  ┆ 89.0  │
        └─────┴──────┴───────┘

        >>> tf.select(td_tf.selectors.first())

        Selected:
        ┌─────┐
        │ Id  │
        │ --- │
        │ i64 │
        ╞═════╡
        │ 101 │
        │ 102 │
        │ 103 │
        │ 104 │
        │ 105 │
        │ 106 │
        │ 107 │
        │ 108 │
        └─────┘
    """
    return SelectorProxy(pl.selectors.first() & _exclude_system_columns())


@pydoc(categories="projection")
def last() -> SelectorProxy:
    """
    Select the last column in the TableFrame.

    Useful when working with dynamically generated schemas where the column
    order is not fixed.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "Score": [95.0, 88.5, 92.0, 76.0, 84.5, 79.0, 91.0, 87.5]
        ... })

        Original:
        ┌────┬──────┬───────┐
        │ Id ┆ Name ┆ Score │
        │ ---┆ ---  ┆ ---   │
        │ i64┆ str  ┆ f64   │
        ╞════╪══════╪═══════╡
        │ 1  ┆ "A"  ┆ 95.0  │
        │ 2  ┆ "B"  ┆ 88.5  │
        │ 3  ┆ "C"  ┆ 92.0  │
        │ 4  ┆ "D"  ┆ 76.0  │
        │ 5  ┆ "E"  ┆ 84.5  │
        │ 6  ┆ "F"  ┆ 79.0  │
        │ 7  ┆ "G"  ┆ 91.0  │
        │ 8  ┆ "H"  ┆ 87.5  │
        └────┴──────┴───────┘

        >>> tf.select(td_tf.selectors.last())

        Selected:
        ┌───────┐
        │ Score │
        │ ---   │
        │ f64   │
        ╞═══════╡
        │ 95.0  │
        │ 88.5  │
        │ 92.0  │
        │ 76.0  │
        │ 84.5  │
        │ 79.0  │
        │ 91.0  │
        │ 87.5  │
        └───────┘
    """
    return SelectorProxy(
        pl.selectors.by_index(-(len(SystemColumns) + 1)) & _exclude_system_columns()
    )


@pydoc(categories="projection")
def by_index(*indices: int | range | Sequence[int | range]) -> SelectorProxy:
    """
    Select columns by their position in the TableFrame.

    Parameters:
        indices: One or more integer positions or ranges. Negative indexes are
        supported, ranging from -1 (indicating last column) to
        -{number of columns} (indicating first column). Indices greater (in
        absolute value) than the number of columns will fail.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "IsActive": [True, False, True, True, False, True, False, True],
        ...     "Score": [90.0, 85.5, 88.0, 91.0, 86.0, 87.5, 84.0, 89.5]
        ... })

        Original:
        ┌──────┬──────┬──────────┬───────┐
        │ Id   ┆ Name ┆ IsActive ┆ Score │
        │ ---  ┆ ---  ┆ ---      ┆ ---   │
        │ i64  ┆ str  ┆ bool     ┆ f64   │
        ╞══════╪══════╪══════════╪═══════╡
        │  1   ┆ "A"  ┆ true     ┆ 90.0  │
        │  2   ┆ "B"  ┆ false    ┆ 85.5  │
        │  3   ┆ "C"  ┆ true     ┆ 88.0  │
        │  4   ┆ "D"  ┆ true     ┆ 91.0  │
        │  5   ┆ "E"  ┆ false    ┆ 86.0  │
        │  6   ┆ "F"  ┆ true     ┆ 87.5  │
        │  7   ┆ "G"  ┆ false    ┆ 84.0  │
        │  8   ┆ "H"  ┆ true     ┆ 89.5  │
        └──────┴──────┴──────────┴───────┘

        >>> tf.select(td_tf.selectors.by_index(1, 3))

        Selected:
        ┌──────┬───────┐
        │ Name ┆ Score │
        │ ---  ┆ ---   │
        │ str  ┆ f64   │
        ╞══════╪═══════╡
        │ "A"  ┆ 90.0  │
        │ "B"  ┆ 85.5  │
        │ "C"  ┆ 88.0  │
        │ "D"  ┆ 91.0  │
        │ "E"  ┆ 86.0  │
        │ "F"  ┆ 87.5  │
        │ "G"  ┆ 84.0  │
        │ "H"  ┆ 89.5  │
        └──────┴───────┘
    """
    system_columns_length = len(SystemColumns)

    def fix(i: int) -> int:
        return i - system_columns_length if i < 0 else i

    normalized_indices = []
    for index in indices:
        if isinstance(index, int):
            normalized_indices.append(fix(index))
        elif isinstance(index, range):
            normalized_indices.extend(fix(i) for i in index)
        elif isinstance(index, Iterable):
            for sub_index in index:
                if isinstance(sub_index, int):
                    normalized_indices.append(fix(sub_index))
                elif isinstance(sub_index, range):
                    normalized_indices.extend(fix(i) for i in sub_index)
                else:
                    raise TypeError(f"Unsupported nested type: {type(sub_index)}")
        else:
            raise TypeError(f"Unsupported index type: {type(index)}")

    return SelectorProxy(
        pl.selectors.by_index(*normalized_indices) & _exclude_system_columns()
    )


"""
Selectors by name.
"""


@pydoc(categories="projection")
def by_name(*names: str | Collection[str], require_all: bool = True) -> SelectorProxy:
    """
    Select all columns whose names match any given names.

    Parameters:
        names: One or more column names to include.
        require_all: If True, raises an error if any name is missing.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [10, 11, 12, 13, 14, 15, 16, 17],
        ...     "Name": ["Anna", "Ben", "Cara", "Dean", "Ella", "Finn", "Gina", "Hugo"],
        ...     "Country": ["US", "UK", "FR", "DE", "IT", "ES", "NL", "SE"],
        ...     "Score": [91.0, 84.5, 88.0, 90.5, 86.0, 87.0, 82.5, 89.5]
        ... })

        Original:
        ┌──────┬────────┬─────────┬───────┐
        │ Id   ┆ Name   ┆ Country ┆ Score │
        │ ---  ┆ ---    ┆ ---     ┆ ---   │
        │ i64  ┆ str    ┆ str     ┆ f64   │
        ╞══════╪════════╪═════════╪═══════╡
        │ 10   ┆ "Anna" ┆ "US"    ┆ 91.0  │
        │ 11   ┆ "Ben"  ┆ "UK"    ┆ 84.5  │
        │ 12   ┆ "Cara" ┆ "FR"    ┆ 88.0  │
        │ 13   ┆ "Dean" ┆ "DE"    ┆ 90.5  │
        │ 14   ┆ "Ella" ┆ "IT"    ┆ 86.0  │
        │ 15   ┆ "Finn" ┆ "ES"    ┆ 87.0  │
        │ 16   ┆ "Gina" ┆ "NL"    ┆ 82.5  │
        │ 17   ┆ "Hugo" ┆ "SE"    ┆ 89.5  │
        └──────┴────────┴─────────┴───────┘

        >>> tf.select(td_tf.selectors.by_name("Name", "Score"))

        Selected:
        ┌────────┬───────┐
        │ Name   ┆ Score │
        │ ---    ┆ ---   │
        │ str    ┆ f64   │
        ╞════════╪═══════╡
        │ "Anna" ┆ 91.0  │
        │ "Ben"  ┆ 84.5  │
        │ "Cara" ┆ 88.0  │
        │ "Dean" ┆ 90.5  │
        │ "Ella" ┆ 86.0  │
        │ "Finn" ┆ 87.0  │
        │ "Gina" ┆ 82.5  │
        │ "Hugo" ┆ 89.5  │
        └────────┴───────┘
    """
    return SelectorProxy(
        pl.selectors.by_name(*names, require_all=require_all)
        & _exclude_system_columns()
    )


@pydoc(categories="projection")
def contains(*substring: str) -> SelectorProxy:
    """
    Select all columns whose names contain one or more of the given substrings.

    Parameters:
        substring: One or more substrings to search for in column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "UserId": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "UserName": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "Age": [22, 34, 28, 45, 30, 33, 26, 29],
        ...     "Email": ["a@x", "b@x", "c@x", "d@x", "e@x", "f@x", "g@x", "h@x"]
        ... })

        Original:
        ┌────────┬──────────┬─────┬───────┐
        │ UserId ┆ UserName ┆ Age ┆ Email │
        ├────────┼──────────┼─────┼───────┤
        │ 1      ┆ "A"      ┆ 22  ┆ "a@x" │
        │ 2      ┆ "B"      ┆ 34  ┆ "b@x" │
        │ 3      ┆ "C"      ┆ 28  ┆ "c@x" │
        │ 4      ┆ "D"      ┆ 45  ┆ "d@x" │
        │ 5      ┆ "E"      ┆ 30  ┆ "e@x" │
        │ 6      ┆ "F"      ┆ 33  ┆ "f@x" │
        │ 7      ┆ "G"      ┆ 26  ┆ "g@x" │
        │ 8      ┆ "H"      ┆ 29  ┆ "h@x" │
        └────────┴──────────┴─────┴───────┘

        >>> tf.select(td_tf.selectors.contains("Name", "Id"))

        Selected:
        ┌────────┬──────────┐
        │ UserId ┆ UserName │
        ├────────┼──────────┤
        │ 1      ┆ "A"      │
        │ 2      ┆ "B"      │
        │ 3      ┆ "C"      │
        │ 4      ┆ "D"      │
        │ 5      ┆ "E"      │
        │ 6      ┆ "F"      │
        │ 7      ┆ "G"      │
        │ 8      ┆ "H"      │
        └────────┴──────────┘
    """
    return SelectorProxy(pl.selectors.contains(*substring) & _exclude_system_columns())


@pydoc(categories="projection")
def starts_with(*prefix: str) -> SelectorProxy:
    """
    Select all columns whose names start with any of the given prefixes.

    Parameters:
        prefix: One or more string prefixes to match at the beginning of column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "PreName": ["Alice", "Bob", "Caro", "Dan", "Eve", "Fay", "Gus", "Hana"],
        ...     "PreScore": [90, 85, 78, 92, 88, 91, 84, 89],
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Note": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌──────────┬──────────┬────┬──────┐
        │ PreName  ┆ PreScore ┆ Id ┆ Note │
        │ ---      ┆ ---      ┆ ---┆ ---  │
        │ str      ┆ i64      ┆ i64┆ str  │
        ╞══════════╪══════════╪════╪══════╡
        │ "Alice"  ┆ 90       ┆ 1  ┆ "A"  │
        │ "Bob"    ┆ 85       ┆ 2  ┆ "B"  │
        │ "Caro"   ┆ 78       ┆ 3  ┆ "C"  │
        │ "Dan"    ┆ 92       ┆ 4  ┆ "D"  │
        │ "Eve"    ┆ 88       ┆ 5  ┆ "E"  │
        │ "Fay"    ┆ 91       ┆ 6  ┆ "F"  │
        │ "Gus"    ┆ 84       ┆ 7  ┆ "G"  │
        │ "Hana"   ┆ 89       ┆ 8  ┆ "H"  │
        └──────────┴──────────┴────┴──────┘

        >>> tf.select(td_tf.selectors.starts_with("Pre"))

        Selected:
        ┌──────────┬──────────┐
        │ PreName  ┆ PreScore │
        │ ---      ┆ ---      │
        │ str      ┆ i64      │
        ╞══════════╪══════════╡
        │ "Alice"  ┆ 90       │
        │ "Bob"    ┆ 85       │
        │ "Caro"   ┆ 78       │
        │ "Dan"    ┆ 92       │
        │ "Eve"    ┆ 88       │
        │ "Fay"    ┆ 91       │
        │ "Gus"    ┆ 84       │
        │ "Hana"   ┆ 89       │
        └──────────┴──────────┘
    """
    return SelectorProxy(pl.selectors.starts_with(*prefix) & _exclude_system_columns())


@pydoc(categories="projection")
def ends_with(*suffix: str) -> SelectorProxy:
    """
    Select all columns whose names end with any of the given suffixes.

    Parameters:
        suffix: One or more string suffixes to match at the end of column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "UserId": [100, 101, 102, 103, 104, 105, 106, 107],
        ...     "SessionId": [201, 202, 203, 204, 205, 206, 207, 208],
        ...     "Value": [10.5, 20.0, 15.5, 12.0, 18.5, 17.0, 16.0, 19.5]
        ... })

        Original:
        ┌──────┬────────┬───────────┬───────┐
        │ Id   ┆ UserId ┆ SessionId ┆ Value │
        │ ---  ┆ ---    ┆ ---       ┆ ---   │
        │ i64  ┆ i64    ┆ i64       ┆ f64   │
        ╞══════╪════════╪═══════════╪═══════╡
        │ 1    ┆ 100    ┆ 201       ┆ 10.5  │
        │ 2    ┆ 101    ┆ 202       ┆ 20.0  │
        │ 3    ┆ 102    ┆ 203       ┆ 15.5  │
        │ 4    ┆ 103    ┆ 204       ┆ 12.0  │
        │ 5    ┆ 104    ┆ 205       ┆ 18.5  │
        │ 6    ┆ 105    ┆ 206       ┆ 17.0  │
        │ 7    ┆ 106    ┆ 207       ┆ 16.0  │
        │ 8    ┆ 107    ┆ 208       ┆ 19.5  │
        └──────┴────────┴───────────┴───────┘

        >>> tf.select(td_tf.selectors.ends_with("Id"))

        Selected:
        ┌──────┬────────┬───────────┐
        │ Id   ┆ UserId ┆ SessionId │
        │ ---  ┆ ---    ┆ ---       │
        │ i64  ┆ i64    ┆ i64       │
        ╞══════╪════════╪═══════════╡
        │ 1    ┆ 100    ┆ 201       │
        │ 2    ┆ 101    ┆ 202       │
        │ 3    ┆ 102    ┆ 203       │
        │ 4    ┆ 103    ┆ 204       │
        │ 5    ┆ 104    ┆ 205       │
        │ 6    ┆ 105    ┆ 206       │
        │ 7    ┆ 106    ┆ 207       │
        │ 8    ┆ 107    ┆ 208       │
        └──────┴────────┴───────────┘
    """
    return SelectorProxy(pl.selectors.ends_with(*suffix) & _exclude_system_columns())


@pydoc(categories="projection")
def matches(pattern: str) -> SelectorProxy:
    """
    Select all columns whose names match a regular expression pattern.

    Parameters:
        pattern: A regular expression pattern to match against column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "ColA": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "ColB": [8, 7, 6, 5, 4, 3, 2, 1],
        ...     "Data2024": [10, 20, 30, 40, 50, 60, 70, 80],
        ...     "Meta": ["a", "b", "c", "d", "e", "f", "g", "h"]
        ... })

        Original:
        ┌──────┬──────┬──────────┬──────┐
        │ ColA ┆ ColB ┆ Data2024 ┆ Meta │
        │ ---  ┆ ---  ┆ ---      ┆ ---  │
        │ i64  ┆ i64  ┆ i64      ┆ str  │
        ╞══════╪══════╪══════════╪══════╡
        │ 1    ┆ 8    ┆ 10       ┆ "a"  │
        │ 2    ┆ 7    ┆ 20       ┆ "b"  │
        │ 3    ┆ 6    ┆ 30       ┆ "c"  │
        │ 4    ┆ 5    ┆ 40       ┆ "d"  │
        │ 5    ┆ 4    ┆ 50       ┆ "e"  │
        │ 6    ┆ 3    ┆ 60       ┆ "f"  │
        │ 7    ┆ 2    ┆ 70       ┆ "g"  │
        │ 8    ┆ 1    ┆ 80       ┆ "h"  │
        └──────┴──────┴──────────┴──────┘

        >>> tf.select(td_tf.selectors.matches(r"^Col"))

        Selected:
        ┌──────┬──────┐
        │ ColA ┆ ColB │
        │ ---  ┆ ---  │
        │ i64  ┆ i64  │
        ╞══════╪══════╡
        │ 1    ┆ 8    │
        │ 2    ┆ 7    │
        │ 3    ┆ 6    │
        │ 4    ┆ 5    │
        │ 5    ┆ 4    │
        │ 6    ┆ 3    │
        │ 7    ┆ 2    │
        │ 8    ┆ 1    │
        └──────┴──────┘
    """
    return SelectorProxy(
        pl.selectors.matches(pattern=pattern) & _exclude_system_columns()
    )


@pydoc(categories="projection")
def alpha(ascii_only: bool = False, *, ignore_spaces: bool = False) -> SelectorProxy:
    """
    Select all columns with names made up of only alphabetic characters.

    Parameters:
        ascii_only: Only consider alphabetic the ASCII letters.
        ignore_spaces: Consider only non-white characters in column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Name": ["Alice", "Bob", "Caro", "Dan", "Eve", "Fay", "Gus", "Hana"],
        ...     "Score": [87, 92, 78, 95, 88, 91, 76, 84],
        ...     "Id1": [101, 102, 103, 104, 105, 106, 107, 108]
        ... })

        Original:
        ┌─────────┬───────┬───────┐
        │ Name    ┆ Score ┆ Id1   │
        │ ---     ┆ ---   ┆ ---   │
        │ str     ┆ i64   ┆ i64   │
        ╞═════════╪═══════╪═══════╡
        │ "Alice" ┆  87   ┆  101  │
        │ "Bob"   ┆  92   ┆  102  │
        │ "Caro"  ┆  78   ┆  103  │
        │ "Dan"   ┆  95   ┆  104  │
        │ "Eve"   ┆  88   ┆  105  │
        │ "Fay"   ┆  91   ┆  106  │
        │ "Gus"   ┆  76   ┆  107  │
        │ "Hana"  ┆  84   ┆  108  │
        └─────────┴───────┴───────┘

        >>> tf.select(td_tf.selectors.alpha())

        Selected:
        ┌─────────┬───────┐
        │ Name    ┆ Score │
        │ ---     ┆ ---   │
        │ str     ┆ i64   │
        ╞═════════╪═══════╡
        │ "Alice" ┆  87   │
        │ "Bob"   ┆  92   │
        │ "Caro"  ┆  78   │
        │ "Dan"   ┆  95   │
        │ "Eve"   ┆  88   │
        │ "Fay"   ┆  91   │
        │ "Gus"   ┆  76   │
        │ "Hana"  ┆  84   │
        └─────────┴───────┘
    """
    return SelectorProxy(
        pl.selectors.alpha(ascii_only=ascii_only, ignore_spaces=ignore_spaces)
        & _exclude_system_columns()
    )


@pydoc(categories="projection")
def alphanumeric(
    ascii_only: bool = False,
    *,
    ignore_spaces: bool = False,
) -> SelectorProxy:
    """
    Select all columns whose names contain only letters and digits.

    Parameters:
        ascii_only: Only consider alphabetic the ASCII letters.
        ignore_spaces: Consider only non-white characters in column names.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "User01": [101, 102, 103, 104, 105, 106, 107, 108],
        ...     "Col_2": [8, 7, 6, 5, 4, 3, 2, 1],
        ...     "Data3": [10, 20, 30, 40, 50, 60, 70, 80],
        ...     "Label": ["a", "b", "c", "d", "e", "f", "g", "h"]
        ... })

        Original:
        ┌────────┬───────┬───────┬───────┐
        │ User01 ┆ Col_2 ┆ Data3 ┆ Label │
        │ ---    ┆ ---   ┆ ---   ┆ ---   │
        │ i64    ┆ i64   ┆ i64   ┆ str   │
        ╞════════╪═══════╪═══════╪═══════╡
        │  101   ┆   8   ┆  10   ┆ "a"   │
        │  102   ┆   7   ┆  20   ┆ "b"   │
        │  103   ┆   6   ┆  30   ┆ "c"   │
        │  104   ┆   5   ┆  40   ┆ "d"   │
        │  105   ┆   4   ┆  50   ┆ "e"   │
        │  106   ┆   3   ┆  60   ┆ "f"   │
        │  107   ┆   2   ┆  70   ┆ "g"   │
        │  108   ┆   1   ┆  80   ┆ "h"   │
        └────────┴───────┴───────┴───────┘

        >>> tf.select(td_tf.selectors.alphanumeric())

        Selected:
        ┌────────┬───────┬───────┐
        │ User01 ┆ Data3 ┆ Label │
        │ ---    ┆ ---   ┆ ---   │
        │ i64    ┆ i64   ┆ str   │
        ╞════════╪═══════╪═══════╡
        │  101   ┆  10   ┆ "a"   │
        │  102   ┆  20   ┆ "b"   │
        │  103   ┆  30   ┆ "c"   │
        │  104   ┆  40   ┆ "d"   │
        │  105   ┆  50   ┆ "e"   │
        │  106   ┆  60   ┆ "f"   │
        │  107   ┆  70   ┆ "g"   │
        │  108   ┆  80   ┆ "h"   │
        └────────┴───────┴───────┘
    """
    return SelectorProxy(
        pl.selectors.alphanumeric(ascii_only=ascii_only, ignore_spaces=ignore_spaces)
        & _exclude_system_columns()
    )


@pydoc(categories="projection")
def digit(ascii_only: bool = False) -> SelectorProxy:
    """
    Select all columns whose names consist only of digit characters.

    Parameters:
        ascii_only: Restrict matching to ASCII digits ("0"–"9") only, instead of
                    all Unicode digits (`\\d').

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "2023": [100, 200, 300, 400, 500, 600, 700, 800],
        ...     "Temp": [20.5, 21.0, 22.0, 20.0, 19.5, 18.0, 21.5, 22.5],
        ...     "01": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Humidity": [60, 65, 55, 70, 75, 80, 68, 66]
        ... })

        Original:
        ┌──────┬──────┬────┬──────────┐
        │ 2023 ┆ Temp ┆ 01 ┆ Humidity │
        │ ---  ┆ ---  ┆ -- ┆ ---      │
        │ i64  ┆ f64  ┆ i64┆ i64      │
        ╞══════╪══════╪════╪══════════╡
        │ 100  ┆ 20.5 ┆ 1  ┆ 60       │
        │ 200  ┆ 21.0 ┆ 2  ┆ 65       │
        │ 300  ┆ 22.0 ┆ 3  ┆ 55       │
        │ 400  ┆ 20.0 ┆ 4  ┆ 70       │
        │ 500  ┆ 19.5 ┆ 5  ┆ 75       │
        │ 600  ┆ 18.0 ┆ 6  ┆ 80       │
        │ 700  ┆ 21.5 ┆ 7  ┆ 68       │
        │ 800  ┆ 22.5 ┆ 8  ┆ 66       │
        └──────┴──────┴────┴──────────┘

        >>> tf.select(td_tf.selectors.digit())

        Selected:
        ┌──────┬─────┐
        │ 2023 ┆ 01  │
        │ ---  ┆ --  │
        │ i64  ┆ i64 │
        ╞══════╪═════╡
        │ 100  ┆ 1   │
        │ 200  ┆ 2   │
        │ 300  ┆ 3   │
        │ 400  ┆ 4   │
        │ 500  ┆ 5   │
        │ 600  ┆ 6   │
        │ 700  ┆ 7   │
        │ 800  ┆ 8   │
        └──────┴─────┘
    """
    return SelectorProxy(
        pl.selectors.digit(ascii_only=ascii_only) & _exclude_system_columns()
    )


"""
Selectors by data type.
"""


@pydoc(categories="projection")
def by_dtype(
    *dtypes: (
        td_typing.DataType
        | PythonDataType
        | Iterable[td_typing.DataType]
        | Iterable[PythonDataType]
    ),
) -> SelectorProxy:
    """
    Select all columns of the specified data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "IsActive": [True, True, False, True, False, True, True, False],
        ...     "Score": [91.0, 85.5, 78.0, 88.5, 90.0, 76.0, 82.5, 89.0]
        ... })

        Original:
        ┌──────┬──────┬──────────┬───────┐
        │ Id   ┆ Name ┆ IsActive ┆ Score │
        │ ---  ┆ ---  ┆ ---      ┆ ---   │
        │ i64  ┆ str  ┆ bool     ┆ f64   │
        ╞══════╪══════╪══════════╪═══════╡
        │  1   ┆ "A"  ┆ true     ┆ 91.0  │
        │  2   ┆ "B"  ┆ true     ┆ 85.5  │
        │  3   ┆ "C"  ┆ false    ┆ 78.0  │
        │  4   ┆ "D"  ┆ true     ┆ 88.5  │
        │  5   ┆ "E"  ┆ false    ┆ 90.0  │
        │  6   ┆ "F"  ┆ true     ┆ 76.0  │
        │  7   ┆ "G"  ┆ true     ┆ 82.5  │
        │  8   ┆ "H"  ┆ false    ┆ 89.0  │
        └──────┴──────┴──────────┴───────┘

        >>> tf.select(td_tf.selectors.by_dtype(td_tf.Float64))

        Selected:
        ┌───────┐
        │ Score │
        │ ---   │
        │ f64   │
        ╞═══════╡
        │ 91.0  │
        │ 85.5  │
        │ 78.0  │
        │ 88.5  │
        │ 90.0  │
        │ 76.0  │
        │ 82.5  │
        │ 89.0  │
        └───────┘
    """
    return SelectorProxy(pl.selectors.by_dtype(*dtypes) & _exclude_system_columns())


"""
Selectors by abstract data types.
"""


@pydoc(categories="projection")
def integer() -> SelectorProxy:
    """
    Select all columns with integer data types.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Score": [91.0, 85.5, 88.0, 90.0, 87.0, 89.5, 84.0, 86.0],
        ...     "Level": [3, 2, 1, 3, 2, 1, 3, 2],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌────┬───────┬───────┬──────┐
        │ Id ┆ Score ┆ Level ┆ Name │
        │ ---┆ ---   ┆ ---   ┆ ---  │
        │ i64┆ f64   ┆ i64   ┆ str  │
        ╞════╪═══════╪═══════╪══════╡
        │ 1  ┆ 91.0  ┆ 3     ┆ "A"  │
        │ 2  ┆ 85.5  ┆ 2     ┆ "B"  │
        │ 3  ┆ 88.0  ┆ 1     ┆ "C"  │
        │ 4  ┆ 90.0  ┆ 3     ┆ "D"  │
        │ 5  ┆ 87.0  ┆ 2     ┆ "E"  │
        │ 6  ┆ 89.5  ┆ 1     ┆ "F"  │
        │ 7  ┆ 84.0  ┆ 3     ┆ "G"  │
        │ 8  ┆ 86.0  ┆ 2     ┆ "H"  │
        └────┴───────┴───────┴──────┘

        >>> tf.select(td_tf.selectors.integer())

        Selected:
        ┌────┬───────┐
        │ Id ┆ Level │
        │ ---┆ ---   │
        │ i64┆ i64   │
        ╞════╪═══════╡
        │ 1  ┆ 3     │
        │ 2  ┆ 2     │
        │ 3  ┆ 1     │
        │ 4  ┆ 3     │
        │ 5  ┆ 2     │
        │ 6  ┆ 1     │
        │ 7  ┆ 3     │
        │ 8  ┆ 2     │
        └────┴───────┘
    """
    return SelectorProxy(pl.selectors.integer() & _exclude_system_columns())


@pydoc(categories="projection")
def signed_integer() -> SelectorProxy:
    """
    Select all columns of signed integer data types.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "UserId": [10, 11, 12, 13, 14, 15, 16, 17],
        ...     "Rating": [5, 3, 4, 2, 5, 1, 3, 4],
        ...     "Change": [-1, 0, 1, -2, 2, 0, -1, 1],
        ...     "Tag": ["x", "y", "x", "y", "z", "x", "z", "y"]
        ... })

        Original:
        ┌────────┬────────┬────────┬──────┐
        │ UserId ┆ Rating ┆ Change ┆ Tag  │
        │ ---    ┆ ---    ┆ ---    ┆ ---  │
        │ i64    ┆ u8     ┆ i64    ┆ str  │
        ╞════════╪════════╪════════╪══════╡
        │ 10     ┆ 5      ┆ -1     ┆ "x"  │
        │ 11     ┆ 3      ┆ 0      ┆ "y"  │
        │ 12     ┆ 4      ┆ 1      ┆ "x"  │
        │ 13     ┆ 2      ┆ -2     ┆ "y"  │
        │ 14     ┆ 5      ┆ 2      ┆ "z"  │
        │ 15     ┆ 1      ┆ 0      ┆ "x"  │
        │ 16     ┆ 3      ┆ -1     ┆ "z"  │
        │ 17     ┆ 4      ┆ 1      ┆ "y"  │
        └────────┴────────┴────────┴──────┘

        >>> tf.select(td_tf.selectors.signed_integer())

        Selected:
        ┌────────┬────────┐
        │ UserId ┆ Change │
        │ ---    ┆ ---    │
        │ i64    ┆ i64    │
        ╞════════╪════════╡
        │ 10     ┆ -1     │
        │ 11     ┆ 0      │
        │ 12     ┆ 1      │
        │ 13     ┆ -2     │
        │ 14     ┆ 2      │
        │ 15     ┆ 0      │
        │ 16     ┆ -1     │
        │ 17     ┆ 1      │
        └────────┴────────┘
    """
    return SelectorProxy(pl.selectors.signed_integer() & _exclude_system_columns())


@pydoc(categories="projection")
def unsigned_integer() -> SelectorProxy:
    """
    Select all columns of unsigned integer data types.

    Unsigned integers include types such as UInt8, UInt16, UInt32, and UInt64.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "UserId": [10, 11, 12, 13, 14, 15, 16, 17],
        ...     "Score": [100, 98, 95, 87, 90, 93, 88, 96],
        ...     "Flags": [1, 0, 1, 0, 1, 0, 1, 0],
        ...     "Change": [-1, 2, -3, 1, 0, -2, 3, -1]
        ... })

        Original:
        ┌────────┬───────┬────────┬────────┐
        │ UserId ┆ Score ┆ Flags  ┆ Change │
        │ ---    ┆ ---   ┆ ---    ┆ ---    │
        │ u64    ┆ u16   ┆ u8     ┆ i64    │
        ╞════════╪═══════╪════════╪════════╡
        │ 10     ┆ 100   ┆ 1      ┆ -1     │
        │ 11     ┆ 98    ┆ 0      ┆ 2      │
        │ 12     ┆ 95    ┆ 1      ┆ -3     │
        │ 13     ┆ 87    ┆ 0      ┆ 1      │
        │ 14     ┆ 90    ┆ 1      ┆ 0      │
        │ 15     ┆ 93    ┆ 0      ┆ -2     │
        │ 16     ┆ 88    ┆ 1      ┆ 3      │
        │ 17     ┆ 96    ┆ 0      ┆ -1     │
        └────────┴───────┴────────┴────────┘

        >>> tf.select(td_tf.selectors.unsigned_integer())

        Selected:
        ┌────────┬───────┬────────┐
        │ UserId ┆ Score ┆ Flags  │
        │ ---    ┆ ---   ┆ ---    │
        │ u64    ┆ u16   ┆ u8     │
        ╞════════╪═══════╪════════╡
        │ 10     ┆ 100   ┆ 1      │
        │ 11     ┆ 98    ┆ 0      │
        │ 12     ┆ 95    ┆ 1      │
        │ 13     ┆ 87    ┆ 0      │
        │ 14     ┆ 90    ┆ 1      │
        │ 15     ┆ 93    ┆ 0      │
        │ 16     ┆ 88    ┆ 1      │
        │ 17     ┆ 96    ┆ 0      │
        └────────┴───────┴────────┘
    """
    return SelectorProxy(pl.selectors.unsigned_integer() & _exclude_system_columns())


# noinspection PyShadowingBuiltins
@pydoc(categories="projection")
def float() -> SelectorProxy:
    """
    Select all columns of float data types.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Score": [91.5, 85.0, 88.5, 90.0, 87.5, 89.0, 84.0, 86.5],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌────┬───────┬──────┐
        │ Id ┆ Score ┆ Name │
        │ ---┆ ---   ┆ ---  │
        │ i64┆ f64   ┆ str  │
        ╞════╪═══════╪══════╡
        │ 1  ┆ 91.5  ┆ "A"  │
        │ 2  ┆ 85.0  ┆ "B"  │
        │ 3  ┆ 88.5  ┆ "C"  │
        │ 4  ┆ 90.0  ┆ "D"  │
        │ 5  ┆ 87.5  ┆ "E"  │
        │ 6  ┆ 89.0  ┆ "F"  │
        │ 7  ┆ 84.0  ┆ "G"  │
        │ 8  ┆ 86.5  ┆ "H"  │
        └────┴───────┴──────┘

        >>> tf.select(td_tf.selectors.float())

        Selected:
        ┌───────┐
        │ Score │
        │ ---   │
        │ f64   │
        ╞═══════╡
        │ 91.5  │
        │ 85.0  │
        │ 88.5  │
        │ 90.0  │
        │ 87.5  │
        │ 89.0  │
        │ 84.0  │
        │ 86.5  │
        └───────┘
    """
    return SelectorProxy(pl.selectors.float() & _exclude_system_columns())


@pydoc(categories="projection")
def numeric() -> SelectorProxy:
    """
    Select all columns of numeric data types.

    This includes integer and float columns of any size or signedness.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Score": [91.5, 85.0, 78.25, 88.75, 90.0, 76.5, 82.25, 89.0],
        ...     "Name": ["Alice", "Bob", "Cara", "Dan", "Eve", "Finn", "Gus", "Hana"],
        ...     "IsActive": [True, False, True, True, False, True, True, False]
        ... })

        Original:
        ┌────┬────────┬────────┬──────────┐
        │ Id ┆ Score  ┆ Name   ┆ IsActive │
        │ ---┆ ---    ┆ ---    ┆ ---      │
        │ i64┆ f64    ┆ str    ┆ bool     │
        ╞════╪════════╪════════╪══════════╡
        │ 1  ┆ 91.5   ┆ "Alice"┆ true     │
        │ 2  ┆ 85.0   ┆ "Bob"  ┆ false    │
        │ 3  ┆ 78.25  ┆ "Cara" ┆ true     │
        │ 4  ┆ 88.75  ┆ "Dan"  ┆ true     │
        │ 5  ┆ 90.0   ┆ "Eve"  ┆ false    │
        │ 6  ┆ 76.5   ┆ "Finn" ┆ true     │
        │ 7  ┆ 82.25  ┆ "Gus"  ┆ true     │
        │ 8  ┆ 89.0   ┆ "Hana" ┆ false    │
        └────┴────────┴────────┴──────────┘

        >>> tf.select(td_tf.selectors.numeric())

        Selected:
        ┌────┬────────┐
        │ Id ┆ Score  │
        │ ---┆ ---    │
        │ i64┆ f64    │
        ╞════╪════════╡
        │ 1  ┆ 91.5   │
        │ 2  ┆ 85.0   │
        │ 3  ┆ 78.25  │
        │ 4  ┆ 88.75  │
        │ 5  ┆ 90.0   │
        │ 6  ┆ 76.5   │
        │ 7  ┆ 82.25  │
        │ 8  ┆ 89.0   │
        └────┴────────┘
    """
    return SelectorProxy(pl.selectors.numeric() & _exclude_system_columns())


@pydoc(categories="projection")
def temporal() -> SelectorProxy:
    """
    Select all columns of temporal data types.

    This includes `datetime`, `date`, `time`, and `timedelta`.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import datetime, date, time, timedelta, timezone
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "CreatedAt": [
        ...         datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 3, 11, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 4, 12, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 5, 13, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 6, 14, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 7, 15, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 8, 16, 0, tzinfo=timezone.utc)
        ...     ],
        ...     "SessionDate": [
        ...         date(2024, 1, 1),
        ...         date(2024, 1, 2),
        ...         date(2024, 1, 3),
        ...         date(2024, 1, 4),
        ...         date(2024, 1, 5),
        ...         date(2024, 1, 6),
        ...         date(2024, 1, 7),
        ...         date(2024, 1, 8)
        ...     ],
        ...     "Duration": [
        ...         timedelta(minutes=0),
        ...         timedelta(minutes=5),
        ...         timedelta(minutes=10),
        ...         timedelta(minutes=15),
        ...         timedelta(minutes=20),
        ...         timedelta(minutes=25),
        ...         timedelta(minutes=30),
        ...         timedelta(minutes=35)
        ...     ],
        ...     "EventTime": [
        ...         time(9, 0),
        ...         time(10, 0),
        ...         time(11, 0),
        ...         time(12, 0),
        ...         time(13, 0),
        ...         time(14, 0),
        ...         time(15, 0),
        ...         time(16, 0)
        ...     ],
        ...     "User": ["Alice", "Bob", "Cara", "Dan", "Eve", "Finn", "Gus", "Hana"]
        ... })

        Original:
        ┌───────────────────────────┬─────────────┬──────────┬───────────┬─────────┐
        │ CreatedAt                 ┆ SessionDate ┆ Duration ┆ EventTime ┆ User    │
        │ ---                       ┆ ---         ┆ ---      ┆ ---       ┆ ---     │
        │ datetime[μs, UTC]         ┆ date        ┆ duration ┆ time      ┆ str     │
        ╞═══════════════════════════╪═════════════╪══════════╪═══════════╪═════════╡
        │ 2024-01-01 09:00:00+00:00 ┆ 2024-01-01  ┆ 0:00:00  ┆ 09:00:00  ┆ "Alice" │
        │ 2024-01-02 10:00:00+00:00 ┆ 2024-01-02  ┆ 0:05:00  ┆ 10:00:00  ┆ "Bob"   │
        │ 2024-01-03 11:00:00+00:00 ┆ 2024-01-03  ┆ 0:10:00  ┆ 11:00:00  ┆ "Cara"  │
        │ 2024-01-04 12:00:00+00:00 ┆ 2024-01-04  ┆ 0:15:00  ┆ 12:00:00  ┆ "Dan"   │
        │ 2024-01-05 13:00:00+00:00 ┆ 2024-01-05  ┆ 0:20:00  ┆ 13:00:00  ┆ "Eve"   │
        │ 2024-01-06 14:00:00+00:00 ┆ 2024-01-06  ┆ 0:25:00  ┆ 14:00:00  ┆ "Finn"  │
        │ 2024-01-07 15:00:00+00:00 ┆ 2024-01-07  ┆ 0:30:00  ┆ 15:00:00  ┆ "Gus"   │
        │ 2024-01-08 16:00:00+00:00 ┆ 2024-01-08  ┆ 0:35:00  ┆ 16:00:00  ┆ "Hana"  │
        └───────────────────────────┴─────────────┴──────────┴───────────┴─────────┘

        >>> tf.select(td_tf.selectors.temporal())

        Selected:
        ┌───────────────────────────┬─────────────┬──────────┬───────────┐
        │ CreatedAt                 ┆ SessionDate ┆ Duration ┆ EventTime │
        │ ---                       ┆ ---         ┆ ---      ┆ ---       │
        │ datetime[μs, UTC]         ┆ date        ┆ duration ┆ time      │
        ╞═══════════════════════════╪═════════════╪══════════╪═══════════╡
        │ 2024-01-01 09:00:00+00:00 ┆ 2024-01-01  ┆ 0:00:00  ┆ 09:00:00  │
        │ 2024-01-02 10:00:00+00:00 ┆ 2024-01-02  ┆ 0:05:00  ┆ 10:00:00  │
        │ 2024-01-03 11:00:00+00:00 ┆ 2024-01-03  ┆ 0:10:00  ┆ 11:00:00  │
        │ 2024-01-04 12:00:00+00:00 ┆ 2024-01-04  ┆ 0:15:00  ┆ 12:00:00  │
        │ 2024-01-05 13:00:00+00:00 ┆ 2024-01-05  ┆ 0:20:00  ┆ 13:00:00  │
        │ 2024-01-06 14:00:00+00:00 ┆ 2024-01-06  ┆ 0:25:00  ┆ 14:00:00  │
        │ 2024-01-07 15:00:00+00:00 ┆ 2024-01-07  ┆ 0:30:00  ┆ 15:00:00  │
        │ 2024-01-08 16:00:00+00:00 ┆ 2024-01-08  ┆ 0:35:00  ┆ 16:00:00  │
        └───────────────────────────┴─────────────┴──────────┴───────────┘
    """
    return SelectorProxy(pl.selectors.temporal() & _exclude_system_columns())


"""
Selectors by concrete data type.
"""


@pydoc(categories="projection")
def binary() -> SelectorProxy:
    """
    Select all columns of binary (bytes) data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "File": [b"A1", b"B2", b"C3", b"D4", b"E5", b"F6", b"G7", b"H8"],
        ...     "Label": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌────────┬───────┐
        │ File   ┆ Label │
        │ ---    ┆ ---   │
        │ binary ┆ str   │
        ╞════════╪═══════╡
        │ b"A1"  ┆ "A"   │
        │ b"B2"  ┆ "B"   │
        │ b"C3"  ┆ "C"   │
        │ b"D4"  ┆ "D"   │
        │ b"E5"  ┆ "E"   │
        │ b"F6"  ┆ "F"   │
        │ b"G7"  ┆ "G"   │
        │ b"H8"  ┆ "H"   │
        └────────┴───────┘

        >>> tf.select(td_tf.selectors.binary())

        Selected:
        ┌────────┐
        │ File   │
        │ ---    │
        │ binary │
        ╞════════╡
        │ b"A1"  │
        │ b"B2"  │
        │ b"C3"  │
        │ b"D4"  │
        │ b"E5"  │
        │ b"F6"  │
        │ b"G7"  │
        │ b"H8"  │
        └────────┘
    """
    return SelectorProxy(pl.selectors.binary() & _exclude_system_columns())


@pydoc(categories="projection")
def boolean() -> SelectorProxy:
    """
    Select all columns of boolean data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "IsValid": [True, False, True, True, False, False, True, False],
        ...     "Score": [95, 88, 92, 76, 84, 79, 91, 87]
        ... })

        Original:
        ┌─────────┬───────┐
        │ IsValid ┆ Score │
        │ ---     ┆ ---   │
        │ bool    ┆ i64   │
        ╞═════════╪═══════╡
        │ true    ┆  95   │
        │ false   ┆  88   │
        │ true    ┆  92   │
        │ true    ┆  76   │
        │ false   ┆  84   │
        │ false   ┆  79   │
        │ true    ┆  91   │
        │ false   ┆  87   │
        └─────────┴───────┘

        >>> tf.select(td_tf.selectors.boolean())

        Selected:
        ┌─────────┐
        │ IsValid │
        │ ---     │
        │ bool    │
        ╞═════════╡
        │ true    │
        │ false   │
        │ true    │
        │ true    │
        │ false   │
        │ false   │
        │ true    │
        │ false   │
        └─────────┘
    """
    return SelectorProxy(pl.selectors.boolean() & _exclude_system_columns())


@pydoc(categories="projection")
def categorical() -> SelectorProxy:
    """
    Select all columns with categorical data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Category": ["A", "B", "A", "C", "B", "A", "C", "B"],
        ...     "Score": [88.0, 92.5, 79.0, 85.0, 91.0, 87.5, 90.0, 86.5]
        ... }).with_columns([
        ...     td_tf.col("Category").cast(td_tf.Categorical)
        ... ])

        Original:
        ┌──────┬──────────┬───────┐
        │ Id   ┆ Category ┆ Score │
        │ ---  ┆ ---      ┆ ---   │
        │ i64  ┆ cat      ┆ f64   │
        ╞══════╪══════════╪═══════╡
        │ 1    ┆ "A"      ┆ 88.0  │
        │ 2    ┆ "B"      ┆ 92.5  │
        │ 3    ┆ "A"      ┆ 79.0  │
        │ 4    ┆ "C"      ┆ 85.0  │
        │ 5    ┆ "B"      ┆ 91.0  │
        │ 6    ┆ "A"      ┆ 87.5  │
        │ 7    ┆ "C"      ┆ 90.0  │
        │ 8    ┆ "B"      ┆ 86.5  │
        └──────┴──────────┴───────┘

        >>> tf.select(td_tf.selectors.categorical())

        Selected:
        ┌──────────┐
        │ Category │
        │ ---      │
        │ cat      │
        ╞══════════╡
        │ "A"      │
        │ "B"      │
        │ "A"      │
        │ "C"      │
        │ "B"      │
        │ "A"      │
        │ "C"      │
        │ "B"      │
        └──────────┘
    """
    return SelectorProxy(pl.selectors.categorical() & _exclude_system_columns())


@pydoc(categories="projection")
def date() -> SelectorProxy:
    """
    Select all columns of date data type.

    This selector matches columns that store calendar dates without time information.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import datetime, timezone, timedelta
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "TimestampUTC": [
        ...         datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 2, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 3, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 4, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 5, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 6, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 7, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 8, tzinfo=timezone.utc),
        ...     ],
        ...     "Event": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "TimestampLocal": [
        ...         datetime(2024, 1, 1, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 2, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 3, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 4, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 5, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 6, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 7, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 8, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        ...     ]
        ... })

        Original:
        ┌──────┬────────────┬───────┐
        │ User ┆ JoinDate   ┆ Score │
        ├──────┼────────────┼───────┤
        │ "A"  ┆ 2023-01-01 ┆ 90    │
        │ "B"  ┆ 2023-01-02 ┆ 85    │
        │ "C"  ┆ 2023-01-03 ┆ 92    │
        │ "D"  ┆ 2023-01-04 ┆ 88    │
        │ "E"  ┆ 2023-01-05 ┆ 87    │
        │ "F"  ┆ 2023-01-06 ┆ 91    │
        │ "G"  ┆ 2023-01-07 ┆ 89    │
        │ "H"  ┆ 2023-01-08 ┆ 86    │
        └──────┴────────────┴───────┘

        >>> tf.select(td_tf.selectors.date())

        Selected:
        ┌────────────┐
        │ JoinDate   │
        ├────────────┤
        │ 2023-01-01 │
        │ 2023-01-02 │
        │ 2023-01-03 │
        │ 2023-01-04 │
        │ 2023-01-05 │
        │ 2023-01-06 │
        │ 2023-01-07 │
        │ 2023-01-08 │
        └────────────┘
    """
    return SelectorProxy(pl.selectors.date() & _exclude_system_columns())


@pydoc(categories="projection")
def datetime(
    time_unit: TimeUnit | Collection[TimeUnit] | None = None,
    time_zone: str | timezone | Collection[str | timezone | None] | None = (
        "*",
        None,
    ),
) -> SelectorProxy:
    """
    Select all columns of datetime data type.

    Parameters:
        time_unit: One or more of the supported time precision units:
                   "ms", "us", or "ns".
                   If omitted, selects datetime columns with any time unit.
        time_zone: Specifies which time zone(s) to match:
                   * Pass one or more valid time zone strings (as defined by the
                     `zoneinfo` module).
                     To list all available zones, use:
                        `import zoneinfo; zoneinfo.available_timezones()`.
                   * Use `None` to select datetime columns with no time zone.
                   * Use "*" to match any column with any time zone.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import datetime, timedelta, timezone
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "TimestampUTC": [
        ...         datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 3, 11, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 4, 12, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 5, 13, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 6, 14, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 7, 15, 0, tzinfo=timezone.utc),
        ...         datetime(2024, 1, 8, 16, 0, tzinfo=timezone.utc),
        ...     ],
        ...     "Event": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "TimestampLocal": [
        ...         datetime(2024, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 2, 11, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 3, 12, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 4, 13, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 5, 14, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 6, 15, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 7, 16, 0, tzinfo=timezone(timedelta(hours=1))),
        ...         datetime(2024, 1, 8, 17, 0, tzinfo=timezone(timedelta(hours=1))),
        ...     ]
        ... })

        Original:
        ┌───────────────────────────┬───────┬───────────────────────────┐
        │ TimestampUTC              ┆ Event ┆ TimestampLocal            │
        │ ---                       ┆ ---   ┆ ---                       │
        │ datetime[μs, UTC]         ┆ str   ┆ datetime[μs, +01:00]      │
        ╞═══════════════════════════╪═══════╪═══════════════════════════╡
        │ 2024-01-01 09:00:00+00:00 ┆ "A"   ┆ 2024-01-01 10:00:00+01:00 │
        │ 2024-01-02 10:00:00+00:00 ┆ "B"   ┆ 2024-01-02 11:00:00+01:00 │
        │ 2024-01-03 11:00:00+00:00 ┆ "C"   ┆ 2024-01-03 12:00:00+01:00 │
        │ 2024-01-04 12:00:00+00:00 ┆ "D"   ┆ 2024-01-04 13:00:00+01:00 │
        │ 2024-01-05 13:00:00+00:00 ┆ "E"   ┆ 2024-01-05 14:00:00+01:00 │
        │ 2024-01-06 14:00:00+00:00 ┆ "F"   ┆ 2024-01-06 15:00:00+01:00 │
        │ 2024-01-07 15:00:00+00:00 ┆ "G"   ┆ 2024-01-07 16:00:00+01:00 │
        │ 2024-01-08 16:00:00+00:00 ┆ "H"   ┆ 2024-01-08 17:00:00+01:00 │
        └───────────────────────────┴───────┴───────────────────────────┘

        >>> tf.select(td_tf.selectors.datetime(time_zone="UTC"))

        Selected:
        ┌───────────────────────────┐
        │ TimestampUTC              │
        │ ---                       │
        │ datetime[μs, UTC]         │
        ╞═══════════════════════════╡
        │ 2024-01-01 09:00:00+00:00 │
        │ 2024-01-02 10:00:00+00:00 │
        │ 2024-01-03 11:00:00+00:00 │
        │ 2024-01-04 12:00:00+00:00 │
        │ 2024-01-05 13:00:00+00:00 │
        │ 2024-01-06 14:00:00+00:00 │
        │ 2024-01-07 15:00:00+00:00 │
        │ 2024-01-08 16:00:00+00:00 │
        └───────────────────────────┘
    """
    return SelectorProxy(
        pl.selectors.datetime(time_unit=time_unit, time_zone=time_zone)
        & _exclude_system_columns()
    )


@pydoc(categories="projection")
def decimal() -> SelectorProxy:
    """
    Select all columns of decimal data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from decimal import Decimal
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Price": [
        ...         Decimal("10.99"), Decimal("15.49"), Decimal("8.75"),
        ...         Decimal("12.00"), Decimal("9.99"), Decimal("20.00"),
        ...         Decimal("7.30"), Decimal("11.45")
        ...     ],
        ...     "Item": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌────────┬──────┐
        │ Price  ┆ Item │
        │ ---    ┆ ---  │
        │ dec    ┆ str  │
        ╞════════╪══════╡
        │ 10.99  ┆ "A"  │
        │ 15.49  ┆ "B"  │
        │ 8.75   ┆ "C"  │
        │ 12.00  ┆ "D"  │
        │ 9.99   ┆ "E"  │
        │ 20.00  ┆ "F"  │
        │ 7.30   ┆ "G"  │
        │ 11.45  ┆ "H"  │
        └────────┴──────┘

        >>> tf.select(td_tf.selectors.decimal())

        Selected:
        ┌────────┐
        │ Price  │
        │ ---    │
        │ dec    │
        ╞════════╡
        │ 10.99  │
        │ 15.49  │
        │ 8.75   │
        │ 12.00  │
        │ 9.99   │
        │ 20.00  │
        │ 7.30   │
        │ 11.45  │
        └────────┘
    """
    return SelectorProxy(pl.selectors.decimal() & _exclude_system_columns())


@pydoc(categories="projection")
def duration(
    time_unit: TimeUnit | Collection[TimeUnit] | None = None,
) -> SelectorProxy:
    """
    Select all columns of duration data type.

    Parameters:
        time_unit: One or more of the supported time precision units:
                   "ms", "us", or "ns".
                   If omitted, selects datetime columns with any time unit.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import timedelta
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Elapsed": [
        ...         timedelta(seconds=5),
        ...         timedelta(seconds=10),
        ...         timedelta(seconds=15),
        ...         timedelta(seconds=20),
        ...         timedelta(seconds=25),
        ...         timedelta(seconds=30),
        ...         timedelta(seconds=35),
        ...         timedelta(seconds=40)
        ...     ],
        ...     "Label": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌───────────┬───────┐
        │ Elapsed   ┆ Label │
        │ ---       ┆ ---   │
        │ duration  ┆ str   │
        ╞═══════════╪═══════╡
        │ 5s        ┆ "A"   │
        │ 10s       ┆ "B"   │
        │ 15s       ┆ "C"   │
        │ 20s       ┆ "D"   │
        │ 25s       ┆ "E"   │
        │ 30s       ┆ "F"   │
        │ 35s       ┆ "G"   │
        │ 40s       ┆ "H"   │
        └───────────┴───────┘

        >>> tf.select(td_tf.selectors.duration())

        Selected:
        ┌──────────┐
        │ Elapsed  │
        │ ---      │
        │ duration │
        ╞══════════╡
        │ 5s       │
        │ 10s      │
        │ 15s      │
        │ 20s      │
        │ 25s      │
        │ 30s      │
        │ 35s      │
        │ 40s      │
        └──────────┘
    """
    return SelectorProxy(
        pl.selectors.duration(time_unit=time_unit) & _exclude_system_columns()
    )


# noinspection PyShadowingBuiltins
@pydoc(categories="projection")
def object() -> SelectorProxy:
    """
    Select all columns of object data type.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import date
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Custom": [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4},
        ...                {"e": 5}, {"f": 6}, {"g": 7}, {"h": 8}],
        ...     "Date": [date(2024, 1, 1), date(2024, 1, 2),
                         date(2024, 1, 3), date(2024, 1, 4),
        ...              date(2024, 1, 5), date(2024, 1, 6),
                         date(2024, 1, 7), date(2024, 1, 8)],
        ... })

        Original:
        ┌────────────┬────────────┐
        │ Custom     ┆ Date       │
        │ ---        ┆ ---        │
        │ object     ┆ date       │
        ╞════════════╪════════════╡
        │ {"a": 1}   ┆ 2024-01-01 │
        │ {"b": 2}   ┆ 2024-01-02 │
        │ {"c": 3}   ┆ 2024-01-03 │
        │ {"d": 4}   ┆ 2024-01-04 │
        │ {"e": 5}   ┆ 2024-01-05 │
        │ {"f": 6}   ┆ 2024-01-06 │
        │ {"g": 7}   ┆ 2024-01-07 │
        │ {"h": 8}   ┆ 2024-01-08 │
        └────────────┴────────────┘

        >>> tf.select(td_tf.selectors.object())

        Selected:
        ┌────────────┐
        │ Custom     │
        │ ---        │
        │ object     │
        ╞════════════╡
        │ {"a": 1}   │
        │ {"b": 2}   │
        │ {"c": 3}   │
        │ {"d": 4}   │
        │ {"e": 5}   │
        │ {"f": 6}   │
        │ {"g": 7}   │
        │ {"h": 8}   │
        └────────────┘
    """
    return SelectorProxy(pl.selectors.object() & _exclude_system_columns())


@pydoc(categories="projection")
def string(*, include_categorical: bool = False) -> SelectorProxy:
    """
    Select all columns of string or categorical data type.

    Parameters:
        include_categorical: If True, also include categorical columns in the selection.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Name": ["Anna", "Ben", "Cara", "Dean", "Ella", "Finn", "Gina", "Hugo"],
        ...     "Country": ["US", "UK", "FR", "DE", "IT", "ES", "NL", "SE"],
        ...     "Score": [90.5, 82.0, 88.5, 91.0, 85.5, 89.0, 87.5, 90.0]
        ... })

        Original:
        ┌────────┬─────────┬───────┐
        │ Name   ┆ Country ┆ Score │
        │ ---    ┆ ---     ┆ ---   │
        │ str    ┆ str     ┆ f64   │
        ╞════════╪═════════╪═══════╡
        │ "Anna" ┆ "US"    ┆ 90.5  │
        │ "Ben"  ┆ "UK"    ┆ 82.0  │
        │ "Cara" ┆ "FR"    ┆ 88.5  │
        │ "Dean" ┆ "DE"    ┆ 91.0  │
        │ "Ella" ┆ "IT"    ┆ 85.5  │
        │ "Finn" ┆ "ES"    ┆ 89.0  │
        │ "Gina" ┆ "NL"    ┆ 87.5  │
        │ "Hugo" ┆ "SE"    ┆ 90.0  │
        └────────┴─────────┴───────┘

        >>> tf.select(td_tf.selectors.string())

        Selected:
        ┌────────┬─────────┐
        │ Name   ┆ Country │
        │ ---    ┆ ---     │
        │ str    ┆ str     │
        ╞════════╪═════════╡
        │ "Anna" ┆ "US"    │
        │ "Ben"  ┆ "UK"    │
        │ "Cara" ┆ "FR"    │
        │ "Dean" ┆ "DE"    │
        │ "Ella" ┆ "IT"    │
        │ "Finn" ┆ "ES"    │
        │ "Gina" ┆ "NL"    │
        │ "Hugo" ┆ "SE"    │
        └────────┴─────────┘
    """
    return SelectorProxy(
        pl.selectors.string(include_categorical=include_categorical)
        & _exclude_system_columns()
    )


@pydoc(categories="projection")
def time() -> SelectorProxy:
    """
    Select columns of Python `time` values.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>> from datetime import time
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "EventTime": [
        ...         time(8, 0), time(9, 30), time(10, 15), time(11, 45),
        ...         time(13, 0), time(14, 20), time(15, 10), time(16, 55)
        ...     ],
        ...     "Label": ["A", "B", "C", "D", "E", "F", "G", "H"]
        ... })

        Original:
        ┌────────────┬───────┐
        │ EventTime  ┆ Label │
        │ ---        ┆ ---   │
        │ time       ┆ str   │
        ╞════════════╪═══════╡
        │ 08:00:00   ┆ "A"   │
        │ 09:30:00   ┆ "B"   │
        │ 10:15:00   ┆ "C"   │
        │ 11:45:00   ┆ "D"   │
        │ 13:00:00   ┆ "E"   │
        │ 14:20:00   ┆ "F"   │
        │ 15:10:00   ┆ "G"   │
        │ 16:55:00   ┆ "H"   │
        └────────────┴───────┘

        >>> tf.select(td_tf.selectors.time())

        Selected:
        ┌───────────┐
        │ EventTime │
        │ ---       │
        │ time      │
        ╞═══════════╡
        │ 08:00:00  │
        │ 09:30:00  │
        │ 10:15:00  │
        │ 11:45:00  │
        │ 13:00:00  │
        │ 14:20:00  │
        │ 15:10:00  │
        │ 16:55:00  │
        └───────────┘
    """
    return SelectorProxy(pl.selectors.time() & _exclude_system_columns())


"""
Selectors of second order.
"""


@pydoc(categories="projection")
def exclude(
    columns: (
        str
        | td_typing.DataType
        | td_typing.DataType
        | td_expr.Expr
        | Collection[str | td_typing.DataType | td_expr.Expr]
    ),
    *more_columns: str | td_typing.DataType | td_expr.Expr,
) -> SelectorProxy:
    """
    Exclude specific columns from selection by name, data type, expression, or selector.

    Parameters:
        columns:
            A single column identifier or a collection of them. This can include:
            - Column names as strings
            - Data types
            - Selector functions
            - Supported expressions

        *more_columns:
            Additional column identifiers passed as positional arguments.

    Example:
        >>> import tabsdata.tableframe as td_tf
        >>>
        >>> tf = td_tf.TableFrame({
        ...     "Id": [1, 2, 3, 4, 5, 6, 7, 8],
        ...     "Name": ["A", "B", "C", "D", "E", "F", "G", "H"],
        ...     "Score": [90.0, 85.5, 88.0, 91.0, 86.0, 87.5, 84.0, 89.5],
        ...     "IsActive": [True, False, True, True, False, True, False, True]
        ... })

        Original:
        ┌────┬──────┬───────┬──────────┐
        │ Id ┆ Name ┆ Score ┆ IsActive │
        │ ---┆ ---  ┆ ---   ┆ ---      │
        │ i64┆ str  ┆ f64   ┆ bool     │
        ╞════╪══════╪═══════╪══════════╡
        │ 1  ┆ "A"  ┆ 90.0  ┆ true     │
        │ 2  ┆ "B"  ┆ 85.5  ┆ false    │
        │ 3  ┆ "C"  ┆ 88.0  ┆ true     │
        │ 4  ┆ "D"  ┆ 91.0  ┆ true     │
        │ 5  ┆ "E"  ┆ 86.0  ┆ false    │
        │ 6  ┆ "F"  ┆ 87.5  ┆ true     │
        │ 7  ┆ "G"  ┆ 84.0  ┆ false    │
        │ 8  ┆ "H"  ┆ 89.5  ┆ true     │
        └────┴──────┴───────┴──────────┘

        >>> tf.select(td_tf.selectors.exclude("Score", td_tf.selectors.boolean()))

        Selected:
        ┌─────┬──────┐
        │ Id  ┆ Name │
        │ --- ┆ ---  │
        │ i64 ┆ str  │
        ╞═════╪══════╡
        │ 1   ┆ "A"  │
        │ 2   ┆ "B"  │
        │ 3   ┆ "C"  │
        │ 4   ┆ "D"  │
        │ 5   ┆ "E"  │
        │ 6   ┆ "F"  │
        │ 7   ┆ "G"  │
        │ 8   ┆ "H"  │
        └───═─┴──────┘
    """
    return SelectorProxy(
        pl.selectors.exclude(columns=columns, *more_columns) & _exclude_system_columns()
    )

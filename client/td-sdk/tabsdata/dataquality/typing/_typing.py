#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TypeAlias, Union

ColumnNamesSpec: TypeAlias = Union[
    str,
    list[str],
    None,
]

ColumNamesWithAltSpec: TypeAlias = Union[
    str,
    list[str],
    tuple[str, str],
    list[tuple[str, str]],
    None,
]

TagsSpec: TypeAlias = Union[
    str,
    list[str],
    None,
]

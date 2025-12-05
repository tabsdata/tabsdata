#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TypeAlias, Union

ColsSpec: TypeAlias = Union[
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

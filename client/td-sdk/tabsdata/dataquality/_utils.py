#
# Copyright 2025 Tabs Data Inc.
#

from typing import Any


def _is_collection(value: Any) -> bool:
    return (
        hasattr(value, "__contains__")
        or hasattr(value, "__iter__")
        or hasattr(value, "__getitem__")
    )

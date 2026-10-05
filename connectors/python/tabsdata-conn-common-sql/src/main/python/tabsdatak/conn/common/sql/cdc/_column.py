#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class TrackedColumn:
    """A column tracked by the CDC ingester.

    Attributes:
        name: Column name as returned by the database catalog.
        satype: Column type as parsed by SQLAlchemy.
        patype: Column type as represented in PyArrow.
        oname: Column name containing the old value when the CDC ingester reads
            from tables that store both old and new values.
            Used by IBM SQL Replication.
        sa_name: Column name after SQLAlchemy identifier normalization.
        sa_oname: Old-value column name after SQLAlchemy identifier normalization.
        converter: Converter a value must go through before it lands in the
            Arrow column, when the Arrow type cannot take the driver's value
            as is -- a text column fed a dict, a UUID or a Decimal; ``None``
            for every column whose values fit their type.
    """

    name: str
    satype: Any
    patype: Any = None
    oname: str | None = None
    sa_name: str | None = None
    sa_oname: str | None = None
    converter: Callable[[Any], Any] | None = None

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

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
    """

    name: str
    satype: Any
    patype: Any = None
    oname: str | None = None
    sa_name: str | None = None
    sa_oname: str | None = None

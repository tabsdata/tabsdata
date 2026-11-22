#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import SchemaEvolution


class WriteMode(Enum):
    APPEND = "append"
    REPLACE = "replace"


@dataclass(frozen=True)
class LoadMode:
    """Context passed to all dialect methods so they can adjust behavior."""

    write_mode: WriteMode
    schema_evolution: SchemaEvolution

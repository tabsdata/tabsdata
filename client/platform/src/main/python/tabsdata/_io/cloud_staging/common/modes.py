#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WriteMode(Enum):
    APPEND = "append"
    REPLACE = "replace"


class SchemaEvolutionMode(Enum):
    NONE = "none"
    NATIVE = "native"
    ICEBERG = "iceberg"


@dataclass(frozen=True)
class LoadMode:
    """Context passed to all dialect methods so they can adjust behavior."""

    write_mode: WriteMode
    schema_evolution: SchemaEvolutionMode
    upload_id: str

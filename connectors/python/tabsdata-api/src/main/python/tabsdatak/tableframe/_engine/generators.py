#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from tabsdatak.tableframe._engine.constants.columns.prefixes import (
    TD_TEMP_COLUMN_PREFIX,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.tableframe import (
        DataVersionProperties as DataVersionProperties_,
    )


@dataclass(frozen=True, slots=True)
class GeneratorContext:
    index: int | None
    properties: DataVersionProperties_ | None
    row_index_column: str | None


class Generator(Protocol):
    def apply(
        self,
        lf: pl_.LazyFrame,
        column: str,
        context: GeneratorContext,
    ) -> pl_.LazyFrame: ...


def temp_column() -> str:
    return f"{TD_TEMP_COLUMN_PREFIX}{uuid.uuid4().hex}"

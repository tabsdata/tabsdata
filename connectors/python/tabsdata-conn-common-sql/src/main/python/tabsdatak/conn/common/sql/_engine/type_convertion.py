#
# Copyright 2026 Tabsdata Inc.
#

# --- DataType -> polars dtype --------------------------------------
from typing import Any, Mapping


def polars_dtype(dt: Any) -> Any:
    """Map a :class:`~tabsdatak.conn.common.types.DataType` member
    to its polars dtype equivalent. Raises ``ValueError`` for any
    member we haven't mapped (defensive -- the DataType enum can
    grow new members and we want a loud failure rather than a
    silent drop)."""
    import polars as pl

    from tabsdatak.conn.common.types import DataType

    mapping = {
        DataType.BOOL: pl.Boolean,
        DataType.INT: pl.Int64,
        DataType.FLOAT: pl.Float64,
        DataType.STRING: pl.Utf8,
        DataType.BYTES: pl.Binary,
        DataType.DATE: pl.Date,
        DataType.TIME: pl.Time,
        DataType.DATETIME: pl.Datetime,
    }
    try:
        return mapping[dt]
    except KeyError as e:
        raise ValueError(f"unsupported DataType: {dt!r}") from e


def resolve_schema_overrides(
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Resolve a ``{column -> DataType}`` map into a
    ``{column -> polars-dtype}`` map. Returns ``None`` unchanged."""
    if overrides is None:
        return None
    return {col: polars_dtype(dt) for col, dt in overrides.items()}

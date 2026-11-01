#
# Copyright 2026 Tabsdata Inc.
#

"""Remove the changes an initial load already holds.

A database without a point-in-time snapshot loads each row at its own moment,
so the changes committed while the load ran are both in the load and in the
change stream. :func:`deduplicate_initial_load` keeps the ones that still
apply: it walks the window's changes in their original order over a state
that starts as the loaded rows of the keys they touch, and keeps a change
only if it applies at its turn. Only a kept change moves the state.

* an insert applies when its key has no row;
* a delete applies when its key has a row;
* an update applies when its key has a row with other contents;
* an update that changes the key applies when its old key has a row or its
  new key has none, and fails when both do: a key would hold two rows.

The last change before the load read a key leaves the state equal to the
loaded row, so every change after it applies as it did at the source.

:func:`deduplicate_initial_load_frames` runs it over the tables the stream
sent, one output slot per table, and returns the kept changes of the tables
it drops changes from, lazily, in the layout the stream sends them in.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._model import TrackedTable
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_OP,
    CDC_META_ORD,
    CDC_META_SCH,
    CDC_META_SEG,
    DOT,
    OP_LOAD,
    CdcOperation,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_VOLATILE_SYSTEM_COLUMN_PREFIX,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import polars as pl

__all__ = [
    "Deduplicated",
    "deduplicate_initial_load",
    "deduplicate_initial_load_frames",
]

logger = logging.getLogger(__name__)

POSITION = f"{TD_VOLATILE_SYSTEM_COLUMN_PREFIX}dedup_position"
OLD_KEY = f"{TD_VOLATILE_SYSTEM_COLUMN_PREFIX}dedup_old"
NEW_KEY = f"{TD_VOLATILE_SYSTEM_COLUMN_PREFIX}dedup_new"
CONTENTS = f"{TD_VOLATILE_SYSTEM_COLUMN_PREFIX}dedup_contents"

ROW_OPERATIONS = {
    CdcOperation.INSERT.value,
    CdcOperation.UPDATE.value,
    CdcOperation.DELETE.value,
}

Values = tuple[Any, ...]


@dataclass(frozen=True)
class Deduplicated:
    changes: pl.LazyFrame
    dropped: int


def deduplicate_initial_load_frames(
    *,
    loaded: list[list[pl.LazyFrame]],
    changes: list[list[pl.LazyFrame]],
    tables: dict[str, TrackedTable],
    schemas: dict[str, SchemaVersion],
    cdc_format: CdcFormat,
) -> dict[str, Deduplicated]:
    """The kept changes of every table deduplication drops changes from,
    lazily, cast to the layout the stream sends them in. A table it drops
    nothing from is left out: its changes are good as they are.

    `loaded` and `changes` hold one list of frames per output slot, in the
    order of `tables`: the initial load's tables, and those of the changes
    committed while it ran. `schemas` holds each table's schema version as
    the load registered it. Every table is checked and deduplicated before
    any is returned, so a failure leaves nothing to send.
    """
    import polars as pl

    reduced: dict[str, Deduplicated] = {}
    for (table, _), load_frames, change_frames in zip(
        tables.items(), loaded, changes, strict=True
    ):
        if not change_frames:
            continue
        schema = schemas[table]
        deduplicated = deduplicate_initial_load(
            combine(load_frames, schema),
            combine(change_frames, schema),
            table=table,
            schema=schema,
            cdc_format=cdc_format,
        )
        if deduplicated.dropped == 0:
            continue
        layout = pl.DataFrame(schema.pa_schema.empty_table()).schema
        reduced[table] = Deduplicated(
            deduplicated.changes.select(layout.names()).cast(dict(layout)),
            deduplicated.dropped,
        )
    return reduced


def combine(frames: list[pl.LazyFrame], schema: SchemaVersion) -> pl.LazyFrame:
    """One frame over `frames`, whose columns may differ, or an empty frame
    of `schema` when there are none."""
    import polars as pl

    if not frames:
        return pl.from_arrow(schema.pa_schema.empty_table()).lazy()
    return pl.concat(frames, how="diagonal_relaxed")


def deduplicate_initial_load(
    loaded: pl.LazyFrame,
    changes: pl.LazyFrame,
    *,
    table: str,
    schema: SchemaVersion,
    cdc_format: CdcFormat,
) -> Deduplicated:
    """The `changes` committed during the initial load of `table` that apply
    to what it `loaded`, lazily and in their original order, with how many it
    dropped.

    One table per call: the stream writes every tracked table, its loaded
    rows and its changes alike, to files of its own (one output slot per
    table), so the frames of two tables never share a file.

    Both frames have the layout the stream writes for `schema` in
    `cdc_format`, and `changes` is in the order the stream delivered them. A
    table without a primary key is not deduplicated: its changes are only
    checked, and returned as they are.

    The load is read once, through a semi-join on every key the changes
    touch, and each change is then one lookup in the resulting state.

    Raises:
        SqlCdcException: ``CDC_22`` if a key column is not in `schema`,
            ``CDC_23`` if a key would hold two rows, ``CDC_24`` on a
            truncate, ``CDC_25`` if a change is in another schema than `schema`,
            ``CDC_26`` for ``columns_format="map"``, ``CDC_27`` on an
            operation it does not handle, ``CDC_28`` if the changes are
            not in ascending ``(seg, ord)``.
    """
    import polars as pl

    key = schema.primary_key
    if not key:
        window = changes.select(CDC_META_OP, CDC_META_SCH).collect()
        check_window(window, schema, table)
        if window.height:
            logger.warning(
                "Table %s has no primary key, so the %d change(s) committed to it "
                "while the initial load read it are staged as read: one the load "
                "already holds arrives again",
                table,
                window.height,
            )
        return Deduplicated(changes, 0)
    if cdc_format.columns_format == "map":
        raise SqlCdcErrorCode.CDC_26.exception()
    names = [column.name for column in schema.columns]
    for column in key:
        if column not in names:
            raise SqlCdcErrorCode.CDC_22.exception(column=column, table=table)

    positioned = changes.with_row_index(POSITION)
    window = positioned.select(
        pl.col(POSITION),
        pl.col(CDC_META_OP),
        pl.col(CDC_META_SCH),
        pl.col(CDC_META_SEG),
        pl.col(CDC_META_ORD),
        key_struct(key, lambda name: old_image(name, cdc_format)).alias(OLD_KEY),
        key_struct(key, lambda name: new_image(name, cdc_format)).alias(NEW_KEY),
        contents_struct(names, cdc_format).alias(CONTENTS),
    ).collect()
    if window.height == 0:
        return Deduplicated(changes, 0)
    check_window(window, schema, table)
    check_order(window, table)

    touched = (
        pl
        .concat([
            window.select(pl.col(OLD_KEY).alias(NEW_KEY)),
            window.select(NEW_KEY),
        ])
        .drop_nulls()
        .unique()
        .unnest(NEW_KEY)
    )
    state = loaded_state(loaded, touched, key, names, cdc_format, table)
    dropped = apply_changes(window, state, table)
    if not dropped:
        return Deduplicated(changes, 0)
    return Deduplicated(
        positioned.filter(~pl.col(POSITION).is_in(dropped)).drop(POSITION),
        len(dropped),
    )


def apply_changes(
    window: pl.DataFrame, state: dict[Values, Values], table: str
) -> list[int]:
    """The positions of the changes that do not apply, walking them in order
    and moving `state` with the ones that do."""
    dropped: list[int] = []
    for position, op, old_row, new_row, contents_row in window.select(
        POSITION, CDC_META_OP, OLD_KEY, NEW_KEY, CONTENTS
    ).iter_rows():
        old = values_of(old_row)
        new = values_of(new_row)
        contents = values_of(contents_row)
        match op:
            case CdcOperation.INSERT:
                applies = new not in state
                if applies:
                    state[new] = contents
            case CdcOperation.DELETE:
                target = old if old is not None else new
                applies = target in state
                if applies:
                    del state[target]
            case CdcOperation.UPDATE if old is None or old == new:
                applies = new in state and state[new] != contents
                if applies:
                    state[new] = contents
            case CdcOperation.UPDATE:
                if old in state and new in state:
                    raise SqlCdcErrorCode.CDC_23.exception(key=new, table=table)
                applies = old in state or new not in state
                if applies:
                    state.pop(old, None)
                    state[new] = contents
            case _:
                raise SqlCdcErrorCode.CDC_27.exception(op=op, table=table)
        if not applies:
            dropped.append(position)
    return dropped


def check_window(window: pl.DataFrame, schema: SchemaVersion, table: str) -> None:
    """Fail on a window this deduplication cannot be sure of: a change in
    another schema than the load's, a truncate, or an operation it does not
    handle. Schema hashes are normalized, so a different hash is a different
    schema, not another way of describing the same one."""
    if set(window[CDC_META_SCH].unique().to_list()) - {schema.hash}:
        raise SqlCdcErrorCode.CDC_25.exception(table=table)
    ops = set(window[CDC_META_OP].unique().to_list())
    if CdcOperation.TRUNCATE.value in ops:
        raise SqlCdcErrorCode.CDC_24.exception(table=table)
    unknown = sorted(ops - ROW_OPERATIONS, key=str)
    if unknown:
        raise SqlCdcErrorCode.CDC_27.exception(op=unknown[0], table=table)


def check_order(window: pl.DataFrame, table: str) -> None:
    """Fail unless the changes are in ascending ``(seg, ord)``, the order the
    stream delivers a table's changes in: the walk applies them in the order
    they come, so any other would be a fault upstream, not something to sort
    away."""
    import polars as pl

    seg, ord_ = pl.col(CDC_META_SEG), pl.col(CDC_META_ORD)
    previous_seg, previous_ord = seg.shift(), ord_.shift()
    backwards = window.with_row_index("row").filter(
        (seg < previous_seg) | ((seg == previous_seg) & (ord_ < previous_ord))
    )
    if backwards.height:
        row = backwards["row"][0]
        raise SqlCdcErrorCode.CDC_28.exception(
            table=table,
            position=(window[CDC_META_SEG][row], window[CDC_META_ORD][row]),
            previous=(window[CDC_META_SEG][row - 1], window[CDC_META_ORD][row - 1]),
        )


def loaded_state(
    loaded: pl.LazyFrame,
    touched: pl.DataFrame,
    key: tuple[str, ...],
    names: list[str],
    cdc_format: CdcFormat,
    table: str,
) -> dict[Values, Values]:
    """The loaded contents of the `touched` keys, read through a semi-join so
    the load streams past."""
    import polars as pl

    rows = (
        loaded
        .filter(pl.col(CDC_META_OP) != OP_LOAD)
        .select(
            *(new_image(name, cdc_format).alias(name) for name in key),
            contents_struct(names, cdc_format).alias(CONTENTS),
        )
        .join(touched.lazy(), on=list(key), how="semi")
        .collect()
    )
    state: dict[Values, Values] = {}
    for values in rows.iter_rows():
        row_key = tuple(values[: len(key)])
        if row_key in state:
            raise SqlCdcErrorCode.CDC_23.exception(key=row_key, table=table)
        state[row_key] = values_of(values[len(key)])
    return state


def new_image(name: str, cdc_format: CdcFormat) -> pl.Expr:
    import polars as pl

    if cdc_format.flatten_columns:
        return pl.col(name)
    if cdc_format.columns_format == "struct":
        return pl.col(CDC_DATA_ROW_NEW).struct.field(name)
    return pl.col(f"{CDC_DATA_COL_NEW_PREFIX}{DOT}{name}")


def old_image(name: str, cdc_format: CdcFormat) -> pl.Expr:
    import polars as pl

    if cdc_format.columns_format == "struct":
        return pl.col(CDC_DATA_ROW_OLD).struct.field(name)
    return pl.col(f"{CDC_DATA_COL_OLD_PREFIX}{DOT}{name}")


def key_struct(key: tuple[str, ...], image: Callable[[str], pl.Expr]) -> pl.Expr:
    """The key as a struct, or null when the image has none: a key column
    holds no nulls, so one null means the source left the image empty. A
    delete's row is in its old image on some sources and its new image on
    others, and an update's old image is empty unless its key changed or the
    source records full old images."""
    import polars as pl

    columns = [image(name).alias(name) for name in key]
    return (
        pl
        .when(pl.all_horizontal([column.is_not_null() for column in columns]))
        .then(pl.struct(columns))
        .otherwise(None)
    )


def contents_struct(names: list[str], cdc_format: CdcFormat) -> pl.Expr:
    import polars as pl

    return pl.struct([new_image(name, cdc_format).alias(name) for name in names])


def values_of(row: dict | None) -> Values | None:
    return None if row is None else tuple(row.values())

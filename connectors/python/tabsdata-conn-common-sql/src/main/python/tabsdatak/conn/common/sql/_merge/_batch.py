#
# Copyright 2026 Tabsdata Inc.
#

"""Reducing a captured batch to what a ``MERGE`` can apply."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import MergeDialect
from tabsdatak.conn.common.sql.cdc import cdc_batch_resets, coalesce_cdc_batch

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)


def incoming_column(
    dialect: MergeDialect,
    names: list[str],
    wanted: str,
    table: str,
) -> str | None:
    """The incoming column ``wanted`` names, in the spelling the batch uses, or
    ``None`` if there is none.

    ``wanted`` arrives folded, and the batch is searched folded, because a load
    matches columns to the target the same way: a publisher writing ``order_id``
    satisfies a config naming ``ORDER_ID``. polars is case-sensitive, so the
    batch's own spelling has to be recovered before any expression can name it.

    Two columns folding onto one name is refused rather than guessed at: which
    one the merge should read is not a decision to make silently.
    """
    matches = [name for name in names if dialect.fold(name) == dialect.fold(wanted)]
    if len(matches) > 1:
        raise dialect.errors.ambiguous_column(
            column=wanted,
            table=table,
            matches=", ".join(repr(name) for name in matches),
        )
    return matches[0] if matches else None


def batch_resets(
    dialect: MergeDialect,
    table: str,
    batch: pl.LazyFrame,
    cfg: MergeCfg,
) -> bool:
    """Whether ``batch`` loads or truncates the table, so that applying it
    means replacing the target's rows rather than merging onto them.

    Asked of the batch as captured: :func:`coalesced_batch` drops the load and
    truncate records once they have cut the batch, so its output cannot tell.
    Only the marker column is read.
    """
    marker = incoming_column(
        dialect, batch.collect_schema().names(), cfg._op_column(), table
    )
    if marker is None:
        raise dialect.errors.marker_absent(column=cfg.op_column, table=table)
    return cdc_batch_resets(batch, marker).collect().item()


def coalesced_batch(
    dialect: MergeDialect,
    table: str,
    batch: pl.LazyFrame,
    cfg: MergeCfg,
) -> pl.LazyFrame:
    """``batch`` reduced to one row per key, to be staged in its place.

    A captured batch can carry several changes for one key, and a ``MERGE``
    matches each incoming row against the target at most once -- a second row
    for the same key is a non-deterministic update, which every engine in this
    family rejects outright. The batch is reduced to one row per key first, so
    the statement only ever sees rows it can apply.

    Records are resolved in arrival order, which for a single batch is the order
    the publisher wrote them in.

    :meth:`MergeDialect.adapt_batch` rides the same plan, so a dialect needing
    a cast before its engine reads the batch pays no extra pass for it. It is
    applied first, so the reduction sees the keys at the precision the target
    will.
    """
    lf = dialect.adapt_batch(batch)
    # Resolving the schema, not the rows: the names have to be known before the
    # transformation can be built, and the transformation never resolves a
    # schema of its own.
    names = lf.collect_schema().names()

    marker = incoming_column(dialect, names, cfg._op_column(), table)
    if marker is None:
        raise dialect.errors.marker_absent(column=cfg.op_column, table=table)
    found = [incoming_column(dialect, names, key, table) for key in cfg._keys()]
    absent = [spec for spec, key in zip(cfg.on, found, strict=True) if key is None]
    if absent:
        raise dialect.errors.keys_absent(
            columns=", ".join(repr(name) for name in absent), table=table
        )
    keys = [key for key in found if key is not None]

    logger.info(
        "Coalescing the batch on %s (marker %r) before the merge onto %s",
        keys,
        marker,
        table,
    )
    return coalesce_cdc_batch(lf, keys, marker)

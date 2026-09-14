#
# Copyright 2026 Tabsdata Inc.
#

"""Reducing a captured batch to what a ``MERGE`` can apply."""

from __future__ import annotations

import contextlib
import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import MergeDialect
from tabsdatak.conn.common.sql.cdc import coalesce_cdc_batch

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


@contextlib.contextmanager
def coalesced_batch(
    dialect: MergeDialect,
    table: str,
    batch: Path,
    cfg: MergeCfg,
) -> Iterator[Path]:
    """Yield a parquet holding one row per key, to be staged in place of
    ``batch``.

    A captured batch can carry several changes for one key, and a ``MERGE``
    matches each incoming row against the target at most once -- a second row
    for the same key is a non-deterministic update, which every engine in this
    family rejects outright. The batch is reduced to one row per key first, so
    the statement only ever sees rows it can apply.

    Records are resolved in arrival order, which for a single batch is the order
    the publisher wrote them in.

    The reduced file keeps the original's basename, so the staged file -- and
    everything downstream keyed off its name -- is indistinguishable from the
    uncoalesced one. It lives in a temp directory removed as soon as it has been
    staged.
    """
    import polars as pl

    lf = pl.scan_parquet(batch)
    # Reading the footer, not the rows: the names have to be known before the
    # transformation can be built, and the transformation never resolves a
    # schema of its own.
    names = lf.collect_schema().names()

    marker = incoming_column(dialect, names, cfg._op_column(), table)
    if marker is None:
        raise dialect.errors.marker_absent(column=cfg.op_column, table=table)
    keys = [incoming_column(dialect, names, key, table) for key in cfg._keys()]
    absent = [spec for spec, found in zip(cfg.on, keys, strict=True) if found is None]
    if absent:
        raise dialect.errors.keys_absent(
            columns=", ".join(repr(name) for name in absent), table=table
        )

    with tempfile.TemporaryDirectory() as work:
        reduced = Path(work) / batch.name
        logger.info(
            "Coalescing %s on %s (marker %r) before the merge onto %s",
            batch.name,
            keys,
            marker,
            table,
        )
        coalesce_cdc_batch(lf, keys, marker).sink_parquet(reduced)
        yield reduced

#
# Copyright 2026 Tabsdata Inc.
#

"""The merge every SQL destination runs, in the order they all run it."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg, WatermarkCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._batch import coalesced_batch

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._statement import render_merge

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import MergeDialect, Staged
from tabsdatak.spi import DestContext

__all__ = ["MergeResult", "merge_batch", "write_columns"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MergeResult:
    """What a merge did, for the caller to log or assert on.

    Args:
        evolved: The target columns the schema-evolution pass added or retyped.
        written: The columns the statement wrote, in the batch's order.
    """

    evolved: list[str]
    written: list[str]


def write_columns(
    dialect: MergeDialect,
    target: str,
    staged: Staged,
    cfg: MergeCfg,
) -> list[str]:
    """The columns a merge onto ``target`` writes, in the batch's order.

    The columns both sides have, less the marker: the marker says what to do
    with a row and is never part of the row. Membership is folded, the way the
    load matches columns and the way the evolution pass diffs them, so a target
    holding the column in another case counts as having it.

    The marker has to be in the batch, since the clauses read it from there; it
    does not have to be in the target, and under ``schema_evolution="update"``
    it is deliberately kept out.
    """
    marker = dialect.fold(cfg._op_column())
    staged_names = staged.names()
    if marker not in {dialect.fold(name) for name in staged_names}:
        raise dialect.errors.marker_absent(column=cfg.op_column, table=target)
    existing = {dialect.fold(name) for name, _ in dialect.target_columns(target)}
    return [
        name
        for name in staged_names
        if dialect.fold(name) in existing and dialect.fold(name) != marker
    ]


def merge_batch(
    dialect: MergeDialect,
    *,
    target: str,
    batch: Path,
    cfg: MergeCfg,
    schema_evolution: str,
    ctx: DestContext,
) -> MergeResult:
    """Apply ``batch`` onto ``target`` as a CDC merge.

    The whole sequence, and the only one: reduce the batch to one row per key,
    stage it where the statement can read it, bring the target into being if it
    is not there, evolve its schema if asked to, work out what the statement
    writes, and run it. The staging is torn down however the merge ends.

    Every engine-specific step is a :class:`MergeDialect` call. A destination
    that needs a step none of the others do belongs inside one of those hooks,
    not in a copy of this function.

    A ``MergeCfg`` naming a watermark column makes the whole thing idempotent:
    the rows written carry this transaction's id, and a rerun that finds the
    target already stamped with it writes nothing rather than applying the same
    batch twice. That column reaches the target under either evolution setting
    -- through the evolution pass on ``"update"``, and through
    :meth:`MergeDialect.ensure_watermark_column` on ``"strict"``, since a
    watermark nothing can be stamped in is no watermark at all.
    """
    watermark_column = cfg._watermark_column()
    watermark_cfg = (
        WatermarkCfg(column=watermark_column, transaction_id=ctx.trx_id)
        if watermark_column is not None
        else None
    )
    with coalesced_batch(dialect, target, batch, cfg) as reduced:
        if watermark_cfg is not None:
            logger.debug(
                "Checking watermark column %s on %s", watermark_cfg.column, target
            )
            if dialect.check_watermark_column(target, watermark_cfg):
                logger.info(
                    "Skipping merge onto %s: watermark column %s already has value %s",
                    target,
                    watermark_cfg.column,
                    watermark_cfg.transaction_id,
                )
                return MergeResult(evolved=[], written=[])
        with dialect.staging(target, reduced, watermark_cfg) as staged:
            marker = cfg._op_column()
            dialect.ensure_target(target, staged, marker)
            evolved = (
                dialect.evolve(target, staged, marker)
                if schema_evolution == "update"
                else dialect.ensure_watermark_column(target, watermark_cfg)
            )
            # Read after the ALTERs above, so a column just added counts as
            # written rather than being left out of the merge.
            written = write_columns(dialect, target, staged, cfg)
            if not written:
                raise dialect.errors.no_shared_columns(table=target)
            logger.info(
                "Merging %s into %s on columns %s", staged.relation, target, written
            )
            dialect.execute(render_merge(dialect, target, staged, cfg, written))
    return MergeResult(evolved=evolved, written=written)

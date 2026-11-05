#
# Copyright 2026 Tabsdata Inc.
#

"""Stamping a write with the id of the transaction that made it.

An idempotency watermark: a column every row a write lands carries the trx
id in, so a rerun of the same transaction can be recognised and skipped
rather than applying the same batch twice. Without it neither disposition is
idempotent -- a merge reapplies its batch, and a bulk load simply appends it
again, because nothing in a load path recognises a re-staged file.

Every destination names the watermark on itself rather than on its
``MergeCfg``: it belongs to the write, not to the way one table's rows are
matched, so one name covers every target a write touches. What each
disposition then does with it is the connector's own business -- a merge
carries it across in the statement, a bulk load writes it into the batch
before staging with :func:`watermarked_batch` -- but the two things every
connector needs of it are here: the batch rewrite, and the checks that the
name does not collide with the columns a merge already reads.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from tabsdatak.conn.common.sql.error import SqlCommonErrorCode

if TYPE_CHECKING:
    import polars as pl

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._merge import MergeCfg

logger = logging.getLogger(__name__)


def watermarked_batch(
    batch: pl.LazyFrame,
    column: str | None,
    transaction_id: str,
    adapt: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
) -> pl.LazyFrame:
    """``batch`` carrying ``column`` stamped with ``transaction_id``, to be
    staged in its place -- or ``batch`` itself when the write neither names a
    watermark nor adapts the batch.

    ``adapt`` is the connector's own rewrite of the frame -- a cast its engine
    needs, typically -- folded into the same plan so the rows are walked once,
    when the batch is staged.

    Written into the staged batch rather than added to the target afterwards: a
    bulk load matches the column onto the target by name like any other, so the
    load stamps the rows it brings in and nothing has to walk them a second
    time. Rows already in the target are left alone, which is what "written
    before there was a watermark" looks like.

    The target still has to be given the column before the load: the engines
    differ on whether a batch carrying a column the target lacks is dropped or
    refused, and neither is a watermark.
    """
    if adapt is not None:
        batch = adapt(batch)
    if column is None:
        return batch

    import polars as pl

    logger.info(
        "Stamping the batch with %s = %r before staging", column, transaction_id
    )
    return batch.with_columns(pl.lit(transaction_id, dtype=pl.String).alias(column))


def validate_watermark_column(cfg: MergeCfg, watermark_column: str | None) -> None:
    """Check a destination's watermark against the columns ``cfg``'s merge
    already reads.

    Both names are compared folded, so a dialect that folds case catches a
    watermark colliding with a key or with the marker under a different
    spelling. Stamping either of those would overwrite what the merge matches
    on, or what it reads each row's fate from.

    Called by a destination's own ``validate``, because the watermark is named
    there; raises `SqlCommonValidateException`.
    """
    if watermark_column is None:
        return
    # noinspection PyProtectedMember
    folded = cfg._fold(watermark_column)
    # noinspection PyProtectedMember
    if folded in set(cfg._keys()):
        # noinspection PyProtectedMember
        raise SqlCommonErrorCode.SQL_COMMON_7.exception(column=watermark_column)
    # noinspection PyProtectedMember
    if folded == cfg._op_column():
        # noinspection PyProtectedMember
        raise SqlCommonErrorCode.SQL_COMMON_8.exception(column=watermark_column)

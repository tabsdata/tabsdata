#
# Copyright 2026 Tabsdata Inc.
#

"""Evolving a merge target with ``ALTER`` statements.

The strategy for an engine that does not evolve a target from inside the
``MERGE``: diff the target against the batch with the shared Iceberg
machinery and run what the dialect's renderer can express. A dialect whose
engine evolves during the statement does not come through here at all --
it leaves :meth:`MergeDialect.evolve` alone and carries a modifier in its
:class:`MergeSyntax`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._iceberg import (
    ColumnDef,
    DialectRenderer,
    SchemaDelta,
    TableSchema,
    TypeWideningMap,
    compute_delta,
    validate_delta,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import MergeDialect, Staged

__all__ = ["evolve_with_iceberg"]

logger = logging.getLogger(__name__)


def evolve_with_iceberg(
    dialect: MergeDialect,
    target: str,
    staged: Staged,
    marker: str,
    *,
    renderer: DialectRenderer,
    widening_map: TypeWideningMap,
    can_safely_widen: Callable[[str, str], bool] | None = None,
    align_type: Callable[[str | None, str], str] | None = None,
    strict: bool = True,
) -> list[str]:
    """``ALTER`` ``target`` to meet the batch where the engine allows it,
    returning the columns changed.

    The marker is left out of the desired schema entirely rather than filtered
    out of the rendered statements, so the diff never reports an addition
    nobody wants.

    Both sides' names are folded before they are compared, the way a load
    matches them, so a target holding a column in another case counts as having
    it -- and a column added here is added under its folded name.

    ``align_type`` restates the batch's type in the target's spelling where the
    two mean the same thing. An engine whose schema read and whose batch
    inference speak different vocabularies needs it, or a column nobody touched
    reads as a retype.

    Only the safe changes the renderer supports are run, and they are lifted
    into a delta of their own first: that drops what the dialect does not do --
    a dropped or moved column is reported and ignored -- and pins ``is_create``
    to False, so the ALTER path is the only path a target can take through here.

    ``strict`` refuses the whole write when a change is one the engine cannot
    make, rather than staging rows into a column that cannot hold them. It is
    the dialect's policy: an engine that tolerates drift passes False.
    """
    align = align_type or (lambda stored, inferred: inferred)
    folded_marker = dialect.fold(marker)

    current = TableSchema(
        table_name=target,
        columns=[
            ColumnDef(name=dialect.fold(name), type=type_)
            for name, type_ in dialect.target_columns(target)
        ],
    )
    stored = {column.name: column.type for column in current.columns}
    desired = TableSchema(
        table_name=target,
        columns=[
            ColumnDef(
                name=dialect.fold(name),
                type=align(stored.get(dialect.fold(name)), type_),
            )
            for name, type_ in staged.columns
            if dialect.fold(name) != folded_marker
        ],
    )

    delta = compute_delta(
        current, desired, widening_map, can_safely_widen=can_safely_widen
    )
    if strict:
        validate_delta(delta, strict=True)

    evolving = SchemaDelta(
        table_name=delta.table_name,
        is_create=False,
        changes=[
            change
            for change in delta.changes
            if change.safe and change.action in renderer.SUPPORTED_ACTIONS
        ],
    )
    for statement in renderer.render_sql(evolving):
        logger.info("Evolving %s: %s", target, statement)
        dialect.execute(statement)
    return [change.column for change in evolving.changes]

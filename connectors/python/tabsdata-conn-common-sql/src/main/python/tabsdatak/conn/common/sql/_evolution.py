#
# Copyright 2026 Tabsdata Inc.
#

"""Evolving a target with ``ALTER`` statements.

The strategy for an engine that will not evolve a target on its own: diff
the target against the incoming batch with the shared Iceberg machinery and
run what the renderer can express.

A merge reaches this through
:meth:`~tabsdatak.conn.common.sql._merge.dialect.MergeDialect.evolve` --
except for a dialect whose engine evolves during the ``MERGE``, which does
not come through here at all and carries a modifier in its
:class:`~tabsdatak.conn.common.sql._merge.dialect.MergeSyntax` instead. A
bulk load reaches it when the destination is asked to evolve the target
itself rather than leaning on the engine's own schema evolution -- which is
why this sits beside :mod:`~tabsdatak.conn.common.sql._iceberg` rather than
under ``_merge``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol

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

logger = logging.getLogger(__name__)


class SchemaEvolutionTarget(Protocol):
    """What :func:`evolve_with_iceberg` needs of the thing being evolved.

    The three operations the pass makes against the destination, and nothing
    else: how it folds a name, how it reads the target's columns, and how it
    runs a statement. :class:`MergeDialect` satisfies it, so a merge passes
    itself; a bulk load has no dialect and supplies its own.
    """

    def fold(self, name: str) -> str:
        """The name's comparable form, per the engine's identifier folding."""

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        """``(name, type)`` per column the target currently has."""

    def execute(self, statement: str) -> None:
        """Run one statement against the destination."""


def evolve_with_iceberg(
    engine: SchemaEvolutionTarget,
    target: str,
    columns: list[tuple[str, str]],
    *,
    marker: str | None = None,
    renderer: DialectRenderer,
    widening_map: TypeWideningMap,
    can_safely_widen: Callable[[str, str], bool] | None = None,
    align_type: Callable[[str | None, str], str] | None = None,
    strict: bool = True,
) -> list[str]:
    """``ALTER`` ``target`` to meet the batch where the engine allows it,
    returning the columns changed.

    ``columns`` is what the incoming batch carries, ``(name, type)`` per
    column. A ``marker`` is left out of the desired schema entirely rather than
    filtered out of the rendered statements, so the diff never reports an
    addition nobody wants; a load with no marker column passes ``None``.

    A column both sides have is named, on the desired side, with the *target's*
    spelling. Names are matched folded, the way a load matches them, so a target
    holding the column in another case counts as having it -- but the two sides
    have to spell it identically or the diff reads one column as a drop plus an
    add. A column only the batch has keeps the batch's spelling, which becomes
    the new column's name.

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
    the caller's policy: an engine that tolerates drift passes False.
    """
    align = align_type or (lambda stored, inferred: inferred)
    folded_marker = engine.fold(marker) if marker is not None else None

    stored = engine.target_columns(target)
    current = TableSchema(
        table_name=target,
        columns=[ColumnDef(name=name, type=type_) for name, type_ in stored],
    )
    spelling = {engine.fold(name): name for name, _ in stored}
    types = {engine.fold(name): type_ for name, type_ in stored}
    desired = TableSchema(
        table_name=target,
        columns=[
            ColumnDef(
                name=spelling.get(engine.fold(name), name),
                type=align(types.get(engine.fold(name)), type_),
            )
            for name, type_ in columns
            if engine.fold(name) != folded_marker
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
        engine.execute(statement)
    return [change.column for change in evolving.changes]

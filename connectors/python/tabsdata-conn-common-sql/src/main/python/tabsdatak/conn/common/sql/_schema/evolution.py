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
why this sits beside :mod:`~tabsdatak.conn.common.sql._schema.delta` rather than
under ``_merge``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import (
    ColumnDef,
    DialectRenderer,
    SchemaDelta,
    TableSchema,
    TypeWideningMap,
    compute_delta,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import SchemaEvolution, governed

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
    same_base: Callable[[str, str], bool] | None = None,
    evolution: SchemaEvolution,
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

    ``evolution`` decides what the diff may do, through
    :func:`~tabsdatak.conn.common.sql._schema.policy.governed`: under
    ``"strict"`` a new column or a wider type refuses the whole write, under
    ``"update"`` only a change the engine cannot make safely does, and a column
    the batch lacks is never dropped. ``same_base`` lets ``"strict"`` leave a
    longer length or a larger precision of the same type to the load, which
    fails only on a value that does not fit: a batch's inferred type is often
    the unbounded one whatever its values are. What is left and the renderer supports is
    run, lifted into a delta of its own with ``is_create`` pinned to False, so
    the ALTER path is the only path a target can take through here.
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

    delta = governed(
        compute_delta(
            current,
            desired,
            widening_map,
            can_safely_widen=can_safely_widen,
            same_base=same_base,
        ),
        evolution,
    )

    evolving = SchemaDelta(
        table_name=delta.table_name,
        is_create=False,
        changes=[
            change
            for change in delta.changes
            if change.action in renderer.SUPPORTED_ACTIONS
        ],
    )
    for statement in renderer.render_sql(evolving):
        logger.info("Evolving %s: %s", target, statement)
        engine.execute(statement)
    return [change.column for change in evolving.changes]

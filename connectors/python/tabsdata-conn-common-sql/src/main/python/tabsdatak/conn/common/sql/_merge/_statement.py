#
# Copyright 2026 Tabsdata Inc.
#

"""Rendering the ``MERGE`` every SQL destination runs."""

from __future__ import annotations

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import MergeDialect, Staged
from tabsdatak.conn.common.sql.cdc.constants import OP_DELETE, OP_INSERT, OP_UPDATE

# The markers ``MergeCfg.op_column`` carries, and what each one makes the merge
# do with the row -- ``CdcOperation``'s, so a CDC source in this family feeds a
# merge without a translation step. Lower case, and the column is lower-cased
# before it is compared, so a feed emitting either case works.
#
# ``.value``, not the member: these are interpolated into the marker predicates,
# and formatting a str-mixin enum yields ``"CdcOperation.INSERT"`` -- which would
# be valid SQL that no row can ever match.
_INSERT = OP_INSERT.value
_UPDATE = OP_UPDATE.value
_DELETE = OP_DELETE.value


def render_merge(
    dialect: MergeDialect,
    target: str,
    staged: Staged,
    cfg: MergeCfg,
    columns: list[str],
) -> str:
    """The statement applying ``staged`` onto ``target``.

    ``columns`` are what gets written: the columns both sides have, without the
    marker. Naming them explicitly is what keeps the marker out of the target --
    the clauses read it off the source and no write list mentions it -- and it
    lifts the demand that both sides carry the same columns, so a batch wider or
    narrower than the target is not the statement's problem. A dialect whose
    grammar can say the same thing with a wildcard sets
    :attr:`MergeSyntax.wildcard_writes` and gets that instead.

    ``MergeCfg.op_column`` gates all three clauses on the marker each incoming
    row carries, lower-cased so a feed emitting either case is claimed. The
    DELETE clause precedes the UPDATE clause because these engines take the
    first matching ``WHEN MATCHED``: a row marked for deletion is deleted rather
    than falling through to the update.
    """
    syntax = dialect.syntax
    quote = dialect.quote_column
    into, using = syntax.target_alias, syntax.source_alias
    keys = cfg._keys()

    on = " AND ".join(f"{into}.{quote(key)} = {using}.{quote(key)}" for key in keys)
    marker = quote(cfg._op_column())

    def marked(op: str) -> str:
        return f" AND (LOWER({using}.{marker}) = '{op}')"

    clauses = [
        f"MERGE{syntax.evolution_modifier} INTO {target} AS {into}"
        f" USING {staged.relation} AS {using} ON {on}",
        f"WHEN MATCHED{marked(_DELETE)} THEN DELETE",
    ]

    if syntax.wildcard_writes is not None:
        written = syntax.wildcard_writes.format(marker=marker)
        clauses.append(f"WHEN MATCHED{marked(_UPDATE)} THEN UPDATE SET {written}")
        clauses.append(f"{syntax.not_matched}{marked(_INSERT)} THEN INSERT {written}")
        return " ".join(clauses)

    # Keys are what the row matched on, so an update never reassigns them.
    # Compared folded, because the write list carries the batch's spelling and
    # the keys their folded one. A batch carrying nothing but keys has nothing to
    # update and the clause is left off rather than emitted empty.
    folded_keys = {dialect.fold(key) for key in keys}
    updatable = [name for name in columns if dialect.fold(name) not in folded_keys]
    if updatable:
        assignments = ", ".join(
            f"{_assigned(syntax, quote, into, name)} = {using}.{quote(name)}"
            for name in updatable
        )
        clauses.append(f"WHEN MATCHED{marked(_UPDATE)} THEN UPDATE SET {assignments}")

    # One list, used twice, so the column and value orders cannot drift apart.
    inserted = ", ".join(quote(name) for name in columns)
    values = ", ".join(f"{using}.{quote(name)}" for name in columns)
    clauses.append(
        f"{syntax.not_matched}{marked(_INSERT)} THEN"
        f" INSERT ({inserted}) VALUES ({values})"
    )
    return " ".join(clauses)


def _assigned(syntax, quote, into: str, name: str) -> str:
    """The left-hand side of one update item."""
    return f"{into}.{quote(name)}" if syntax.update_qualified else quote(name)

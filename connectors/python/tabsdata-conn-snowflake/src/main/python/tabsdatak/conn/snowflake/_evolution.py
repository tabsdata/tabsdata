#
# Copyright 2026 Tabsdata Inc.
#

"""Evolving a Snowflake target with ``ALTER`` statements.

Snowflake has schema evolution of its own -- ``ENABLE_SCHEMA_EVOLUTION``,
which ``COPY INTO`` honours -- but it only ever adds a column. Diffing the
target against the incoming file instead reaches what that cannot: a column
whose type the batch has outgrown is widened, and a change Snowflake could
not make refuses the write rather than letting the load discover it.

A merge always comes through here -- the mark is read by ``COPY INTO`` and
not by a ``MERGE``, so a merge target is evolved here or not at all. A bulk
load comes through only when the write asks for it with
``dest_cfg={"snowflake.schema_evolution_engine": "iceberg"}``.
"""

from __future__ import annotations

import logging

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._evolution import evolve_with_iceberg
from tabsdatak.conn.snowflake import _iceberg

logger = logging.getLogger(__name__)


def fold_identifier(name: str) -> str:
    """Snowflake upper-folds an unquoted identifier, and the load matches
    columns with ``MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE``, so upper case is
    the form two names are the same in."""
    return name.upper()


def describe_table_columns(cursor, target: str) -> list[tuple[str, str]]:
    """``(name, type)`` per column ``target`` currently has -- the name in the
    case Snowflake stores it under, the type in the exact spelling a statement
    can use: ``NUMBER(38,0)``, ``VARCHAR(16777216)``, ``TIMESTAMP_NTZ(9)``.

    ``DESC TABLE`` is the read that reports a type in that form; the
    description of an empty result set carries the names but only the driver's
    own type codes. Its output columns are located by name in
    ``cursor.description`` rather than by position, so nothing here depends on
    where Snowflake puts them or on how many of them there are.
    """
    cursor.execute(f"DESC TABLE {target}")
    fields = [field[0].upper() for field in cursor.description]
    name_at, type_at = fields.index("NAME"), fields.index("TYPE")
    return [(row[name_at], row[type_at]) for row in cursor.fetchall()]


def aligned_type(stored: str | None, inferred: str) -> str:
    """``inferred`` in the target's own spelling when the difference between the
    two is not one to act on.

    The diff compares type strings, and the two sides come from different
    vocabularies: ``DESC TABLE`` reports ``VARCHAR(16777216)`` where
    ``infer_schema`` reports ``TEXT`` for the very same column. Left alone,
    every such column reads as a retype -- an unsafe one, since there is nothing
    to widen -- and the write is refused over a column nobody touched.

    Two spellings are folded together on either of two grounds: they name one
    type, or they differ only in the parameters of a base Snowflake will not
    alter. ``TIMESTAMP_NTZ(9)`` against ``TIMESTAMP_NTZ(6)`` is the second kind:
    a real difference, but ``ALTER COLUMN`` reaches the parameters of ``NUMBER``
    and ``VARCHAR`` alone, so there is no statement to render and the load casts
    into the target column as it always has. Refusing the write for it would
    stop a load that works.

    What survives the folding is a retype worth reporting: a column whose type
    really moved within a base that can be altered, or one that changed base
    outright.
    """
    if stored is None:
        return inferred
    if _iceberg.same_type(stored, inferred) or _iceberg.same_unalterable_base(
        stored, inferred
    ):
        return stored
    return inferred


class SnowflakeSchemaEvolver:
    """The three operations the Iceberg evolution pass makes against Snowflake.

    Satisfies
    :class:`~tabsdatak.conn.common.sql._evolution.SchemaEvolutionTarget`
    over a bare cursor, which is what lets a bulk load evolve its target without
    a merge dialect to borrow one from. The merge dialect implements the same
    three methods itself -- they are part of its own contract -- and reads them
    from the same helpers above, so the two paths cannot drift.
    """

    def __init__(self, cursor) -> None:
        self._cursor = cursor

    def fold(self, name: str) -> str:
        return fold_identifier(name)

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        return describe_table_columns(self._cursor, target)

    def execute(self, statement: str) -> None:
        self._cursor.execute(statement)

    def evolve(self, target: str, columns: list[tuple[str, str]]) -> list[str]:
        """``ALTER`` ``target`` to meet ``columns``, returning what changed.

        ``strict``: a retype Snowflake cannot make refuses the whole write
        rather than staging rows into a column that cannot hold them. That is
        only safe because :func:`aligned_type` has already folded away the
        differences that are not real ones -- without it the two vocabularies
        would refuse nearly every write.
        """
        return evolve_with_iceberg(
            self,
            target,
            columns,
            renderer=_iceberg.SnowflakeRenderer(),
            widening_map=_iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
            can_safely_widen=_iceberg.can_widen,
            align_type=aligned_type,
            strict=True,
        )

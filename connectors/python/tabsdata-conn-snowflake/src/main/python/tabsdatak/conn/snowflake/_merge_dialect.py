#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake's half of the shared merge driver.

The ordering, the CDC coalesce and the ``MERGE`` itself live in
``tabsdatak.conn.common.sql._merge``; this module is only what Snowflake
does differently -- staging through a named stage, templating relations
from ``infer_schema``, and evolving the target with ``ALTER`` statements
because ``ENABLE_SCHEMA_EVOLUTION`` does not reach a ``MERGE``.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from collections.abc import Iterator
from pathlib import Path

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import fqn, unfqn

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._evolution import evolve_with_iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import (
    MergeDialect,
    MergeErrors,
    MergeSyntax,
    Staged,
)
from tabsdatak.conn.snowflake import _iceberg
from tabsdatak.conn.snowflake._staging import (
    copy_into,
    infer_schema_call,
    inferred_schema_template,
    put_file_to_stage,
)

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake._validation import (
    _SNOWFLAKE_FQN_DEF,
    MERGE_SOURCE_ALIAS,
    MERGE_TARGET_ALIAS,
)
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

__all__ = ["SnowflakeMergeDialect"]

logger = logging.getLogger(__name__)

_SYNTAX = MergeSyntax(
    target_alias=MERGE_TARGET_ALIAS,
    source_alias=MERGE_SOURCE_ALIAS,
    # Snowflake evolves nothing from inside a MERGE -- ENABLE_SCHEMA_EVOLUTION
    # is honoured by COPY INTO alone -- and its grammar has no EXCEPT, so both
    # write lists are spelled out column by column.
    evolution_modifier="",
    not_matched="WHEN NOT MATCHED",
    wildcard_writes=None,
)

_ERRORS = MergeErrors(
    marker_absent=SnowflakeErrorCode.SNOWFLAKE_18.exception,
    keys_absent=SnowflakeErrorCode.SNOWFLAKE_19.exception,
    no_shared_columns=SnowflakeErrorCode.SNOWFLAKE_16.exception,
    ambiguous_column=SnowflakeErrorCode.SNOWFLAKE_20.exception,
)


class SnowflakeMergeDialect(MergeDialect):
    """Drives a merge over one open Snowflake cursor and one stage.

    Built per (file, table) pair by ``_impl``, which already holds the session:
    the namespace, the warehouse, the stage and the parquet file format are set
    up once per connection and are not this object's business.
    """

    def __init__(
        self,
        cursor,
        stage: str,
        *,
        match_by_column_name: str,
        purge: str,
    ) -> None:
        self._cursor = cursor
        self._stage = stage
        self._match_by_column_name = match_by_column_name
        self._purge = purge

    @property
    def syntax(self) -> MergeSyntax:
        return _SYNTAX

    @property
    def errors(self) -> MergeErrors:
        return _ERRORS

    def quote_column(self, name: str) -> str:
        return _iceberg.quote_column(name)

    def fold(self, name: str) -> str:
        """Snowflake upper-folds an unquoted identifier, and the load matches
        columns with ``MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE``, so upper case
        is the form two names are the same in."""
        return name.upper()

    @contextlib.contextmanager
    def staging(self, target: str, batch: Path) -> Iterator[Staged]:
        """``PUT`` the batch, template a temporary table from it and ``COPY``
        the rows in.

        The scratch table carries the file's whole schema, marker column and
        all: the clauses read the marker off it. Nothing drops it on the way
        out -- a ``TEMPORARY`` table is session-scoped and goes with the
        connection, and its name carries a suffix unique to this call, so the
        few a multi-table write leaves behind collide with nothing.

        The columns are read from the file before the ``COPY``, which purges it:
        after that the staged file is gone and ``infer_schema`` has nothing to
        report.
        """
        file_name = put_file_to_stage(self._cursor, batch, self._stage)
        columns = self._staged_columns(file_name)
        scratch = self._create_scratch_table(target, file_name)
        copy_into(
            self._cursor,
            self._stage,
            file_name,
            scratch,
            match_by_column_name=self._match_by_column_name,
            purge=self._purge,
        )
        yield Staged(relation=scratch, columns=columns)

    def ensure_target(self, target: str, staged: Staged, marker: str) -> None:
        """``CREATE TABLE IF NOT EXISTS`` from the scratch table's schema, less
        the marker.

        Built from the staged relation rather than from the file, because by
        now the ``COPY`` has purged it. ``LIMIT 0`` because the rows arrive
        through the ``MERGE``, not through this.
        """
        excluded = ", ".join(
            self.quote_column(name)
            for name, _ in staged.columns
            if self.fold(name) == self.fold(marker)
        )
        projection = f"* EXCLUDE ({excluded})" if excluded else "*"
        self._cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {target} AS"
            f" SELECT {projection} FROM {staged.relation} LIMIT 0"
        )

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        """``(name, type)`` per column ``target`` currently has -- the name in
        the case Snowflake stores it under, the type in the exact spelling a
        statement can use: ``NUMBER(38,0)``, ``VARCHAR(16777216)``,
        ``TIMESTAMP_NTZ(9)``.

        ``DESC TABLE`` is the read that reports a type in that form; the
        description of an empty result set carries the names but only the
        driver's own type codes. Its output columns are located by name in
        ``cursor.description`` rather than by position, so nothing here depends
        on where Snowflake puts them or on how many of them there are.
        """
        self._cursor.execute(f"DESC TABLE {target}")
        fields = [field[0].upper() for field in self._cursor.description]
        name_at, type_at = fields.index("NAME"), fields.index("TYPE")
        return [(row[name_at], row[type_at]) for row in self._cursor.fetchall()]

    def execute(self, statement: str) -> None:
        self._cursor.execute(statement)

    def evolve(self, target: str, staged: Staged, marker: str) -> list[str]:
        """Add the columns the batch has and the target does not.

        ``ENABLE_SCHEMA_EVOLUTION`` is honoured by ``COPY INTO`` and not by a
        ``MERGE``, so a merge target is evolved here or not at all.

        Adding is the whole of it: :class:`SnowflakeRenderer` supports
        ``ADD_COLUMN`` alone, so a retype the diff reports renders to nothing.
        That is also why ``strict`` is False -- the target's types come from
        ``DESC TABLE`` and the batch's from ``infer_schema``, which spell the
        same type differently, so a column neither side touched reads as an
        unsafe retype. Refusing the write for it would refuse nearly all of
        them.
        """
        return evolve_with_iceberg(
            self,
            target,
            staged,
            marker,
            renderer=_iceberg.SnowflakeRenderer(),
            widening_map=_iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
            strict=False,
        )

    # --- Snowflake internals ------------------------------------------

    def _staged_columns(self, file_name: str) -> list[tuple[str, str]]:
        self._cursor.execute(
            f"SELECT COLUMN_NAME, TYPE FROM table"
            f" ({infer_schema_call(self._stage, file_name)})"
        )
        return [(row[0], row[1]) for row in self._cursor.fetchall()]

    def _create_scratch_table(self, target: str, file_name: str) -> str:
        scratch = _scratch_table_name(target)
        logger.debug("Creating scratch table %s for %s", scratch, target)
        self._cursor.execute(
            f"CREATE TEMPORARY TABLE {scratch} USING template ("
            f"{inferred_schema_template(self._stage, file_name)}"
            f")"
        )
        return scratch


def _scratch_table_name(target: str) -> str:
    """A name for a scratch table beside ``target``, unique per call.

    Keeps the target's database / schema parts, so the table is created where
    the write is happening, and suffixes only the last part. The suffix is what
    makes it safe: a temporary table shadows a permanent one of the same name
    for the rest of the session, so it must never be spelled exactly like the
    target it is about to be merged into.
    """
    parts = unfqn(target, _SNOWFLAKE_FQN_DEF)
    parts[-1] = f"{parts[-1]}_{uuid.uuid4().hex[:8].upper()}"
    return fqn(parts, _SNOWFLAKE_FQN_DEF)

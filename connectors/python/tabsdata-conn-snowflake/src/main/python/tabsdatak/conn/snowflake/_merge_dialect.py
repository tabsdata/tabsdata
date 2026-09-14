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
from tabsdatak.conn.common.sql._evolution import evolve_with_iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import WatermarkCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import (
    MergeDialect,
    MergeErrors,
    MergeSyntax,
    Staged,
)
from tabsdatak.conn.snowflake import _iceberg
from tabsdatak.conn.snowflake._evolution import (
    WATERMARK_TYPE,
    aligned_type,
    check_watermark_column as _check_watermark_column,
    describe_table_columns,
    ensure_watermark_column as _ensure_watermark_column,
    fold_identifier,
)
from tabsdatak.conn.snowflake._staging import (
    copy_into,
    inferred_columns,
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
        return fold_identifier(name)

    @contextlib.contextmanager
    def staging(
        self, target: str, batch: Path, watermark_cfg: WatermarkCfg | None = None
    ) -> Iterator[Staged]:
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

        A watermark column is added to the scratch table once the rows are in,
        not to its template: the batch does not carry the column, so the
        ``COPY`` would have nothing to match it against. Adding it afterwards
        with the transaction's id as its default stamps every row that was just
        loaded, and reporting it among the staged columns is what carries it
        through ``ensure_target``, ``evolve`` and the write list onto the
        target.
        """
        file_name = put_file_to_stage(self._cursor, batch, self._stage)
        columns = inferred_columns(self._cursor, self._stage, file_name)
        scratch = self._create_scratch_table(target, file_name)
        copy_into(
            self._cursor,
            self._stage,
            file_name,
            scratch,
            match_by_column_name=self._match_by_column_name,
            purge=self._purge,
        )
        if watermark_cfg is not None:
            self._add_watermark_column(scratch, watermark_cfg)
            columns.append((watermark_cfg.column, WATERMARK_TYPE))
        yield Staged(relation=scratch, columns=columns)

    def check_watermark_column(self, target: str, watermark_cfg: WatermarkCfg) -> bool:
        """Whether ``target`` already holds a row stamped with this
        transaction's id -- see
        :func:`~tabsdatak.conn.snowflake._evolution.check_watermark_column`,
        which an append reaches for too.

        ``watermark_cfg.column`` has already been through ``MergeCfg._fold``,
        which is the stored form the probe matches on.
        """
        return _check_watermark_column(
            self._cursor,
            target,
            watermark_cfg.column,
            watermark_cfg.transaction_id,
        )

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
        return describe_table_columns(self._cursor, target)

    def execute(self, statement: str) -> None:
        self._cursor.execute(statement)

    def evolve(self, target: str, staged: Staged, marker: str) -> list[str]:
        """Add the columns the batch has and the target does not, and widen a
        column whose type the batch has outgrown.

        ``ENABLE_SCHEMA_EVOLUTION`` is honoured by ``COPY INTO`` and not by a
        ``MERGE``, so a merge target is evolved here or not at all.

        ``strict``: a retype Snowflake cannot make refuses the whole write
        rather than staging rows into a column that cannot hold them. That is
        only safe because ``align_type`` has already folded away the
        differences that are not real ones -- without it the two vocabularies
        would refuse nearly every write.
        """
        return evolve_with_iceberg(
            self,
            target,
            staged.columns,
            marker=marker,
            renderer=_iceberg.SnowflakeRenderer(),
            widening_map=_iceberg.SNOWFLAKE_TYPE_WIDENING_MAP,
            can_safely_widen=_iceberg.can_widen,
            align_type=aligned_type,
            strict=True,
        )

    def ensure_watermark_column(
        self, target: str, watermark_cfg: WatermarkCfg | None
    ) -> list[str]:
        """``ALTER TABLE`` the watermark column onto the target under strict.

        Strict holds the target's schema still, and this is the one column it
        cannot hold back: nothing else adds it, so without this the target never
        gains it, the merge never writes it, and the rerun guard reads every run
        as the first one -- a watermark that silently does nothing.

        Unlike the scratch table's copy it is added without a ``DEFAULT`` --
        see :func:`~tabsdatak.conn.snowflake._evolution.ensure_watermark_column`,
        which an append reaches for too.
        """
        if watermark_cfg is None:
            return []
        added = _ensure_watermark_column(self._cursor, target, watermark_cfg.column)
        return [watermark_cfg.column] if added else []

    # --- Snowflake internals ------------------------------------------

    def _create_scratch_table(self, target: str, file_name: str) -> str:
        scratch = _scratch_table_name(target)
        logger.debug("Creating scratch table %s for %s", scratch, target)
        self._cursor.execute(
            f"CREATE TEMPORARY TABLE {scratch} USING template ("
            f"{inferred_schema_template(self._stage, file_name)}"
            f")"
        )
        return scratch

    def _add_watermark_column(self, table: str, cfg: WatermarkCfg) -> None:
        """Stamp every row of ``table`` with the transaction's id.

        The default is what does the stamping: Snowflake backfills a constant
        default into the rows already there, so the ``COPY``'s rows carry it
        without a second pass over them.
        """
        logger.debug("Adding watermark column %s to %s", cfg.column, table)
        self._cursor.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS"
            f" {self.quote_column(cfg.column)} {WATERMARK_TYPE}"
            f" DEFAULT '{cfg.transaction_id}'"
        )


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

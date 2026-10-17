#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks' half of the shared merge driver.

The ordering, the CDC coalesce and the ``MERGE`` itself live in
``tabsdatak.conn.common.sql._merge``; this module is only what Databricks
does differently -- staging through a Unity Catalog volume, and letting
the engine evolve the target from inside the statement rather than
ALTERing it beforehand.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import WatermarkCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import (
    MergeDialect,
    MergeErrors,
    MergeSyntax,
    Staged,
)
from tabsdatak.conn.databricks.error import DatabricksErrorCode

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_ERRORS = MergeErrors(
    marker_absent=DatabricksErrorCode.DATABRICKS_19.exception,
    keys_absent=DatabricksErrorCode.DATABRICKS_20.exception,
    no_shared_columns=DatabricksErrorCode.DATABRICKS_22.exception,
    ambiguous_column=DatabricksErrorCode.DATABRICKS_21.exception,
)


class DatabricksMergeDialect(MergeDialect):
    """Drives a merge over one open warehouse cursor, one volume and one target.

    Built per (file, table) pair by ``_impl``, which already holds the session:
    the workspace client and the SQL connection are opened once per write and
    are not this object's business.

    Args:
        cursor: The open warehouse cursor every statement runs on.
        ws_client: The workspace client the volume upload goes through.
        conn: The destination connection, read for the volume path.
        fq_table: The target's bare ``catalog.schema.table``, which is what the
            volume path is derived from. The quoted form the statements use is
            what the driver passes as ``target``.
        schema_evolution: ``"update"`` or ``"strict"``. Databricks evolves from
            inside the ``MERGE``, so this decides the statement's own shape as
            well as the property set on the target.
        copy_options: Extra ``COPY INTO`` options, appended verbatim.
        copy_kwargs: Extra keyword arguments for the ``COPY INTO`` execute.
    """

    def __init__(
        self,
        cursor,
        ws_client,
        conn,
        fq_table: str,
        *,
        schema_evolution: str,
        copy_options: str = "",
        copy_kwargs: dict | None = None,
    ) -> None:
        self._cursor = cursor
        self._ws_client = ws_client
        self._conn = conn
        self._fq_table = fq_table
        self._schema_evolution = schema_evolution
        self._copy_options = copy_options
        self._copy_kwargs = copy_kwargs or {}

    @property
    def _evolving(self) -> bool:
        return self._schema_evolution == "update"

    @property
    def syntax(self) -> MergeSyntax:
        """The statement's shape, which here depends on the write's own setting.

        ``WITH SCHEMA EVOLUTION`` is what lets a batch carrying a new column
        land it on the target; under ``"strict"`` it is left off and Databricks
        rejects the write instead.

        The marker is excluded from the wildcard only while the schema is
        evolving, because that is the only case where it could reach the target:
        evolution widens ``*`` to the columns the source has and the target does
        not, which is exactly what the marker is. With evolution off, ``*``
        covers the target's own columns, and the marker is not among them.

        A batch that loads or truncates the table replaces the target's rows in
        one ``MERGE ... ON FALSE``, which evolves the target the same way.
        """
        return MergeSyntax(
            target_alias="target",
            source_alias="source",
            evolution_modifier=" WITH SCHEMA EVOLUTION" if self._evolving else "",
            not_matched="WHEN NOT MATCHED BY TARGET",
            wildcard_writes="* EXCEPT ({marker})" if self._evolving else "*",
            replace="merge",
        )

    @property
    def errors(self) -> MergeErrors:
        return _ERRORS

    def quote_column(self, name: str) -> str:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import _quote_column

        return _quote_column(name)

    def fold(self, name: str) -> str:
        """Databricks resolves identifiers case-insensitively, and backticks do
        not make a name case-sensitive, so lower case is the form two names are
        the same in."""
        return name.lower()

    def adapt_batch(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Narrow what Delta cannot store, inside the coalesce's rewrite."""
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import narrow_timestamps

        return narrow_timestamps(frame)

    @contextlib.contextmanager
    def staging(
        self, target: str, batch: Path, watermark_cfg: WatermarkCfg | None = None
    ) -> Iterator[Staged]:
        """Upload the batch to the volume, build a table from its schema and
        ``COPY`` the rows in.

        The scratch table carries the file's whole schema, marker column and
        all: the clauses read the marker off it. It is a real catalog table
        rather than a session relation, so it outlives the connection and has to
        be dropped here -- as does the staged file, which the volume keeps until
        it is deleted.

        A watermark column is added to the scratch table once the rows are in,
        not to its schema: the batch does not carry the column, so the ``COPY``
        would have nothing to fill it from. Stamping it there is what puts the
        id on the rows the ``MERGE`` goes on to write.
        """
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import (
            _create_temp_table_from_volume_schema,
            _execute_volume_statement,
            delete_from_volume,
            drop_temp_table,
            upload_file_to_volume,
        )

        staged_file = upload_file_to_volume(
            self._ws_client, self._conn, self._fq_table, batch
        )
        try:
            scratch = _create_temp_table_from_volume_schema(
                self._cursor, staged_file, self._fq_table
            )
            try:
                _execute_volume_statement(
                    self._cursor,
                    f"COPY INTO {scratch} FROM '{staged_file}' "
                    f"FILEFORMAT = PARQUET"
                    f"{self._copy_options}",
                    self._copy_kwargs,
                )
                if watermark_cfg is not None:
                    self._stamp(scratch, watermark_cfg)
                yield Staged(relation=scratch, columns=self._relation_columns(scratch))
            finally:
                drop_temp_table(self._cursor, scratch)
        finally:
            logger.debug("databricks: deleting staged file %s", staged_file)
            delete_from_volume(self._ws_client, staged_file)

    def ensure_target(self, target: str, staged: Staged, marker: str) -> None:
        """``CREATE TABLE IF NOT EXISTS`` from the scratch table's schema, less
        the marker, and mark the target for type widening.

        The marker says what to do with a row and is never part of the row, so a
        target this call brings into being must not carry it either -- nothing
        would ever write it, and every row would read NULL.

        ``delta.enableTypeWidening`` is set on every merge and not only when
        evolving: a target an earlier update-mode run marked would otherwise go
        on widening after a run asked for strict. It is what lets the ``MERGE``
        land a value the target's column type could not hold -- the type half of
        what ``WITH SCHEMA EVOLUTION`` does for whole columns.
        """
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import _volume_schema_projection

        excluded = [
            name for name, _ in staged.columns if self.fold(name) == self.fold(marker)
        ]
        logger.debug("Creating target table %s for merge", target)
        self._cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {target} USING DELTA AS SELECT "
            f"{_volume_schema_projection(excluded)} "
            f"FROM {staged.relation} LIMIT 0"
        )
        widening = str(self._evolving).lower()
        logger.debug("Setting delta.enableTypeWidening=%s for %s", widening, target)
        self._cursor.execute(
            f"ALTER TABLE {target} "
            f"SET TBLPROPERTIES ('delta.enableTypeWidening' = '{widening}')"
        )

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        """``(name, type)`` per column ``target`` currently has.

        ``DESCRIBE TABLE`` reports more than the columns -- a partition or
        metadata block follows them, opening with a blank name -- so the walk
        stops there. The fields are located by name in ``cursor.description``
        rather than by position.
        """
        return self._relation_columns(target)

    def execute(self, statement: str) -> None:
        logger.debug("Executing: %s", statement)
        self._cursor.execute(statement)

    def evolve(self, target: str, staged: Staged, marker: str) -> list[str]:
        """Nothing to do: Databricks evolves the target from inside the
        ``MERGE``.

        ``WITH SCHEMA EVOLUTION`` adds the columns the batch has and the target
        does not, and ``delta.enableTypeWidening`` -- set by
        :meth:`ensure_target` -- widens a column whose type the batch has
        outgrown. Both happen as the statement runs, so there is nothing to
        ALTER beforehand and nothing to report back: the driver's caller learns
        what changed by reading the target, not from here.
        """
        return []

    def check_watermark_column(self, target: str, watermark_cfg: WatermarkCfg) -> bool:
        """Whether ``target`` already holds a row stamped with this
        transaction's id.

        Asked of ``information_schema.columns`` first, and only then of the
        table: a target that does not exist yet, or one that predates the
        watermark column, has no such row and must not be probed for one --
        selecting a column the table has not got is an error rather than an
        answer. The column view covers both, since a missing table contributes
        no rows to it.
        """
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import check_watermark_column

        return check_watermark_column(
            self._cursor,
            self._fq_table,
            target,
            watermark_cfg.column,
            watermark_cfg.transaction_id,
        )

    def ensure_watermark_column(
        self, target: str, watermark_cfg: WatermarkCfg | None
    ) -> list[str]:
        """``ALTER TABLE`` the watermark column onto the target under strict.

        The one thing strict has to do here. Under ``"update"`` the ``MERGE``
        carries ``WITH SCHEMA EVOLUTION`` and lands the column itself, so this is
        never reached; with the modifier off, the statement writes through ``*``,
        which resolves against the columns the *target* has -- so a target
        without the column would never be written one, nothing would be stamped,
        and the rerun guard would read every run as the first.

        Added bare, with no value: the rows already in the target were written by
        earlier transactions and keep the NULL a new column starts with, and the
        rows this merge writes take their id from the staged copy through the
        statement itself.
        """
        if watermark_cfg is None:
            return []
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import ensure_watermark_column

        added = ensure_watermark_column(
            self._cursor, self._fq_table, target, watermark_cfg.column
        )
        return [watermark_cfg.column] if added else []

    # --- Databricks internals -----------------------------------------

    def _stamp(self, relation: str, watermark_cfg: WatermarkCfg) -> None:
        """Give the scratch table the watermark column and fill it in.

        Two statements: a Delta column added with a ``DEFAULT`` reaches rows
        inserted afterwards, and every row here is already in, so the value is
        written with an ``UPDATE``.
        """
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks._impl import WATERMARK_TYPE, string_literal

        column = self.quote_column(watermark_cfg.column)
        self.execute(f"ALTER TABLE {relation} ADD COLUMNS ({column} {WATERMARK_TYPE})")
        self.execute(
            f"UPDATE {relation} SET {column} ="
            f" {string_literal(watermark_cfg.transaction_id)}"
        )

    def _relation_columns(self, relation: str) -> list[tuple[str, str]]:
        self._cursor.execute(f"DESCRIBE TABLE {relation}")
        rows = self._cursor.fetchall()
        fields = [field[0].lower() for field in self._cursor.description]
        name_at, type_at = fields.index("col_name"), fields.index("data_type")
        columns = []
        for row in rows:
            name = row[name_at]
            if not name or name.startswith("#"):
                break
            columns.append((name, row[type_at]))
        return columns

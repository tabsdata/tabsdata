#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery's half of the shared merge driver.

The ordering, the CDC coalesce and the ``MERGE`` itself live in
``tabsdatak.conn.common.sql._merge``; this module is only what BigQuery
does differently -- staging through a GCS object and a load job, and
evolving the target with ``ALTER`` statements because a ``MERGE`` here
evolves nothing on its own.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from tabsdatak.conn.bigquery import _iceberg
from tabsdatak.conn.bigquery.error import BigQueryErrorCode

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

logger = logging.getLogger(__name__)

_SYNTAX = MergeSyntax(
    target_alias="target",
    source_alias="source",
    # A MERGE evolves nothing here, and BigQuery has no ``UPDATE SET *`` -- an
    # update item names one column -- so both write lists are spelled out.
    evolution_modifier="",
    not_matched="WHEN NOT MATCHED BY TARGET",
    wildcard_writes=None,
    # An update item takes a bare column name: it can only resolve against the
    # target, and BigQuery reads a qualified name there as an error.
    update_qualified=False,
)

# A trx id is an opaque string, and STRING is what the SchemaField walk reports
# back for the column, so a target that already has it does not read as a
# retype of it.
_WATERMARK_TYPE = "STRING"

_ERRORS = MergeErrors(
    marker_absent=BigQueryErrorCode.BIGQUERY_15.exception,
    keys_absent=BigQueryErrorCode.BIGQUERY_16.exception,
    no_shared_columns=BigQueryErrorCode.BIGQUERY_20.exception,
    ambiguous_column=BigQueryErrorCode.BIGQUERY_19.exception,
    keys_absent_from_target=BigQueryErrorCode.BIGQUERY_17.exception,
)


def _string_literal(value: str) -> str:
    """``value`` as a GoogleSQL string literal.

    The trx id is a system-generated token, but it is the one value here that
    does not come from the connector itself, and a statement built by hand has
    nowhere to bind it.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


class BigQueryMergeDialect(MergeDialect):
    """Drives a merge over one BigQuery client, one GCS object and one target.

    Built per (file, table) pair by ``_plugin``, which already holds the
    clients and has worked out where the batch is to be staged.

    Args:
        bq_client: The BigQuery client every statement and load job runs on.
        gcs_client: The storage client the batch is uploaded through.
        uri: The ``gs://`` object the batch is uploaded to. Deleting it is the
            plugin's business, not this object's -- it owns the best-effort
            cleanup of every staged file a write made, merge or not.
        target_fqn: The target's bare ``project.dataset.table``. The quoted form
            the statements use is what the driver passes as ``target``.
    """

    def __init__(
        self,
        bq_client: Any,
        gcs_client: Any,
        *,
        uri: str,
        target_fqn: str,
    ) -> None:
        self._bq_client = bq_client
        self._gcs_client = gcs_client
        self._uri = uri
        self._target_fqn = target_fqn

    @property
    def syntax(self) -> MergeSyntax:
        return _SYNTAX

    @property
    def errors(self) -> MergeErrors:
        return _ERRORS

    def quote_column(self, name: str) -> str:
        return _iceberg.quote_column(name)

    def fold(self, name: str) -> str:
        """BigQuery resolves a column name case-insensitively and refuses a table
        two columns differing only in case, so lower case is the form two names
        are the same in. Backticks quote a name; they do not make it
        case-sensitive."""
        return name.lower()

    @contextlib.contextmanager
    def staging(
        self, target: str, batch: Path, watermark_cfg: WatermarkCfg | None = None
    ) -> Iterator[Staged]:
        """Upload the batch to GCS and load it into a staging table.

        A ``MERGE`` cannot read a parquet object, so the batch is loaded into a
        table of its own and the statement takes that as its source. The staging
        table carries the file's whole schema, marker column and all: the clauses
        read the marker off it.

        A watermark column is added to the staging table once the rows are in,
        not to the load: the batch does not carry the column, so the load job
        would have nothing to fill it from. Reporting it among the staged columns
        is what carries it through ``ensure_target``, ``evolve`` and the write
        list onto the target.

        The table is dropped on the way out -- it is a real table, not a session
        relation. The GCS object is not: the plugin deletes every URI a write
        staged, whichever disposition wrote it.
        """
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._impl import (
            build_merge_staging_load_job_config,
            table_column_types,
            temp_table_fqn,
        )

        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._plugin import _load_table_from_uri, _upload_to_gcs

        try:
            _upload_to_gcs(self._gcs_client, batch, self._uri)
        except Exception as e:
            raise BigQueryErrorCode.BIGQUERY_6.exception(cause=e, uri=self._uri)

        staging_table = temp_table_fqn(self._target_fqn)
        logger.debug(
            "bigquery: staging the merge batch for %r in %r",
            self._target_fqn,
            staging_table,
        )
        try:
            _load_table_from_uri(
                self._bq_client,
                uri=self._uri,
                table=staging_table,
                job_config=build_merge_staging_load_job_config(),
            )
        except Exception as e:
            raise BigQueryErrorCode.BIGQUERY_7.exception(cause=e, table=staging_table)

        try:
            relation = self.quote_table(staging_table)
            columns = table_column_types(self._bq_client, staging_table)
            if watermark_cfg is not None:
                self._stamp(relation, watermark_cfg)
                columns.append((watermark_cfg.column, _WATERMARK_TYPE))
            yield Staged(relation=relation, columns=columns)
        finally:
            logger.debug("bigquery: dropping staging table %r", staging_table)
            self._bq_client.delete_table(staging_table, not_found_ok=True)

    def ensure_target(self, target: str, staged: Staged, marker: str) -> None:
        """Create the target from the staging table's schema when it is not there.

        A merge reads the target to decide what it can write, so the target has
        to exist before the first batch is merged onto it. It is built from the
        staging table rather than from the parquet, so the column types are the
        ones BigQuery itself settled on when it loaded the batch -- the target
        and the source cannot disagree about what a column is.

        The marker is left out: it says what to do with a row and is never part
        of the row, so a target created here never has it.

        An existing target is left exactly as it is; adding to it and widening
        it are :meth:`evolve`'s job. The rendered DDL still carries ``IF NOT EXISTS``,
        so a second run that read the same missing target creates nothing rather
        than failing.
        """
        from google.cloud.exceptions import NotFound

        try:
            self._bq_client.get_table(self._target_fqn)
            return
        except NotFound:
            pass

        desired = _iceberg.table_schema_from_columns(
            target,
            [
                (name, type_)
                for name, type_ in staged.columns
                if self.fold(name) != self.fold(marker)
            ],
        )
        if not desired.columns:
            # Nothing but the marker was staged. A CREATE with no columns is not
            # a statement; the merge reports what the batch is missing.
            return
        delta = _iceberg.compute_delta(
            None, desired, _iceberg.BIGQUERY_TYPE_WIDENING_MAP
        )
        (statement,) = _iceberg.render_bigquery_sql(delta)
        self._bq_client.query_and_wait(statement)

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        """The ``(name, type)`` pairs of the target, in the table's own order."""
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._impl import table_column_types

        return table_column_types(self._bq_client, self._target_fqn)

    def execute(self, statement: str) -> None:
        logger.debug("bigquery: executing %s", statement)
        self._bq_client.query_and_wait(statement)

    def evolve(self, target: str, staged: Staged, marker: str) -> list[str]:
        """``ALTER TABLE`` the target to add every column the batch has and it
        does not, and to widen a column whose type the batch has outgrown.

        A ``MERGE`` evolves nothing here -- there is no modifier that would make
        it -- so a merge target gains a column, or a wider type, through this
        pass or not at all.

        Both sides' types are read by the same walk over a ``SchemaField``, so a
        column neither side touched reports as unchanged and there is no
        vocabulary to reconcile first. That is what makes ``strict`` safe: the
        retypes the diff reports are real, and one BigQuery cannot make refuses
        the whole write rather than staging rows into a column that cannot hold
        them.
        """
        return evolve_with_iceberg(
            self,
            target,
            staged.columns,
            marker=marker,
            renderer=_iceberg.BigQueryRenderer(),
            widening_map=_iceberg.BIGQUERY_TYPE_WIDENING_MAP,
            can_safely_widen=_iceberg.can_widen,
            strict=True,
        )

    def check_watermark_column(self, target: str, watermark_cfg: WatermarkCfg) -> bool:
        """Whether ``target`` already holds a row stamped with this
        transaction's id.

        The target's own schema answers the first half, so a target that does
        not exist yet, or one that predates the watermark column, reports
        unstamped without being queried -- selecting a column the table has not
        got is an error rather than an answer.
        """
        columns = self._existing_columns()
        if columns is None or self.fold(watermark_cfg.column) not in columns:
            return False
        rows = self._bq_client.query_and_wait(
            f"SELECT COUNT(*) > 0 FROM {target}"
            f" WHERE {self.quote_column(watermark_cfg.column)}"
            f" = {_string_literal(watermark_cfg.transaction_id)}"
        )
        return any(row[0] for row in rows)

    def ensure_watermark_column(
        self, target: str, watermark_cfg: WatermarkCfg | None
    ) -> list[str]:
        """``ALTER TABLE`` the watermark column onto the target under strict.

        Strict holds the target's schema still, and this is the one column it
        cannot hold back: nothing else adds it, so without this the target never
        gains it, the merge never writes it, and the rerun guard reads every run
        as the first one -- a watermark that silently does nothing.

        The rows already in the target were written by earlier transactions and
        are left alone, so they keep the NULL a new column starts with. That is
        what "written before there was a watermark" looks like; stamping them
        with this trx's id would claim them for it.
        """
        if watermark_cfg is None:
            return []
        columns = self._existing_columns()
        if columns is None or self.fold(watermark_cfg.column) in columns:
            return []
        logger.debug(
            "bigquery: adding watermark column %r to %r",
            watermark_cfg.column,
            self._target_fqn,
        )
        self.execute(
            f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS"
            f" {self.quote_column(watermark_cfg.column)} {_WATERMARK_TYPE}"
        )
        return [watermark_cfg.column]

    # --- BigQuery internals -------------------------------------------

    def _existing_columns(self) -> set[str] | None:
        """The target's folded column names, or ``None`` when it is not there.

        A missing target is not an error here: both watermark passes run before
        anything has been written to it, and "no table" and "no such column"
        lead to the same answer.
        """
        from google.cloud.exceptions import NotFound

        try:
            return {
                self.fold(name) for name, _ in self.target_columns(self._target_fqn)
            }
        except NotFound:
            return None

    def _stamp(self, relation: str, watermark_cfg: WatermarkCfg) -> None:
        """Give the staging table the watermark column and fill it in.

        Two statements where Snowflake needs one: a ``DEFAULT`` on an added
        BigQuery column applies to rows inserted afterwards, and every row here
        is already in, so the value is written with an ``UPDATE``. BigQuery
        requires a ``WHERE`` on one, and every staged row is to be stamped.
        """
        column = self.quote_column(watermark_cfg.column)
        self.execute(
            f"ALTER TABLE {relation} ADD COLUMN IF NOT EXISTS"
            f" {column} {_WATERMARK_TYPE}"
        )
        self.execute(
            f"UPDATE {relation} SET {column} ="
            f" {_string_literal(watermark_cfg.transaction_id)} WHERE TRUE"
        )

    def quote_table(self, table_fqn: str) -> str:
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._impl import quote_table

        return quote_table(table_fqn)

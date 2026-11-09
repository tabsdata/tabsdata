#
# Copyright 2026 Tabsdata Inc.
#

"""Evolving a BigQuery target with ``ALTER`` statements.

BigQuery has schema evolution of its own -- ``schema_update_options`` on the
load job -- but it only ever adds a column or relaxes one to nullable.
Diffing the target against the incoming batch instead reaches what that
cannot: a column whose type the batch has outgrown is widened, and a change
BigQuery could not make refuses the write rather than letting the load job
discover it.

A merge comes through here -- a ``MERGE`` takes no load-job options, so a
merge target is evolved here or not at all; the write asks for it with
``dest_cfg={"bigquery.schema_evolution_engine": "iceberg"}`` and
``schema_evolution="update"``. A bulk load evolves natively, and only its
strict check diffs here.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any

from tabsdatak.conn.bigquery import _iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.evolution import evolve_with_iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import SchemaEvolution

logger = logging.getLogger(__name__)


_DECIMAL = re.compile(
    r"^\s*(BIG)?(?:NUMERIC|DECIMAL)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$",
    re.IGNORECASE,
)


def _decimal_digits(type_: str) -> tuple[int, int] | None:
    """The integer digits and the scale a BigQuery decimal column holds."""
    match = _DECIMAL.match(type_)
    if not match:
        return None
    if match[2] is None:
        return (76 - 38, 38) if match[1] else (38 - 9, 9)
    precision, scale = int(match[2]), int(match[3] or 0)
    return precision - scale, scale


def narrowed(stored: list[tuple[str, str]], footer: Any) -> dict[str, str]:
    """The stored type of each decimal column narrower than the batch's type
    that still holds every value of the batch, keyed by column.

    polars decimals reach BigQuery as ``BIGNUMERIC`` past 29 integer digits,
    and a ``NUMERIC`` column cannot be loaded from one, nor assigned one,
    whatever its values. The batch's types and values are read off the staged
    file's Parquet footer, with no query: a column is taken when it keeps at
    most the column's fractional digits (more would be rounded away) and
    every value fits the column's integer digits.
    """
    import pyarrow as pa

    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._schema.footer import column_range

    held = {fold_identifier(name): type_ for name, type_ in stored}
    taken = {}
    for field in footer.schema.to_arrow_schema():
        if not pa.types.is_decimal(field.type):
            continue
        type_ = held.get(fold_identifier(field.name))
        digits = _decimal_digits(type_) if type_ else None
        if digits is None:
            continue
        integer, scale = digits
        batch = field.type
        if batch.scale > scale or batch.precision - batch.scale <= integer:
            continue
        values = column_range([footer], field.name)
        limit = Decimal(10) ** integer
        if values is not None and (
            values[0] is None or (-limit < values[0] and values[1] < limit)
        ):
            taken[field.name] = type_
    return taken


def _projection(columns: list[str], casts: dict[str, str]) -> str:
    quote_column = _iceberg.quote_column
    return ", ".join(
        f"CAST({quote_column(name)} AS {casts[name]}) AS {quote_column(name)}"
        if name in casts
        else quote_column(name)
        for name in columns
    )


def insert_cast(
    bq_client: Any,
    uri: str,
    target_fqn: str,
    columns: list[str],
    casts: dict[str, str],
) -> None:
    """Append the batch at ``uri`` to ``target_fqn`` with ``casts`` applied.

    The load job a bulk append otherwise runs cannot narrow a column, so this
    one append reads the staged file through a query-scoped external table and
    inserts it cast. That query is billed by the bytes it reads, where a load
    job is not, so it runs only when a column needs it.
    """
    from google.cloud import bigquery

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import DECIMAL_TARGET_TYPES, quote_table

    config = bigquery.ExternalConfig(bigquery.SourceFormat.PARQUET)
    config.source_uris = [uri]
    config.decimal_target_types = DECIMAL_TARGET_TYPES
    names = ", ".join(_iceberg.quote_column(name) for name in columns)
    logger.info("bigquery: appending onto %r with %s cast", target_fqn, casts)
    bq_client.query_and_wait(
        f"INSERT INTO {quote_table(target_fqn)} ({names}) "
        f"SELECT {_projection(columns, casts)} FROM _td_batch",
        job_config=bigquery.QueryJobConfig(table_definitions={"_td_batch": config}),
    )


def narrowed_target(
    bq_client: Any, store: Any, key: str, target_fqn: str
) -> dict[str, str]:
    """:func:`narrowed` against ``target_fqn`` for the batch staged at ``key``,
    reading only the staged file's footer; nothing when the target does not
    exist yet, as the write then creates it from the batch."""
    import pyarrow.parquet as pq
    from google.cloud.exceptions import NotFound

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import table_column_types

    try:
        stored = table_column_types(bq_client, target_fqn)
    except NotFound:
        return {}
    source = store.ranged(key)
    try:
        footer = pq.read_metadata(source)
    finally:
        source.close()
    return narrowed(stored, footer)


def cast_in_place(
    bq_client: Any, relation: str, columns: list[str], casts: dict[str, str]
) -> None:
    """Rewrite the staging ``relation`` with ``casts`` applied, so a ``MERGE``
    can assign it: BigQuery assigns no ``BIGNUMERIC`` to a ``NUMERIC``."""
    logger.info("bigquery: casting %s in %s", casts, relation)
    bq_client.query_and_wait(
        f"CREATE OR REPLACE TABLE {relation} AS "
        f"SELECT {_projection(columns, casts)} FROM {relation}"
    )


def fold_identifier(name: str) -> str:
    """BigQuery resolves a column name case-insensitively and refuses a table
    two columns differing only in case, so lower case is the form two names are
    the same in. Backticks quote a name; they do not make it case-sensitive."""
    return name.lower()


# A trx id is an opaque string, and STRING is what the SchemaField walk reports
# back for the column, so a target that already has it does not read as a
# retype of it.
WATERMARK_TYPE = "STRING"


def string_literal(value: str) -> str:
    """``value`` as a GoogleSQL string literal.

    The trx id is a system-generated token, but it is the one value here that
    does not come from the connector itself, and a statement built by hand has
    nowhere to bind it.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _existing_columns(bq_client: Any, target_fqn: str) -> set[str] | None:
    """``target_fqn``'s folded column names, or ``None`` when it is not there.

    A missing target is not an error here: every watermark pass runs before
    anything has been written to it, and "no table" and "no such column" lead to
    the same answer.
    """
    from google.cloud.exceptions import NotFound

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import table_column_types

    try:
        return {
            fold_identifier(name)
            for name, _ in table_column_types(bq_client, target_fqn)
        }
    except NotFound:
        return None


def check_watermark_column(
    bq_client: Any, target_fqn: str, column: str, transaction_id: str
) -> bool:
    """Whether ``target_fqn`` already holds a row stamped with ``transaction_id``.

    The target's own schema answers the first half, so a target that does not
    exist yet, or one that predates the watermark column, reports unstamped
    without being queried -- selecting a column the table has not got is an
    error rather than an answer.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import quote_table

    columns = _existing_columns(bq_client, target_fqn)
    if columns is None or fold_identifier(column) not in columns:
        return False
    rows = bq_client.query_and_wait(
        f"SELECT COUNT(*) > 0 FROM {quote_table(target_fqn)}"
        f" WHERE {_iceberg.quote_column(column)}"
        f" = {string_literal(transaction_id)}"
    )
    return any(row[0] for row in rows)


def ensure_watermark_column(bq_client: Any, target_fqn: str, column: str) -> bool:
    """``ALTER`` the watermark column onto ``target_fqn`` unless it is there
    already, reporting whether anything was added.

    A target that does not exist yet is left alone: whatever creates it -- the
    load job for a bulk write, ``ensure_target`` for a merge -- builds it from a
    batch already carrying the column.

    The rows already in the target were written by earlier transactions and are
    left alone, so they keep the NULL a new column starts with. That is what
    "written before there was a watermark" looks like; stamping them with this
    trx's id would claim them for it.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import quote_table

    columns = _existing_columns(bq_client, target_fqn)
    if columns is None or fold_identifier(column) in columns:
        return False
    logger.debug("bigquery: adding watermark column %r to %r", column, target_fqn)
    bq_client.query_and_wait(
        f"ALTER TABLE {quote_table(target_fqn)} ADD COLUMN IF NOT EXISTS"
        f" {_iceberg.quote_column(column)} {WATERMARK_TYPE}"
    )
    return True


def batch_columns(bq_client: Any, uri: str, target_fqn: str) -> list[tuple[str, str]]:
    """The ``(name, type)`` pairs the batch at ``uri`` carries.

    Read off an external table over the staged file rather than by walking the
    parquet footer: the types then are the ones BigQuery itself settles on for
    this file, which is what the target's own schema will be compared against.
    A mapping written here could disagree with the load job about a decimal's
    precision or an int's width, and the diff would act on the difference. An
    external table reads the file's metadata only, so the batch is not loaded
    a second time just to learn its types; it reports the types a load does.

    The table is dropped on the way out. The GCS object is not -- the plugin
    deletes every URI a write staged, and the real load still has to read it.
    """
    from google.cloud import bigquery

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import (
        DECIMAL_TARGET_TYPES,
        table_column_types,
        temp_table_fqn,
    )

    external = temp_table_fqn(target_fqn)
    logger.debug(
        "bigquery: reading the batch schema for %r through %r",
        target_fqn,
        external,
    )
    config = bigquery.ExternalConfig(bigquery.SourceFormat.PARQUET)
    config.source_uris = [uri]
    config.decimal_target_types = DECIMAL_TARGET_TYPES
    table = bigquery.Table(external)
    table.external_data_configuration = config
    bq_client.create_table(table)
    try:
        return table_column_types(bq_client, external)
    finally:
        bq_client.delete_table(external, not_found_ok=True)


class BigQuerySchemaEvolver:
    """The three operations the Iceberg evolution pass makes against BigQuery.

    Satisfies
    :class:`~tabsdatak.conn.common.sql._schema.evolution.SchemaEvolutionTarget` over a
    client and one target, which is what lets a bulk load evolve its target
    without a merge dialect to borrow one from. The merge dialect implements the
    same three methods itself -- they are part of its own contract.
    """

    def __init__(self, bq_client: Any, target_fqn: str) -> None:
        self._bq_client = bq_client
        self._target_fqn = target_fqn

    def fold(self, name: str) -> str:
        return fold_identifier(name)

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._impl import table_column_types

        return table_column_types(self._bq_client, self._target_fqn)

    def execute(self, statement: str) -> None:
        logger.debug("bigquery: executing %s", statement)
        self._bq_client.query_and_wait(statement)

    def evolve(
        self, columns: list[tuple[str, str]], evolution: SchemaEvolution
    ) -> list[str]:
        """``ALTER`` the target to meet ``columns``, returning what changed.

        A target that does not exist yet is left alone: the load job creates it
        from the batch, so there is nothing to reconcile and nothing to read.

        No ``align_type``: both sides are read by the same walk over a
        ``SchemaField``, so a column neither side touched reports as unchanged
        and there is no vocabulary to fold away first. That is also what makes
        ``strict`` safe -- the retypes the diff reports are real ones.
        """
        # noinspection PyProtectedMember
        from google.cloud.exceptions import NotFound

        from tabsdatak.conn.bigquery._impl import quote_table

        try:
            self._bq_client.get_table(self._target_fqn)
        except NotFound:
            logger.debug(
                "bigquery: %r does not exist yet; the load creates it",
                self._target_fqn,
            )
            return []

        return evolve_with_iceberg(
            self,
            quote_table(self._target_fqn),
            columns,
            renderer=_iceberg.BigQueryRenderer(),
            widening_map=_iceberg.BIGQUERY_TYPE_WIDENING_MAP,
            can_safely_widen=_iceberg.can_widen,
            same_base=_iceberg.same_base,
            evolution=evolution,
        )

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

A merge always comes through here -- a ``MERGE`` takes no load-job options,
so a merge target is evolved here or not at all. A bulk load comes through
only when the write asks for it with
``dest_cfg={"bigquery.schema_evolution_engine": "iceberg"}``.
"""

from __future__ import annotations

import logging
from typing import Any

from tabsdatak.conn.bigquery import _iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._evolution import evolve_with_iceberg

logger = logging.getLogger(__name__)


def fold_identifier(name: str) -> str:
    """BigQuery resolves a column name case-insensitively and refuses a table
    two columns differing only in case, so lower case is the form two names are
    the same in. Backticks quote a name; they do not make it case-sensitive."""
    return name.lower()


def batch_columns(bq_client: Any, uri: str, target_fqn: str) -> list[tuple[str, str]]:
    """The ``(name, type)`` pairs the batch at ``uri`` carries.

    Read by loading it into a table of its own and describing that, rather than
    by walking the parquet footer: the types then are the ones BigQuery itself
    settled on for this file, which is what the target's own schema will be
    compared against. A mapping written here could disagree with the load job
    about a decimal's precision or an int's width, and the diff would act on the
    difference.

    The table is dropped on the way out. The GCS object is not -- the plugin
    deletes every URI a write staged, and the real load still has to read it.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._impl import (
        build_merge_staging_load_job_config,
        table_column_types,
        temp_table_fqn,
    )

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery._plugin import _load_table_from_uri

    staging_table = temp_table_fqn(target_fqn)
    logger.debug(
        "bigquery: reading the batch schema for %r out of %r",
        target_fqn,
        staging_table,
    )
    _load_table_from_uri(
        bq_client,
        uri=uri,
        table=staging_table,
        job_config=build_merge_staging_load_job_config(),
    )
    try:
        return table_column_types(bq_client, staging_table)
    finally:
        bq_client.delete_table(staging_table, not_found_ok=True)


class BigQuerySchemaEvolver:
    """The three operations the Iceberg evolution pass makes against BigQuery.

    Satisfies
    :class:`~tabsdatak.conn.common.sql._evolution.SchemaEvolutionTarget` over a
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

    def evolve(self, columns: list[tuple[str, str]]) -> list[str]:
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
            strict=True,
        )

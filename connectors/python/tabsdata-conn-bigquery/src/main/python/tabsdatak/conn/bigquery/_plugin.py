#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import polars as pl

from tabsdatak.conn.bigquery import (
    BigQueryDest,
    BigQueryDestConn,
    DestCfgKey,
    DestCfgSpec,
)
from tabsdatak.conn.bigquery._explorer import BigQueryConnectionExplorer
from tabsdatak.conn.bigquery.error import BigQueryErrorCode
from tabsdatak.conn.common.error import ConnException

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import cloud_full_path
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
)

# All ``google-cloud-*`` imports stay inside the plugin method bodies
# below so that the conn-bigquery package is cheap to import in
# registry scans where the plugin is not actually invoked.

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "bigquery.logging_level"
_KEY_DEST_SCHEMA_EVOLUTION_ENGINE: DestCfgKey = "bigquery.schema_evolution_engine"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
    _KEY_DEST_SCHEMA_EVOLUTION_ENGINE: "iceberg",
}

_SCHEMA_EVOLUTION_ENGINES = ("native", "iceberg")


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int
    schema_evolution_engine: str


def _resolve_dest_cfg(dest_cfg: DestCfgSpec | None) -> _WriterCfg:
    cfg = dest_cfg or {}
    for k in cfg:
        if k not in _DEST_CFGS_DEFAULTS:
            logger.warning(
                f"Unknown dest_cfg key {k!r}, ignoring (allowed: "
                f"{sorted(_DEST_CFGS_DEFAULTS)})"
            )

    def _get(key: DestCfgKey) -> Any:
        return cfg.get(key, _DEST_CFGS_DEFAULTS[key])

    engine = _get(_KEY_DEST_SCHEMA_EVOLUTION_ENGINE)
    if engine not in _SCHEMA_EVOLUTION_ENGINES:
        # A typo here would otherwise fall back to the default engine, with
        # nothing said about it.
        raise BigQueryErrorCode.BIGQUERY_21.exception(
            value=engine,
            allowed=" or ".join(repr(e) for e in _SCHEMA_EVOLUTION_ENGINES),
        )

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
        schema_evolution_engine=engine,
    )


class _BigQueryDestPlugin(
    DestPlugin[BigQueryDestConn, BigQueryDest, list[pl.LazyFrame]],
):
    def write_out(
        self,
        ctx: DestContext,
        conn: BigQueryDestConn,
        dest: BigQueryDest,
        tables: list[pl.LazyFrame],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._evolution import (
            BigQuerySchemaEvolver,
            batch_columns,
            check_watermark_column,
            ensure_watermark_column,
            insert_cast,
            narrowed_target,
        )
        from tabsdatak.conn.bigquery._impl import (
            BigQueryWriteCfg,
            build_bigquery_client,
            build_load_job_config,
            discard,
            quote_table,
            stage,
            staging_store,
        )

        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._merge import BigQueryMergeDialect

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._merge._driver import merge_batch

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._watermark import watermarked_batch

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "bigquery: write_out starting (%d table(s))", len(dest.tables or [])
        )

        write_cfg = BigQueryWriteCfg.from_dest(conn, dest)

        # Defensive: framework should have already lined ``dest.tables``
        # up with ``tables`` -- when ``tables is None`` the legacy
        # plugin pulled table names from the upstream input plugin, but
        # the new SPI does not surface that. The cardinality lambda on
        # the DestDef ensures this matches in normal flows; we still
        # double-check at the plugin boundary.
        if len(write_cfg.target_tables) != len(tables):
            raise BigQueryErrorCode.BIGQUERY_4.exception(
                tables=len(write_cfg.target_tables),
                slots=len(tables),
            )

        project_str = conn._project()

        logger.debug("bigquery: establishing BigQuery client (project=%r)", project_str)
        try:
            bq_client = build_bigquery_client(
                conn.credentials,
                project_str,
                conn.conn_cfg,
            )
        except Exception as e:
            raise BigQueryErrorCode.BIGQUERY_5.exception(
                cause=e,
                project=project_str,
            )

        store = staging_store(conn.staging)
        staging_keys = [
            cloud_full_path(
                conn.staging._base_path(),
                f"auxiliary_{index}_{uuid.uuid4().hex[:16]}.parquet",
            )
            for index in range(len(tables))
        ]
        staging_uris = [store.uri(key) for key in staging_keys]
        # URIs a rerun guard declined to stage, so the load loop skips them.
        skipped: set[str] = set()

        total = len(write_cfg.target_tables)
        try:
            if write_cfg.if_table_exists == "merge":
                # One cfg per table when merging (``validate`` guarantees it),
                # so the merge config travels with its table rather than being
                # indexed out by the 1-based progress counter.
                merge_cfgs = write_cfg.merge_cfgs
                assert merge_cfgs is not None
                # A merge stages, creates, evolves and writes one table at a
                # time: the driver owns that sequence, and everything BigQuery
                # contributes to it lives in the dialect.
                for i, (frame, key, table_fqn, merge_cfg) in enumerate(
                    zip(
                        tables,
                        staging_keys,
                        write_cfg.target_tables,
                        merge_cfgs,
                    ),
                    1,
                ):
                    logger.info(
                        "bigquery: merging table %d/%d: %r", i, total, table_fqn
                    )
                    try:
                        merge_batch(
                            BigQueryMergeDialect(
                                bq_client,
                                store,
                                key=key,
                                target_fqn=table_fqn,
                            ),
                            target=quote_table(table_fqn),
                            batch=frame,
                            cfg=merge_cfg._sql_merge_cfg(),
                            schema_evolution=write_cfg.schema_evolution,
                            ctx=ctx,
                            watermark_column=write_cfg.watermark_column,
                        )
                    except ConnException:
                        raise
                    except Exception as e:
                        raise BigQueryErrorCode.BIGQUERY_18.exception(
                            cause=e,
                            table=table_fqn,
                        )
                    logger.debug("bigquery: finished table %r", table_fqn)
                return

            # A strict append whatever the engine: the load job would refuse the
            # same batch, but with its own error rather than SQL_COMMON_10. A
            # replace's load job takes the batch's schema, so there is nothing
            # for strict to refuse. "update" only under iceberg: the native
            # engine's load job evolves itself.
            strict_append = (
                write_cfg.schema_evolution == "strict"
                and write_cfg.if_table_exists == "append"
            )
            diffing = strict_append or (
                write_cfg.schema_evolution == "update"
                and cfg.schema_evolution_engine == "iceberg"
            )

            # Append is the only disposition that stamps: a merge has returned
            # above, and a replace is refused the column by
            # BigQueryDest.validate. Named again here rather than inferred from
            # those two, so widening the API later cannot quietly start
            # stamping a disposition that has no business being stamped.
            watermark = (
                write_cfg.watermark_column
                if write_cfg.if_table_exists == "append"
                else None
            )

            # Phase 1: stage each table as parquet at its GCS URI.
            logger.info("bigquery: staging %d table(s) to GCS", total)
            for i, (frame, key, uri, table_fqn) in enumerate(
                zip(tables, staging_keys, staging_uris, write_cfg.target_tables), 1
            ):
                if watermark is not None and check_watermark_column(
                    bq_client, table_fqn, watermark, ctx.transaction_id
                ):
                    # Asked before the batch is staged: a rerun that has
                    # nothing to do should not pay to read and ship rows it
                    # will not load.
                    logger.info(
                        "bigquery: skipping append onto %r: watermark column %s"
                        " already has value %s",
                        table_fqn,
                        watermark,
                        ctx.transaction_id,
                    )
                    skipped.add(uri)
                    continue
                logger.debug("bigquery: staging %d/%d to %s", i, total, uri)
                try:
                    stage(
                        store,
                        key,
                        watermarked_batch(frame, watermark, ctx.transaction_id),
                    )
                except Exception as e:
                    raise BigQueryErrorCode.BIGQUERY_6.exception(
                        cause=e,
                        uri=uri,
                    )

            # Phase 2: kick off one ``load_table_from_uri`` per table
            # and wait for each load job to complete.
            for i, (table_fqn, key, uri) in enumerate(
                zip(write_cfg.target_tables, staging_keys, staging_uris), 1
            ):
                if uri in skipped:
                    continue
                logger.info(
                    "bigquery: writing table %d/%d: %r",
                    i,
                    total,
                    table_fqn,
                )
                if watermark is not None:
                    # Strict is what makes this necessary: it adds nothing, and
                    # a load job handed a batch carrying a column the target
                    # has not got fails outright rather than dropping it. The
                    # same exception the merge carves out for its own watermark.
                    #
                    # Ahead of the evolution pass so that under "update" the
                    # column is already there when the diff runs, leaving one
                    # place that adds it rather than two engines each having a
                    # claim.
                    ensure_watermark_column(bq_client, table_fqn, watermark)
                casts: dict[str, str] = {}
                columns: list[tuple[str, str]] = []
                if diffing:
                    # Before the load, so the rows land in a target already
                    # shaped to hold them -- and before a replace truncates the
                    # table it is about to refill.
                    try:
                        columns = batch_columns(bq_client, uri, table_fqn)
                        if strict_append:
                            casts = narrowed_target(bq_client, store, key, table_fqn)
                            columns = [(n, casts.get(n, t)) for n, t in columns]
                        BigQuerySchemaEvolver(bq_client, table_fqn).evolve(
                            columns, write_cfg.schema_evolution
                        )
                    except ConnException:
                        raise
                    except Exception as e:
                        raise BigQueryErrorCode.BIGQUERY_23.exception(
                            cause=e, table=table_fqn
                        )
                if casts:
                    try:
                        insert_cast(
                            bq_client,
                            uri,
                            table_fqn,
                            [name for name, _ in columns],
                            casts,
                        )
                    except Exception as e:
                        raise BigQueryErrorCode.BIGQUERY_7.exception(
                            cause=e,
                            table=table_fqn,
                        )
                    logger.debug("bigquery: finished table %r", table_fqn)
                    continue
                job_config = build_load_job_config(
                    write_cfg.if_table_exists,
                    write_cfg.schema_evolution,
                    cfg.schema_evolution_engine,
                )
                try:
                    _load_table_from_uri(
                        bq_client,
                        uri=uri,
                        table=table_fqn,
                        job_config=job_config,
                    )
                except Exception as e:
                    raise BigQueryErrorCode.BIGQUERY_7.exception(
                        cause=e,
                        table=table_fqn,
                    )
                logger.debug("bigquery: finished table %r", table_fqn)
        finally:
            # Best effort: a staged file that cannot be deleted is logged and
            # never fails the write, and one a rerun guard skipped or a failed
            # sink left unwritten is simply absent.
            discard(store, staging_keys)


def _load_table_from_uri(
    bq_client: Any,
    *,
    uri: str,
    table: str,
    job_config: Any,
) -> None:
    load_job = bq_client.load_table_from_uri(uri, table, job_config=job_config)
    load_job.result()


BIGQUERY_DEST = DestDef(
    conn=BigQueryDestConn,
    type_="bigquery-bulk-out",
    system="Google BigQuery",
    dest_version="v1",
    dest=BigQueryDest,
    cardinality=lambda dest: len(dest.tables) if dest.tables is not None else 0,
    plugin=_BigQueryDestPlugin,
    explorer=BigQueryConnectionExplorer,
    icon="conn/bigquery/resources/bigquery.svg",
)

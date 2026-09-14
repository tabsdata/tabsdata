#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    TableFileSpec,
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
    _KEY_DEST_SCHEMA_EVOLUTION_ENGINE: "native",
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
        # A typo here would otherwise fall back to native evolution and lose
        # the widening the write asked for, with nothing said about it.
        raise BigQueryErrorCode.BIGQUERY_21.exception(
            value=engine,
            allowed=" or ".join(repr(e) for e in _SCHEMA_EVOLUTION_ENGINES),
        )

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
        schema_evolution_engine=engine,
    )


class _BigQueryDestPlugin(DestPlugin[BigQueryDestConn, BigQueryDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: BigQueryDestConn,
        dest: BigQueryDest,
        tables: list[TableFileSpec],
    ) -> None:
        # noinspection PyProtectedMember
        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._evolution import (
            BigQuerySchemaEvolver,
            batch_columns,
            check_watermark_column,
            ensure_watermark_column,
        )
        from tabsdatak.conn.bigquery._impl import (
            BigQueryWriteCfg,
            build_bigquery_client,
            build_gcs_client,
            build_load_job_config,
            quote_table,
        )

        # noinspection PyProtectedMember
        from tabsdatak.conn.bigquery._merge_dialect import BigQueryMergeDialect

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

        project_str = conn._project() if conn.project is not None else None

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

        logger.debug("bigquery: establishing GCS staging client")
        gcs_client = build_gcs_client(
            conn.staging.credentials,
            conn.staging.cfg,
        )

        staging_bucket = conn.staging._bucket()

        staging_uris: list[str] = []
        # URIs a rerun guard declined to stage, so neither the load loop nor
        # the cleanup goes looking for them.
        skipped: set[str] = set()
        for index in range(len(tables)):
            file_name = (f"auxiliary_{index}_{uuid.uuid4().hex[:16]}.parquet").replace(
                "-", "_"
            )
            relative = cloud_full_path(conn.staging._base_path(), file_name)
            staging_uris.append(f"gs://{staging_bucket}/{relative}")

        total = len(write_cfg.target_tables)
        merging = write_cfg.if_table_exists == "merge"
        # One cfg per table when merging (``validate`` guarantees it),
        # so the merge config travels with its table rather than
        # being indexed out by the 1-based progress counter.
        merge_cfgs = write_cfg.merge_cfgs or [None] * total
        try:
            if merging:
                # A merge stages, creates, evolves and writes one table at a
                # time: the driver owns that sequence, and everything BigQuery
                # contributes to it lives in the dialect.
                for i, (parquet_path, uri, table_fqn, merge_cfg) in enumerate(
                    zip(
                        tables,
                        staging_uris,
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
                                gcs_client,
                                uri=uri,
                                target_fqn=table_fqn,
                            ),
                            target=quote_table(table_fqn),
                            batch=Path(parquet_path),
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

            evolving_with_iceberg = (
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

            # Phase 1: upload each parquet to its staging GCS URI.
            logger.info("bigquery: staging %d parquet file(s) to GCS", total)
            for i, (parquet_path, uri, table_fqn) in enumerate(
                zip(tables, staging_uris, write_cfg.target_tables), 1
            ):
                if watermark is not None and check_watermark_column(
                    bq_client, table_fqn, watermark, ctx.trx_id
                ):
                    # Asked before the batch is rewritten and uploaded: a rerun
                    # that has nothing to do should not pay to build and ship a
                    # file it will not load.
                    logger.info(
                        "bigquery: skipping append onto %r: watermark column %s"
                        " already has value %s",
                        table_fqn,
                        watermark,
                        ctx.trx_id,
                    )
                    skipped.add(uri)
                    continue
                logger.debug("bigquery: staging %d/%d to %s", i, total, uri)
                try:
                    with watermarked_batch(
                        Path(parquet_path), watermark, ctx.trx_id
                    ) as batch:
                        _upload_to_gcs(gcs_client, batch, uri)
                except Exception as e:
                    raise BigQueryErrorCode.BIGQUERY_6.exception(
                        cause=e,
                        uri=uri,
                    )

            # Phase 2: kick off one ``load_table_from_uri`` per table
            # and wait for each load job to complete.
            for i, (table_fqn, uri) in enumerate(
                zip(write_cfg.target_tables, staging_uris), 1
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
                if evolving_with_iceberg:
                    # Before the load, so the rows land in a target already
                    # shaped to hold them -- and before a replace truncates the
                    # table it is about to refill.
                    try:
                        BigQuerySchemaEvolver(bq_client, table_fqn).evolve(
                            batch_columns(bq_client, uri, table_fqn)
                        )
                    except ConnException:
                        raise
                    except Exception as e:
                        raise BigQueryErrorCode.BIGQUERY_23.exception(
                            cause=e, table=table_fqn
                        )
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
            # Best-effort cleanup of staging files (mirrors the
            # legacy behaviour). A failed deletion is logged at WARN
            # level but never propagates -- the user's load already
            # succeeded / failed; the auxiliary file's fate is
            # secondary.
            for uri in staging_uris:
                # A URI a rerun guard skipped was never uploaded, so deleting
                # it would warn about a file that is absent by design.
                if uri in skipped:
                    continue
                logger.debug("bigquery: deleting staged file %s", uri)
                try:
                    _delete_gcs_uri(gcs_client, uri)
                except Exception:  # pragma: no cover - defensive
                    logger.warning(
                        "Failed to delete auxiliary file %r from GCS",
                        uri,
                    )


def _upload_to_gcs(gcs_client: Any, local_path: Path, gs_uri: str) -> None:
    bucket_name, blob_name = _parse_gs_uri(gs_uri)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))


def _delete_gcs_uri(gcs_client: Any, gs_uri: str) -> None:
    bucket_name, blob_name = _parse_gs_uri(gs_uri)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.lower().startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {gs_uri!r}")
    rest = gs_uri[len("gs://") :]
    bucket, _, blob = rest.partition("/")
    return bucket, blob


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

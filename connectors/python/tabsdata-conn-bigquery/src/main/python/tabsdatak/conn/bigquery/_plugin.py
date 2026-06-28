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
from tabsdatak.conn.bigquery.error import BigqueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)

# All ``google-cloud-*`` imports stay inside the plugin method bodies
# below so that the conn-bigquery package is cheap to import in
# registry scans where the plugin is not actually invoked.

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "bigquery.logging_level"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int


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

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
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
        from tabsdatak.conn.bigquery._impl import (
            BigQueryWriteCfg,
            build_bigquery_client,
            build_gcs_client,
            build_load_job_config,
        )

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
            raise BigqueryErrorCode.BIGQUERY_4.exception(
                tables=len(write_cfg.target_tables),
                slots=len(tables),
            )

        project_str = _convert(conn.project, str) if conn.project is not None else None

        logger.debug("bigquery: establishing BigQuery client (project=%r)", project_str)
        try:
            bq_client = build_bigquery_client(
                conn.credentials,
                project_str,
                conn.conn_cfg,
            )
        except Exception as e:
            raise BigqueryErrorCode.BIGQUERY_5.exception(
                cause=e,
                project=project_str,
            )

        logger.debug("bigquery: establishing GCS staging client")
        gcs_client = build_gcs_client(
            conn.staging.credentials,
            conn.staging.cfg,
        )

        staging_bucket = _convert(conn.staging.bucket, str)

        staging_uris: list[str] = []
        for index in range(len(tables)):
            file_name = (f"auxiliary_{index}_{uuid.uuid4().hex[:16]}.parquet").replace(
                "-", "_"
            )
            relative = conn.staging.full_path(file_name)
            staging_uris.append(f"gs://{staging_bucket}/{relative}")

        total = len(write_cfg.target_tables)
        try:
            # Phase 1: upload each parquet to its staging GCS URI.
            logger.info("bigquery: staging %d parquet file(s) to GCS", total)
            for i, (parquet_path, uri) in enumerate(zip(tables, staging_uris), 1):
                logger.debug("bigquery: staging %d/%d to %s", i, total, uri)
                try:
                    _upload_to_gcs(gcs_client, Path(parquet_path), uri)
                except Exception as e:
                    raise BigqueryErrorCode.BIGQUERY_6.exception(
                        cause=e,
                        uri=uri,
                    )

            # Phase 2: kick off one ``load_table_from_uri`` per table
            # and wait for each load job to complete.
            for i, (table_fqn, uri) in enumerate(
                zip(
                    write_cfg.target_tables,
                    staging_uris,
                ),
                1,
            ):
                logger.info(
                    "bigquery: writing table %d/%d: %r",
                    i,
                    total,
                    table_fqn,
                )
                try:
                    _load_table_from_uri(
                        bq_client,
                        uri=uri,
                        table=table_fqn,
                        job_config=build_load_job_config(
                            write_cfg.if_table_exists,
                            write_cfg.schema_evolution,
                        ),
                    )
                except Exception as e:
                    raise BigqueryErrorCode.BIGQUERY_7.exception(
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


class _BigQueryExplorer(Explorer[BigQueryDestConn]):
    def metadata(self, conn: BigQueryDestConn) -> Any:
        # TODO
        pass

    def data(self, conn: BigQueryDestConn, query: str) -> Any:
        # TODO
        pass


BIGQUERY_DEST = DestDef(
    conn=BigQueryDestConn,
    dest_type="bigquery",
    dest_version="v1",
    dest=BigQueryDest,
    cardinality=lambda dest: len(dest.tables) if dest.tables is not None else 0,
    plugin=_BigQueryDestPlugin,
    explorer=_BigQueryExplorer,
)

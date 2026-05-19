from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.druid import (
    DestCfgKey,
    DestCfgSpec,
    DruidConn,
    DruidDest,
)
from tabsdatak.conn.druid.error import DruidErrorCode
from tabsdatak.error import TabsdataException
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "druid.logging_level"
_KEY_DEST_POLL_INTERVAL_S: DestCfgKey = "druid.poll_interval_seconds"
_KEY_DEST_RETRY_ATTEMPTS: DestCfgKey = "druid.retry_attempts"
_KEY_DEST_RETRY_BACKOFF_S: DestCfgKey = "druid.retry_backoff_seconds"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
    _KEY_DEST_POLL_INTERVAL_S: None,
    _KEY_DEST_RETRY_ATTEMPTS: None,
    _KEY_DEST_RETRY_BACKOFF_S: None,
}


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int
    poll_interval_seconds: float | None
    retry_attempts: int | None
    retry_backoff_seconds: float | None


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

    poll = _get(_KEY_DEST_POLL_INTERVAL_S)
    retry_a = _get(_KEY_DEST_RETRY_ATTEMPTS)
    retry_b = _get(_KEY_DEST_RETRY_BACKOFF_S)
    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
        poll_interval_seconds=float(poll) if poll is not None else None,
        retry_attempts=int(retry_a) if retry_a is not None else None,
        retry_backoff_seconds=float(retry_b) if retry_b is not None else None,
    )


class DruidDestPlugin(DestPlugin[DruidDest]):

    def write_out(
        self,
        ctx: DestContext,
        dest: DruidDest,
        tables: list[TableFileSpec],
    ) -> None:
        # All adapter imports live inside the method body so the
        # tabsdata-conn-druid package stays cheap to import for a
        # registry scan that never invokes ``write_out``.
        # noinspection PyProtectedMember
        from tabsdatak.conn.druid import _impl as adapter

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)

        # ---- Defensive recheck ----------------------------------
        # The framework already validates ``cardinality`` upstream; the
        # explicit check here pins the contract for unit tests and
        # keeps the failure mode local rather than letting downstream
        # mis-indexing produce a cryptic error.
        if len(dest.tables) != len(tables):
            # noinspection PyProtectedMember
            raise DruidErrorCode.DRUID_6.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        conn = cast(DruidConn, dest.conn)
        staging = conn.staging

        # ---- Stage parquet to the cloud --------------------------
        # All slots stage under a single ``td-druid/<trx>/<rand>``
        # subdir so each transaction is isolated from others. The
        # per-table folder partitions the staged files inside that
        # subdir so the Druid ingestion spec for each datasource sees
        # only the files for that datasource.
        # noinspection PyProtectedMember
        subdir_root = adapter._staging_subdir(ctx.trx_id)

        # ---- Submit one ingestion task per datasource ------------
        client = adapter.make_overlord_client_for_dest(conn, dest)
        with client:
            for slot_idx, (datasource, parquet) in enumerate(
                zip(dest.tables, tables)
            ):
                slot_subdir = f"{subdir_root}/slot{slot_idx}"
                try:
                    staged_uris = adapter.upload_staged_files(
                        staging=staging,
                        parquet_files=[parquet],
                        subdir=slot_subdir,
                        work_dir=ctx.work_dir(),
                    )
                except (
                    _transporter.TransporterRunError,
                    _transporter.TransporterReportError,
                    _transporter.TransporterBinaryNotFoundError,
                ) as e:
                    # noinspection PyProtectedMember
                    raise DruidErrorCode.DRUID_7.exception(
                        cause=e,
                        datasource=datasource,
                    )

                # Build the per-task ingestion spec.
                input_source = adapter.build_input_source(
                    staging, staged_uris
                )
                spec = adapter.build_ingestion_spec(
                    datasource=datasource,
                    input_source=input_source,
                    is_replace=(dest.if_table_exists == "replace"),
                    time_column=dest.time_column,
                    segment_granularity=dest.segment_granularity,
                    max_concurrent_subtasks=dest.max_concurrent_subtasks,
                    max_rows_per_segment=dest.max_rows_per_segment,
                    max_rows_in_memory=dest.max_rows_in_memory,
                )

                # Submit + wait.
                try:
                    task_id = client.submit_task(spec)
                except TabsdataException:
                    # ``submit_task`` already raised DRUID_10 with the
                    # safe-URL context baked in; let it propagate.
                    raise
                except Exception as e:
                    # noinspection PyProtectedMember
                    raise DruidErrorCode.DRUID_8.exception(
                        cause=e,
                        datasource=datasource,
                        # noinspection PyProtectedMember
                        url=adapter._sanitize_url(
                            adapter._resolve(conn.overlord_url),
                        ),
                    )

                logger.info(
                    "Druid ingestion task submitted "
                    "(datasource=%s, task_id=%s)",
                    datasource,
                    task_id,
                )
                try:
                    client.wait_for_completion(task_id)
                except TabsdataException as e:
                    if e.api_error is DruidErrorCode.DRUID_9:
                        # ``wait_for_completion`` doesn't know which
                        # datasource produced the failure; rebuild
                        # the exception with the right context.
                        reason = e.extra.get("reason", "ingestion failed")
                        # noinspection PyProtectedMember
                        raise DruidErrorCode.DRUID_9.exception(
                            datasource=datasource,
                            task_id=task_id,
                            reason=reason,
                        ) from e
                    raise


class DruidExplorer(Explorer[DruidConn]):
    def metadata(self, conn: DruidConn) -> Any:
        # TODO
        pass

    def data(self, conn: DruidConn, query: str) -> Any:
        # TODO
        pass


DRUID_DEST = DestDef(
    conn=DruidConn,
    dest_type="druid",
    dest_version="v1",
    dest=DruidDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=DruidDestPlugin,
    explorer=DruidExplorer,
)

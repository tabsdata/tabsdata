#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import polars as pl

# noinspection PyProtectedMember
from tabsdatak.conn.common._dest import present
from tabsdatak.conn.druid import (
    DestCfgKey,
    DestCfgSpec,
    DruidDest,
    DruidDestConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid._explorer import DruidConnectionExplorer
from tabsdatak.conn.druid.error import DruidErrorCode
from tabsdatak.error import TabsdataException
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
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


class DruidDestPlugin(
    DestPlugin[DruidDestConn, DruidDest, list[pl.LazyFrame | None]],
):
    """Apache Druid destination plugin.

    Druid ingestion is asynchronous: parquet files first land in a
    cloud staging area (S3 / Azure) and then the plugin POSTs a
    native batch ``index_parallel`` task to the Overlord. The plugin
    polls the Overlord until the task terminates and returns ``None``
    on success or raises ``DRUID_*`` on any failure. Once the task has
    ended, successfully or not, its staged file is deleted, best effort.

    One ingestion task is submitted per target datasource with a table; a
    datasource whose table is ``None`` is left as it is. Per-task
    parallelism is governed by ``DruidDest.max_concurrent_subtasks``.
    """

    def write_out(
        self,
        ctx: DestContext,
        conn: DruidDestConn,
        dest: DruidDest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        # All adapter imports live inside the method body so the
        # tabsdata-conn-druid package stays cheap to import for a
        # registry scan that never invokes ``write_out``.
        # noinspection PyProtectedMember
        from tabsdatak.conn.druid import _impl as adapter

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("druid: write_out starting (%d table(s))", len(dest.tables or []))

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

        frames, datasources = present(tables, dest.tables)
        if not frames:
            logger.info("druid: no table to write; nothing to do")
            return

        staging = conn.staging

        # ---- Stage parquet to the cloud --------------------------
        # All slots stage under a single ``td-druid/<trx>/<rand>``
        # subdir so each transaction is isolated from others. The
        # per-table folder partitions the staged files inside that
        # subdir so the Druid ingestion spec for each datasource sees
        # only the files for that datasource.
        # noinspection PyProtectedMember
        subdir_root = adapter._staging_subdir(ctx.transaction_id)

        # ---- Submit one ingestion task per datasource ------------
        logger.debug("druid: establishing overlord client")
        client = adapter.make_overlord_client_for_dest(conn, dest)
        store = adapter.staging_store(staging)
        total = len(datasources)
        with client:
            for slot_idx, (datasource, frame) in enumerate(zip(datasources, frames)):
                logger.info(
                    "druid: writing datasource %d/%d: %r",
                    slot_idx + 1,
                    total,
                    datasource,
                )
                logger.info("druid: staging parquet for %r", datasource)
                try:
                    key, staged_uri = adapter.stage(
                        staging,
                        store,
                        frame,
                        f"{subdir_root}/slot{slot_idx}",
                        f"part-00000-{slot_idx}.parquet",
                    )
                except Exception as e:
                    # noinspection PyProtectedMember
                    raise DruidErrorCode.DRUID_7.exception(
                        cause=e,
                        datasource=datasource,
                    )
                try:
                    self._ingest(
                        adapter, client, conn, dest, staging, datasource, staged_uri
                    )
                except BaseException as error:
                    if (
                        isinstance(error, TabsdataException)
                        and error.api_error is DruidErrorCode.DRUID_9
                    ):
                        adapter.discard(store, [key])
                    else:
                        logger.warning(
                            "druid: the ingestion of %r may still read its "
                            "staged file; keeping %s",
                            datasource,
                            key,
                        )
                    raise
                adapter.discard(store, [key])

    @staticmethod
    def _ingest(
        adapter: Any,
        client: Any,
        conn: DruidDestConn,
        dest: DruidDest,
        staging: Any,
        datasource: str,
        staged_uri: str,
    ) -> None:
        input_source = adapter.build_input_source(staging, [staged_uri])
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
                    conn._overlord_url(),
                ),
            )

        logger.info(
            "Druid ingestion task submitted (datasource=%s, task_id=%s)",
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


DRUID_DEST = DestDef(
    conn=DruidDestConn,
    type_="druid-bulk-out",
    system="Apache Druid",
    dest_version="v1",
    dest=DruidDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=DruidDestPlugin,
    explorer=DruidConnectionExplorer,
    icon="conn/druid/resources/druid.svg",
)

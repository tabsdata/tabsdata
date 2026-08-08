#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast

from tabsdatak.common._fileformat.taxonomy import DestFileFormat, SrcFileFormat
from tabsdatak.common._transporter.facade import make_handler
from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
    TransporterReportError,
    TransporterRunError,
)
from tabsdatak.conn.common.constants import SRC_META_DROP_KEY
from tabsdatak.conn.common.file import _input, _output
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    EXEC_PLAN_ID,
    EXEC_PLAN_TS,
    TRX_ID,
)
from tabsdatak.error import ApiException, ErrorCode
from tabsdatak.spi import (
    DestContext,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
)

logger = logging.getLogger(__name__)

# --- Format / value helpers ----------------------------------------


def initial_lastmod_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def resolve_dest_tokens(target_path: str, ctx: DestContext) -> str:
    tokens = {
        EXEC_PLAN_ID: ctx.exec_plan_id,
        TRX_ID: ctx.trx_id,
        EXEC_PLAN_TS: ctx.exec_plan_ts,
    }
    resolved = target_path
    for token, value in tokens.items():
        resolved = resolved.replace(token, value)
    return resolved


# --- Reader / writer adapter conf --------------------------------


@dataclass(frozen=True)
class ReaderConf:
    # The Conn instance the orchestrator hands to ``build_location``
    # / ``run_env``.  Typed Any so this module stays decoupled from
    # the connector-specific Conn class. ``file_format`` is the raw
    # (possibly AUTO) format enum; the handler resolves AUTO against
    # ``paths`` and maps it to its own transporter format object.
    conn: Any
    paths: list[str]
    file_format: SrcFileFormat
    format_cfg: dict | None = None
    initial_lastmod: str | None = None
    drop_metadata: bool = False

    @classmethod
    def from_src(cls, conn: Any, src: Any) -> "ReaderConf":
        return cls(
            conn=conn,
            paths=list(src.paths),
            file_format=src.format,
            format_cfg=src.format_cfg,
            initial_lastmod=initial_lastmod_to_iso(src.initial_last_modified),
            drop_metadata=bool((src.src_cfg or {}).get(SRC_META_DROP_KEY, False)),
        )


@dataclass(frozen=True)
class WriterConf:
    conn: Any
    paths: list[str]
    file_format: DestFileFormat
    format_cfg: dict | None = None

    @classmethod
    def from_dest(cls, conn: Any, dest: Any) -> "WriterConf":
        return cls(
            conn=conn,
            paths=list(dest.paths),
            file_format=dest.format,
            format_cfg=dest.format_cfg,
        )


# --- Error-code bundle --------------------------------------------


@dataclass(frozen=True)
class FilePluginErrors:
    binary_not_found: ErrorCode
    read_failed: ErrorCode
    write_failed: ErrorCode
    report_invalid: ErrorCode
    cred_resolution: ErrorCode | None = None  # None when the connector has no creds


LocationBuilder = Callable[[Any, str, TransporterHandler], Any]
RunEnvBuilder = Callable[[Any, TransporterHandler], dict[str, str]]


# --- Orchestrators -------------------------------------------------


# Per-slot incremental cursor key. The transporter emits an opaque
# ``lastmod_info`` blob summarizing every file it observed in this
# slot; we stash it on the trx context so the next trx can resume
# where this one left off. Slots are addressed positionally
# (``slot0``, ``slot1``, ...) so a Src whose ``paths`` ordering is
# stable round-trips cleanly. The key shape (one alphanumeric label
# per dot-separated segment, no underscores) matches the context-key
# regex declared on `ContextKeySpec`.
#
# The ``file-lastmod.`` prefix is reserved for this plugin's
# bookkeeping; see :meth:`tabsdatak.api.TrxCtx.set_attr` for the
# reserved-prefix convention.
_LASTMOD_KEY_PREFIX = "file-lastmod.slot"


def import_files(
    *,
    ctx: SrcPluginCtx,
    cfg: ReaderConf,
    build_location: LocationBuilder,
    errors: FilePluginErrors,
    run_env: RunEnvBuilder | None = None,
) -> list[list[TableFileInput]]:
    """Import every path in ``cfg.paths``, one output slot per path.

    Each imported file is reported with the location it was read from as
    its `FILE_META_PATH` metadata, or with no metadata at all when
    ``cfg.drop_metadata`` is set (the source's
    ``tabsdata.src_metadata.drop`` config key).
    """
    conn = cfg.conn
    handler = make_handler()

    env: dict[str, str] | None = None
    if run_env is not None:
        try:
            env = run_env(conn, handler)
        except ApiException as e:
            raise errors.cred_resolution.exception(cause=e, reason=str(e))

    total = len(cfg.paths)
    logger.info("file-reader: import_files starting (%d path(s))", total)
    results: list[list[TableFileInput]] = []
    for slot_idx, entry in enumerate(cfg.paths):
        logger.info("file-reader: importing path %d/%d: %r", slot_idx + 1, total, entry)
        slot_dir = ctx.work_dir / f"slot{slot_idx}"
        slot_dir.mkdir(parents=True, exist_ok=True)
        location = build_location(conn, entry, handler)
        lastmod_key = f"{_LASTMOD_KEY_PREFIX}{slot_idx}"
        # Resume from the prior trx's cursor if one was stored; otherwise
        # fall back to the Src's baseline (the first-run seed).
        prior_lastmod = cast(str | None, ctx.get_attr(lastmod_key))
        try:
            report = _input.import_one(
                handler=handler,
                source=location,
                target_dir=slot_dir,
                file_format=cfg.file_format,
                paths=cfg.paths,
                format_cfg=cfg.format_cfg,
                initial_lastmod=cfg.initial_lastmod,
                lastmod_info=prior_lastmod,
                env=env,
            )
        except TransporterBinaryNotFoundError as e:
            raise errors.binary_not_found.exception(cause=e)
        except TransporterRunError as e:
            raise errors.read_failed.exception(cause=e, path=entry)
        except TransporterReportError as e:
            raise errors.report_invalid.exception(cause=e, path=entry)

        # Stage this slot's fresh cursor for the next trx. The transporter
        # may return ``None`` (no files observed; nothing to track), in
        # which case the prior value stays in effect for the next run.
        if report.lastmod_info is not None:
            ctx.set_attr(lastmod_key, report.lastmod_info)

        slot_files: list[TableFileInput] = []
        for f in report.files:
            target = f.get("to") if isinstance(f, dict) else None
            if target:
                metadata = (
                    None
                    if cfg.drop_metadata
                    else {FILE_META_PATH: str(f.get("from", ""))}
                )
                slot_files.append(
                    TableFileInput(
                        file=Path(cast(str, target)),
                        metadata=metadata,
                    )
                )
        logger.debug(
            "file-reader: path %d/%d produced %d file(s)",
            slot_idx + 1,
            total,
            len(slot_files),
        )
        results.append(slot_files)
    logger.info("file-reader: import_files finished (%d path(s))", total)
    return results


def copy_out(
    *,
    ctx: DestContext,
    cfg: WriterConf,
    tables: list[TableFileSpec],
    build_location: LocationBuilder,
    errors: FilePluginErrors,
    run_env: RunEnvBuilder | None = None,
) -> None:
    if len(tables) != len(cfg.paths):
        raise FileCommonErrorCode.FILE_COMMON_16.exception(
            tables=len(tables),
            paths=len(cfg.paths),
        )

    conn = cfg.conn
    handler = make_handler()

    env: dict[str, str] | None = None
    if run_env is not None:
        try:
            env = run_env(conn, handler)
        except ApiException as e:
            raise errors.cred_resolution.exception(cause=e, reason=str(e))

    total = len(cfg.paths)
    logger.info("file-writer: copy_out starting (%d path(s))", total)
    pairs: list[tuple[Any, Any]] = []
    for i, (parquet_path, entry) in enumerate(zip(tables, cfg.paths), 1):
        logger.info("file-writer: writing path %d/%d: %r", i, total, entry)
        resolved_entry = resolve_dest_tokens(entry, ctx)
        source_loc = handler.local_path_location(Path(parquet_path))
        target_loc = build_location(conn, resolved_entry, handler)
        pairs.append((source_loc, target_loc))

    try:
        _output.copy_one(
            handler=handler,
            source_target_pairs=pairs,
            work_dir=ctx.work_dir,
            file_format=cfg.file_format,
            paths=cfg.paths,
            format_cfg=cfg.format_cfg,
            env=env,
        )
    except TransporterBinaryNotFoundError as e:
        raise errors.binary_not_found.exception(cause=e)
    except TransporterRunError as e:
        raise errors.write_failed.exception(
            cause=e,
            target=", ".join(cfg.paths),
        )
    except TransporterReportError as e:
        raise errors.report_invalid.exception(
            cause=e,
            path=", ".join(cfg.paths),
        )
    logger.info("file-writer: copy_out finished (%d path(s))", total)

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast

from tabsdatak.common._transporter import _transporter1
from tabsdatak.conn.common.file import _input, _output
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    EXEC_PLAN_ID,
    EXEC_PLAN_TS,
    TRX_ID,
    FileInfo,
)
from tabsdatak.error import ApiException, ErrorCode
from tabsdatak.spi import (
    DestContext,
    SrcPluginCtx,
    TableFileSpec,
)

logger = logging.getLogger(__name__)

# --- Format / value helpers ----------------------------------------


def file_info_to_str(file_info: FileInfo | None) -> str | None:
    return file_info.value if file_info is not None else None


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
    # the connector-specific Conn class.
    conn: Any
    paths: list[str]
    format: _transporter1.TransporterFormat
    initial_lastmod: str | None = None
    file_info: str | None = None

    @classmethod
    def from_src(cls, conn: Any, src: Any) -> "ReaderConf":
        paths = list(src.paths)
        # Resolve ``format=AUTO`` from the paths' extensions before
        # mapping to a transporter format. Without this step
        # ``src_format_to_transporter`` would have silently fallen
        # back to Parquet for AUTO, which masks CSV/JSON/AVRO
        # mismatches as parquet-magic-byte failures inside the
        # transporter binary.
        resolved_fmt = _transporter1.resolve_auto_src_format(src.format, paths)
        return cls(
            conn=conn,
            paths=paths,
            format=_transporter1.src_format_to_transporter(
                resolved_fmt,
                src.format_cfg.get(resolved_fmt) if src.format_cfg else None,
            ),
            initial_lastmod=initial_lastmod_to_iso(src.initial_last_modified),
            file_info=file_info_to_str(src.file_info),
        )


@dataclass(frozen=True)
class WriterConf:
    conn: Any
    paths: list[str]
    format: _transporter1.TransporterFormat

    @classmethod
    def from_dest(cls, conn: Any, dest: Any) -> "WriterConf":
        paths = list(dest.paths)
        # Resolve ``format=AUTO`` from the paths' extensions before
        # mapping to a transporter format -- see the mirroring note
        # on :meth:`ReaderConf.from_src`.
        resolved_fmt = _transporter1.resolve_auto_dest_format(dest.format, paths)
        return cls(
            conn=conn,
            paths=paths,
            format=_transporter1.dest_format_to_transporter(
                resolved_fmt,
                dest.format_cfg.get(resolved_fmt) if dest.format_cfg else None,
            ),
        )


# --- Error-code bundle --------------------------------------------


@dataclass(frozen=True)
class FilePluginErrors:
    binary_not_found: ErrorCode
    read_failed: ErrorCode
    write_failed: ErrorCode
    report_invalid: ErrorCode
    cred_resolution: ErrorCode | None = None  # None when the connector has no creds


LocationBuilder = Callable[[Any, str], _transporter1.TransporterLocation]
RunEnvBuilder = Callable[[Any], dict[str, str]]


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
) -> list[list[TableFileSpec]]:
    conn = cfg.conn

    env: dict[str, str] | None = None
    if run_env is not None:
        try:
            env = run_env(conn)
        except ApiException as e:
            raise errors.cred_resolution.exception(cause=e, reason=str(e))

    total = len(cfg.paths)
    logger.info("file-reader: import_files starting (%d path(s))", total)
    results: list[list[TableFileSpec]] = []
    for slot_idx, entry in enumerate(cfg.paths):
        logger.info("file-reader: importing path %d/%d: %r", slot_idx + 1, total, entry)
        slot_dir = ctx.work_dir / f"slot{slot_idx}"
        slot_dir.mkdir(parents=True, exist_ok=True)
        location = build_location(conn, entry)
        lastmod_key = f"{_LASTMOD_KEY_PREFIX}{slot_idx}"
        # Resume from the prior trx's cursor if one was stored; otherwise
        # fall back to the Src's baseline (the first-run seed).
        prior_lastmod = cast(str | None, ctx.get_attr(lastmod_key))
        try:
            report = _input.import_one(
                source=location,
                target_dir=slot_dir,
                format=cfg.format,
                initial_lastmod=cfg.initial_lastmod,
                lastmod_info=prior_lastmod,
                file_info=cfg.file_info,
                env=env,
            )
        except _transporter1.TransporterBinaryNotFoundError as e:
            raise errors.binary_not_found.exception(cause=e)
        except _transporter1.TransporterRunError as e:
            raise errors.read_failed.exception(cause=e, path=entry)
        except _transporter1.TransporterReportError as e:
            raise errors.report_invalid.exception(cause=e, path=entry)

        # Stage this slot's fresh cursor for the next trx. The transporter
        # may return ``None`` (no files observed; nothing to track), in
        # which case the prior value stays in effect for the next run.
        if report.lastmod_info is not None:
            ctx.set_attr(lastmod_key, report.lastmod_info)

        slot_files: list[TableFileSpec] = []
        for f in report.files:
            target = f.get("to") if isinstance(f, dict) else None
            if target:
                slot_files.append(Path(cast(str, target)))
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

    env: dict[str, str] | None = None
    if run_env is not None:
        try:
            env = run_env(conn)
        except ApiException as e:
            raise errors.cred_resolution.exception(cause=e, reason=str(e))

    total = len(cfg.paths)
    logger.info("file-writer: copy_out starting (%d path(s))", total)
    pairs: list[tuple[Any, Any]] = []
    for i, (parquet_path, entry) in enumerate(zip(tables, cfg.paths), 1):
        logger.info("file-writer: writing path %d/%d: %r", i, total, entry)
        resolved_entry = resolve_dest_tokens(entry, ctx)
        source_loc = _transporter1.TransporterLocalFile(
            _transporter1.path_to_file_uri(parquet_path)
        )
        target_loc = build_location(conn, resolved_entry)
        pairs.append((source_loc, target_loc))

    try:
        _output.copy_one(
            source_target_pairs=pairs,
            work_dir=ctx.work_dir,
            format=cfg.format,
            env=env,
        )
    except _transporter1.TransporterBinaryNotFoundError as e:
        raise errors.binary_not_found.exception(cause=e)
    except _transporter1.TransporterRunError as e:
        raise errors.write_failed.exception(
            cause=e,
            target=", ".join(cfg.paths),
        )
    except _transporter1.TransporterReportError as e:
        raise errors.report_invalid.exception(
            cause=e,
            path=", ".join(cfg.paths),
        )
    logger.info("file-writer: copy_out finished (%d path(s))", total)

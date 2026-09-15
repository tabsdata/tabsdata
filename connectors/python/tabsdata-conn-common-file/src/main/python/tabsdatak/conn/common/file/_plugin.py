#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast

from tabsdatak.common._fileformat.taxonomy import (
    EBCDIC_COPYBOOK_KEYS,
    EXCEL_SHEET_KEY,
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.common._transporter.facade import make_handler
from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
    TransporterReportError,
    TransporterRunError,
)
from tabsdatak.conn.common.constants import SRC_META_DROP_KEY
from tabsdatak.conn.common.file import _input, _output
from tabsdatak.conn.common.file._validation import split_sheet_fragment
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


def format_cfg_with_sheets(
    format_cfg: dict | None, sheets: list[int | None]
) -> dict | None:
    """``format_cfg`` with the per-path worksheet numbers folded into its
    EXCEL entry, as a list parallel to ``paths``.

    Returned unchanged when no path named a worksheet, so a source that has
    nothing to do with Excel keeps the config it was given -- ``None``
    included. The user's own dict is copied, never mutated.
    """
    if all(sheet is None for sheet in sheets):
        return format_cfg
    merged = dict(format_cfg or {})
    excel = dict(merged.get(SrcFileFormat.EXCEL) or {})
    excel[EXCEL_SHEET_KEY] = list(sheets)
    merged[SrcFileFormat.EXCEL] = excel
    return merged


def sheet_in(format_cfg: dict | None) -> int | None:
    """The worksheet a *narrowed* ``format_cfg`` names, or ``None``.

    The inverse of what :meth:`ReaderConf.format_cfg_for` produced, read
    back for provenance -- the reader gets the number from the same place.
    """
    return ((format_cfg or {}).get(SrcFileFormat.EXCEL) or {}).get(EXCEL_SHEET_KEY)


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
    # Carries the worksheet numbers the ``#N`` fragments named, as a list
    # under its EXCEL entry -- see :func:`format_cfg_with_sheets`. Built by
    # :meth:`from_src`, which is also where the fragments are split off
    # ``paths``.
    format_cfg: dict | None = None
    initial_lastmod: str | None = None
    drop_metadata: bool = False
    # Delete the source files the lastmod cursor turned down as already
    # consumed, once every file in this run has been copied.
    purge: bool = False

    @classmethod
    def from_src(cls, conn: Any, src: Any) -> "ReaderConf":
        split = [split_sheet_fragment(p, src.format) for p in src.paths]
        return cls(
            conn=conn,
            paths=[path for path, _ in split],
            file_format=src.format,
            format_cfg=format_cfg_with_sheets(
                src.format_cfg, [sheet for _, sheet in split]
            ),
            initial_lastmod=initial_lastmod_to_iso(src.initial_last_modified),
            drop_metadata=bool((src.src_cfg or {}).get(SRC_META_DROP_KEY, False)),
            purge=bool(src.purge),
        )

    def format_cfg_for(self, index: int) -> dict | None:
        """:attr:`format_cfg` with every per-path list narrowed to the value
        belonging to the path at ``index`` -- EXCEL's worksheet number and
        EBCDIC's copybook.

        Every request covers one path, so the reader is handed one value.
        Narrowing here rather than in :meth:`from_src` keeps ``format_cfg``
        the one per-source dict it has always been.
        """
        cfg = self.format_cfg
        if not cfg:
            return cfg
        cfg = self._narrowed(cfg, SrcFileFormat.EXCEL, (EXCEL_SHEET_KEY,), index)
        return self._narrowed(cfg, SrcFileFormat.EBCDIC, EBCDIC_COPYBOOK_KEYS, index)

    @staticmethod
    def _narrowed(
        cfg: dict, fmt: SrcFileFormat, keys: tuple[str, ...], index: int
    ) -> dict:
        """``cfg`` with each of ``keys`` under ``fmt`` replaced by its element
        at ``index``. An empty slot drops the key, so the reader falls back to
        its own default rather than seeing a ``None``."""
        entry = cfg.get(fmt) or {}
        present = [key for key in keys if key in entry]
        if not present:
            return cfg
        narrowed = dict(entry)
        for key in present:
            values = narrowed[key]
            value = values[index] if index < len(values) else None
            if value is None:
                del narrowed[key]
            else:
                narrowed[key] = value
        return {**cfg, fmt: narrowed}


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
        slot_cfg = cfg.format_cfg_for(slot_idx)
        sheet = sheet_in(slot_cfg)
        try:
            report = _input.import_one(
                handler=handler,
                source=location,
                target_dir=slot_dir,
                file_format=cfg.file_format,
                paths=cfg.paths,
                format_cfg=slot_cfg,
                initial_lastmod=cfg.initial_lastmod,
                lastmod_info=prior_lastmod,
                purge=cfg.purge,
                env=env,
            )
        except TransporterBinaryNotFoundError as e:
            raise errors.binary_not_found.exception(cause=e)
        except TransporterRunError as e:
            raise errors.read_failed.exception(cause=e, path=entry)
        except TransporterReportError as e:
            raise errors.report_invalid.exception(cause=e, path=entry)

        # ``None`` unless this was a purging find-copy; transporter1 never
        # sets it.
        if report.purged is not None:
            logger.info(
                "file-reader: slot %d purged %d source file(s)",
                slot_idx,
                len(report.purged.deleted),
            )

        # Stage this slot's fresh cursor for the next trx. The transporter
        # may return ``None`` (no files observed; nothing to track), in
        # which case the prior value stays in effect for the next run.
        if report.lastmod_info is not None:
            ctx.set_attr(lastmod_key, report.lastmod_info)

        slot_files: list[TableFileInput] = []
        for f in report.files:
            target = f.get("to") if isinstance(f, dict) else None
            if target:
                # The uri the transporter read, with this slot's worksheet
                # put back on it: the transporter never saw the fragment,
                # but provenance has to name the sheet a row came from, and
                # the value has to be pasteable straight back into ``paths``.
                #
                # Appended to each *file's* own ``from`` rather than taken
                # from the configured path, because one slot can glob many
                # files: they differ in uri and share the one worksheet.
                origin = str(f.get("from", ""))
                if sheet is not None:
                    origin = f"{origin}#{sheet}"
                metadata = None if cfg.drop_metadata else {FILE_META_PATH: origin}
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

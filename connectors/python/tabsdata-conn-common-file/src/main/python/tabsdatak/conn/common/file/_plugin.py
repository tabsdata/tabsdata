#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from tabsdatak._api import path_timestamp
from tabsdatak._legacy import warn_renamed
from tabsdatak.common._fileformat.taxonomy import (
    EXCEL_SHEET_KEY,
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.conn.common.constants import SRC_META_DROP_KEY
from tabsdatak.conn.common.file._validation import split_sheet_fragment
from tabsdatak.conn.common.file.typing_ import (
    EXEC_PLAN_ID,
    EXEC_PLAN_TS,
    TRIGGERED_ON,
    TRX_ID,
)
from tabsdatak.error import ErrorCode
from tabsdatak.spi import (
    DestContext,
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
    if EXEC_PLAN_TS in target_path:
        logger.warning(
            f"The destination path '{target_path}' uses '{EXEC_PLAN_TS}', which is "
            f"deprecated; use '{TRIGGERED_ON}' instead."
        )
        warn_renamed(EXEC_PLAN_TS, TRIGGERED_ON)
    triggered_on = path_timestamp(ctx.triggered_on)
    tokens = {
        EXEC_PLAN_ID: ctx.execution_id,
        TRX_ID: ctx.transaction_id,
        TRIGGERED_ON: triggered_on,
        EXEC_PLAN_TS: triggered_on,
    }
    resolved = target_path
    for token, value in tokens.items():
        resolved = resolved.replace(token, value)
    return resolved


# --- Reader / writer adapter conf --------------------------------


@dataclass(frozen=True)
class ReaderConf:
    # The connector-specific Conn instance, typed Any so this module
    # stays decoupled from it. ``file_format`` is the raw (possibly
    # AUTO) format enum; the reader resolves AUTO against ``paths``.
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
    # consumed, once every file in this run has been read.
    purge: bool = False
    include_compressed: bool = False

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
            include_compressed=src.include_compressed,
        )

    def format_cfg_for(self, index: int) -> dict | None:
        """:attr:`format_cfg` with every per-path list narrowed to the value
        belonging to the path at ``index`` -- EXCEL's worksheet number.

        Every request covers one path, so the reader is handed one value.
        Narrowing here rather than in :meth:`from_src` keeps ``format_cfg``
        the one per-source dict it has always been.
        """
        cfg = self.format_cfg
        if not cfg:
            return cfg
        # return self._narrowed(cfg, SrcFileFormat.EBCDIC, EBCDIC_COPYBOOK_KEYS, index)
        return self._narrowed(cfg, SrcFileFormat.EXCEL, (EXCEL_SHEET_KEY,), index)

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
    read_failed: ErrorCode
    write_failed: ErrorCode
    cred_resolution: ErrorCode | None = None  # None when the connector has no creds


# --- Incremental cursor --------------------------------------------


# Per-slot incremental cursor key: each slot stores its encoded
# last-modified cursor on the trx context so the next trx resumes where
# this one left off. Slots are addressed positionally (``slot0``,
# ``slot1``, ...) so a Src whose ``paths`` ordering is stable
# round-trips cleanly. The key shape (one alphanumeric label per
# dot-separated segment, no underscores) matches the context-key regex
# declared on `ContextKeySpec`.
#
# The ``file-lastmod.`` prefix is reserved for this plugin's
# bookkeeping; see :meth:`tabsdatak.api.TransactionCtx.set_attr` for the
# reserved-prefix convention.
_LASTMOD_KEY_PREFIX = "file-lastmod.slot"

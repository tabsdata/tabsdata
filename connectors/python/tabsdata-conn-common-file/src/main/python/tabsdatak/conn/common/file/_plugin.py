from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from tabsdatak.conn.common.file import _input, _output, _transporter
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import FileInfo
from tabsdatak.error import ApiException, ErrorCode
from tabsdatak.spi import (
    DestContext,
    ParquetFile,
    SrcContext,
    TableFileSpec,
)

# --- Format / value helpers ----------------------------------------


def file_info_to_str(file_info: FileInfo | None) -> str | None:
    return file_info.value if file_info is not None else None


def initial_lastmod_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def resolve_dest_tokens(target_path: str) -> str:
    # Placeholder: dest-token substitution lands here when wired.
    return target_path


# --- Reader / writer adapter conf --------------------------------


@dataclass(frozen=True)
class ReaderConf:
    # The Conn instance the orchestrator hands to ``build_location``
    # / ``run_env``.  Typed Any so this module stays decoupled from
    # the connector-specific Conn class.
    conn: Any
    paths: list[str]
    format: _transporter.TransporterFormat
    initial_lastmod: str | None = None
    file_info: str | None = None

    @classmethod
    def from_src(cls, src: Any) -> "ReaderConf":
        return cls(
            conn=src.conn,
            paths=list(src.paths),
            format=_transporter.src_format_to_transporter(
                src.format,
                src.format_cfg.get(src.format) if src.format_cfg else None,
            ),
            initial_lastmod=initial_lastmod_to_iso(src.initial_last_modified),
            file_info=file_info_to_str(src.file_info),
        )


@dataclass(frozen=True)
class WriterConf:
    conn: Any
    paths: list[str]
    format: _transporter.TransporterFormat

    @classmethod
    def from_dest(cls, dest: Any) -> "WriterConf":
        return cls(
            conn=dest.conn,
            paths=list(dest.paths),
            format=_transporter.dest_format_to_transporter(
                dest.format,
                dest.format_cfg.get(dest.format) if dest.format_cfg else None,
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


LocationBuilder = Callable[[Any, str], _transporter.TransporterLocation]
RunEnvBuilder = Callable[[Any], dict[str, str]]


# --- Orchestrators -------------------------------------------------


def import_files(
    *,
    ctx: SrcContext,
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

    results: list[list[TableFileSpec]] = []
    for slot_idx, entry in enumerate(cfg.paths):
        slot_dir = ctx.work_dir() / f"slot{slot_idx}"
        slot_dir.mkdir(parents=True, exist_ok=True)
        location = build_location(conn, entry)
        try:
            report = _input.import_one(
                source=location,
                target_dir=slot_dir,
                format=cfg.format,
                initial_lastmod=cfg.initial_lastmod,
                file_info=cfg.file_info,
                env=env,
            )
        except _transporter.TransporterBinaryNotFoundError as e:
            raise errors.binary_not_found.exception(cause=e)
        except _transporter.TransporterRunError as e:
            raise errors.read_failed.exception(cause=e, path=entry)
        except _transporter.TransporterReportError as e:
            raise errors.report_invalid.exception(cause=e, path=entry)

        slot_files: list[TableFileSpec] = []
        for f in report.files:
            target = f.get("to") if isinstance(f, dict) else None
            if target:
                slot_files.append(ParquetFile(target))
        results.append(slot_files)
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

    pairs: list[tuple[Any, Any]] = []
    for parquet_path, entry in zip(tables, cfg.paths):
        resolved_entry = resolve_dest_tokens(entry)
        source_loc = _transporter.TransporterLocalFile(
            _transporter.path_to_file_uri(parquet_path)
        )
        target_loc = build_location(conn, resolved_entry)
        pairs.append((source_loc, target_loc))

    try:
        _output.copy_one(
            source_target_pairs=pairs,
            work_dir=ctx.work_dir(),
            format=cfg.format,
            env=env,
        )
    except _transporter.TransporterBinaryNotFoundError as e:
        raise errors.binary_not_found.exception(cause=e)
    except _transporter.TransporterRunError as e:
        raise errors.write_failed.exception(
            cause=e,
            target=", ".join(cfg.paths),
        )
    except _transporter.TransporterReportError as e:
        raise errors.report_invalid.exception(
            cause=e,
            path=", ".join(cfg.paths),
        )

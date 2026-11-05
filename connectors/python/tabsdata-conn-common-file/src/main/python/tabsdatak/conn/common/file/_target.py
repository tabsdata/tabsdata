#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import obstore
import polars as pl

from tabsdata.expansions.foundation.writers import avro  # noqa: TID251
from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import DestFileFormat
from tabsdatak.conn.common._find import PATH_SEPARATOR
from tabsdatak.conn.common.file._plugin import (
    FilePluginErrors,
    WriterConf,
    resolve_dest_tokens,
)
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import DestContext

logger = logging.getLogger(__name__)

EXTENSIONS = {
    ".avro": DestFileFormat.AVRO,
    ".csv": DestFileFormat.CSV,
    ".json": DestFileFormat.JSON,
    ".jsonl": DestFileFormat.JSON,
    ".ndjson": DestFileFormat.JSON,
    ".parquet": DestFileFormat.PARQUET,
    ".pq": DestFileFormat.PARQUET,
}

EOL_CHAR = "eol_char"
LINE_TERMINATOR = "line_terminator"
UTC = "UTC"
WINDOWS_SEPARATOR = "\\"


def extension(path: str) -> str:
    name = path.rpartition(PATH_SEPARATOR)[2]
    stem, dot, suffix = name.rpartition(".")
    return f".{suffix.lower()}" if dot and stem else ""


def format_of(file_format: DestFileFormat, path: str) -> DestFileFormat:
    if file_format is not DestFileFormat.AUTO:
        return file_format
    found = EXTENSIONS.get(extension(path))
    if found is None:
        raise FileFormatErrorCode.FILE_FORMAT_4.exception(
            paths=[path], reason=f"no recognised extension on {path!r}"
        )
    return found


def formats(file_format: DestFileFormat, paths: list[str]) -> list[DestFileFormat]:
    found = [format_of(file_format, path) for path in paths]
    if len(set(found)) > 1:
        raise FileFormatErrorCode.FILE_FORMAT_4.exception(
            paths=paths,
            reason=f"mixed formats inferred ({sorted({f.value for f in found})})",
        )
    return found


def csv_options(format_cfg: Mapping[Any, Any] | None) -> dict[str, Any]:
    options = dict((format_cfg or {}).get(DestFileFormat.CSV) or {})
    if EOL_CHAR in options:
        options[LINE_TERMINATOR] = options.pop(EOL_CHAR)
    return options


def in_utc(frame: pl.LazyFrame) -> pl.LazyFrame:
    zoned = [
        name
        for name, dtype in frame.collect_schema().items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
    ]
    if not zoned:
        return frame
    return frame.with_columns(pl.col(zoned).dt.convert_time_zone(UTC))


def key(path: str) -> str:
    return path.replace(WINDOWS_SEPARATOR, PATH_SEPARATOR)


def write(
    store: FileStore,
    path: str,
    file_format: DestFileFormat,
    frame: pl.LazyFrame,
    csv: Mapping[str, Any],
) -> None:
    writer = obstore.open_writer(store.store(), key(path))
    match file_format:
        case DestFileFormat.CSV:
            in_utc(frame).sink_csv(writer, **csv)
        case DestFileFormat.JSON:
            in_utc(frame).sink_ndjson(writer)
        case DestFileFormat.PARQUET:
            frame.sink_parquet(writer)
        case DestFileFormat.AVRO:
            avro(frame.collect_batches(), writer)
        case _:
            raise FileFormatErrorCode.FILE_FORMAT_2.exception(format=file_format)
    writer.close()


def write_files(
    *,
    ctx: DestContext,
    cfg: WriterConf,
    store: FileStore,
    tables: list[pl.LazyFrame],
    errors: FilePluginErrors,
) -> None:
    if len(tables) != len(cfg.paths):
        raise FileCommonErrorCode.FILE_COMMON_16.exception(
            tables=len(tables),
            paths=len(cfg.paths),
        )
    targets = [resolve_dest_tokens(path, ctx) for path in cfg.paths]
    found = formats(cfg.file_format, targets)
    csv = csv_options(cfg.format_cfg)
    total = len(targets)
    for position, (target, file_format, frame) in enumerate(
        zip(targets, found, tables), 1
    ):
        logger.info("file-writer: writing path %d/%d: %r", position, total, target)
        try:
            write(store, target, file_format, frame, csv)
        except Exception as error:
            raise errors.write_failed.exception(
                cause=error,
                target=store.uri(key(target)),
            )
    logger.info("file-writer: wrote %d path(s)", total)

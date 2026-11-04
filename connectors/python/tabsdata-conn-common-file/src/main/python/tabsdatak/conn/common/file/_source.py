#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import replace
from datetime import datetime
from typing import cast

from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.conn.common._cursor import LastModifiedCursor
from tabsdatak.conn.common._find import PATH_SEPARATOR, Found, find, split_pattern
from tabsdatak.conn.common.file._codecs import decompressed
from tabsdatak.conn.common.file._plugin import (
    _LASTMOD_KEY_PREFIX,
    FilePluginErrors,
    ReaderConf,
    sheet_in,
)
from tabsdatak.conn.common.file._read._blocks import Readable
from tabsdatak.conn.common.file._read._policy import Format, policy
from tabsdatak.conn.common.file._select import accepts, codec, plain
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import FrameMetadata, LazyFrameInput, SrcPluginCtx

logger = logging.getLogger(__name__)

# ToDo(PW7): on once the streaming matrix passes on every codec.
CODECS_ENABLED = False

EXTENSIONS = {
    ".avro": SrcFileFormat.AVRO,
    ".csv": SrcFileFormat.CSV,
    ".json": SrcFileFormat.JSON,
    ".jsonl": SrcFileFormat.JSON,
    ".log": SrcFileFormat.LOG,
    ".ndjson": SrcFileFormat.JSON,
    ".parquet": SrcFileFormat.PARQUET,
    ".pq": SrcFileFormat.PARQUET,
    ".xlsx": SrcFileFormat.EXCEL,
}

READERS = {
    SrcFileFormat.AVRO: Format.AVRO,
    SrcFileFormat.CSV: Format.CSV,
    SrcFileFormat.EXCEL: Format.EXCEL,
    SrcFileFormat.JSON: Format.NDJSON,
    SrcFileFormat.LOG: Format.LOG,
}

Origin = Callable[[FileStore, str], str]


def extension(path: str) -> str:
    name = plain(path.rpartition(PATH_SEPARATOR)[2])
    stem, dot, suffix = name.rpartition(".")
    return f".{suffix.lower()}" if dot and stem else ""


def resolved(file_format: SrcFileFormat, paths: list[str]) -> SrcFileFormat:
    if file_format is not SrcFileFormat.AUTO:
        return file_format
    found = {path: EXTENSIONS.get(extension(path)) for path in paths}
    unknown = [path for path, detected in found.items() if detected is None]
    if unknown:
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=paths, reason=f"no recognised extension on {unknown!r}"
        )
    formats = set(found.values())
    if len(formats) > 1:
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=paths,
            reason=f"mixed formats inferred ({sorted(f.value for f in formats)})",
        )
    return formats.pop()


def read_files(
    *,
    ctx: SrcPluginCtx,
    cfg: ReaderConf,
    store: FileStore,
    origin: Origin,
    errors: FilePluginErrors,
) -> list[Iterable[LazyFrameInput]]:
    file_format = resolved(cfg.file_format, cfg.paths)
    slots: list[Iterable[LazyFrameInput]] = []
    for slot, entry in enumerate(cfg.paths):
        directory, pattern = split_pattern(entry.replace("\\", PATH_SEPARATOR))
        slot_cfg = cfg.format_cfg_for(slot)
        options = (slot_cfg or {}).get(file_format)
        sheet = sheet_in(slot_cfg)
        if sheet is not None and file_format is not SrcFileFormat.EXCEL:
            raise FileFormatErrorCode.FILE_FORMAT_5.exception(
                sheet=sheet, format=file_format.value
            )
        found = selected(ctx, cfg, store, slot, directory, pattern, entry, errors)
        logger.info(
            "file-reader: path %d/%d %r took %d file(s)",
            slot + 1,
            len(cfg.paths),
            entry,
            len(found.selected),
        )
        paths = [joined(directory, file.name) for file in found.selected]
        purgeable = [joined(directory, file.name) for file in found.purgeable]
        slots.append(
            tables(cfg, store, origin, file_format, options, sheet, paths, purgeable)
        )
    return slots


def selected(
    ctx: SrcPluginCtx,
    cfg: ReaderConf,
    store: FileStore,
    slot: int,
    directory: str,
    pattern: str,
    entry: str,
    errors: FilePluginErrors,
) -> Found:
    try:
        listed = store.listed(directory)
    except Exception as error:
        raise errors.read_failed.exception(cause=error, path=entry)
    test = accepts(pattern, cfg.include_compressed)
    if cfg.initial_lastmod is None:
        return find(listed, pattern, None, accepts=test)
    key = f"{_LASTMOD_KEY_PREFIX}{slot}"
    cursor = LastModifiedCursor.decode(
        cast(str | None, ctx.get_attr(key)),
        datetime.fromisoformat(cfg.initial_lastmod),
    )
    found = find(listed, pattern, cursor, accepts=test)
    ctx.set_attr(key, cursor.encode())
    return found


def tables(
    cfg: ReaderConf,
    store: FileStore,
    origin: Origin,
    file_format: SrcFileFormat,
    options: dict | None,
    sheet: int | None,
    paths: list[str],
    purgeable: list[str],
) -> Iterator[LazyFrameInput]:
    for path in paths:
        found = codec(path)
        if found is not None and not CODECS_ENABLED:
            raise FileCommonErrorCode.FILE_COMMON_37.exception(path=path, codec=found)
        table = read(store, file_format, options, path, found)
        if cfg.drop_metadata:
            yield table
            continue
        where = origin(store, path)
        yield described(table, where if sheet is None else f"{where}#{sheet}")
    if cfg.purge and purgeable:
        store.delete(purgeable)
        logger.info("file-reader: purged %d source file(s)", len(purgeable))


def read(
    store: FileStore,
    file_format: SrcFileFormat,
    options: dict | None,
    path: str,
    compression: str | None,
) -> LazyFrameInput:
    match file_format:
        case SrcFileFormat.EXCEL | SrcFileFormat.PARQUET if compression is not None:
            raise FileCommonErrorCode.FILE_COMMON_38.exception(
                path=path, format=file_format.value
            )
        case SrcFileFormat.PARQUET:
            return LazyFrameInput(store.scan_parquet(path))
        case SrcFileFormat.EXCEL:
            return policy(Format.EXCEL, path, options).read(lambda: store.ranged(path))
        case (
            SrcFileFormat.AVRO
            | SrcFileFormat.CSV
            | SrcFileFormat.JSON
            | SrcFileFormat.LOG
        ):
            name = path.rpartition(PATH_SEPARATOR)[2]
            reader = policy(READERS[file_format], name, options)
            return reader.read(lambda: opened(store, path, compression))
        case _:
            raise ValueError(f"The format '{file_format}' cannot be read")


def opened(store: FileStore, path: str, compression: str | None) -> Readable:
    stream = store.open(path)
    return stream if compression is None else decompressed(stream, compression)


def described(table: LazyFrameInput, where: str) -> LazyFrameInput:
    columns = {FILE_META_PATH: where}
    metadata = table.metadata
    if metadata is None:
        return LazyFrameInput(table.frame, FrameMetadata(columns=columns))
    if callable(metadata):
        computed = metadata
        return LazyFrameInput(table.frame, lambda: replace(computed(), columns=columns))
    return LazyFrameInput(table.frame, replace(metadata, columns=columns))


def joined(directory: str, name: str) -> str:
    return f"{directory}{PATH_SEPARATOR}{name}" if directory else name

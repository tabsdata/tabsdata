#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import replace
from datetime import datetime
from typing import cast

from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.conn.common._cursor import LastModifiedCursor
from tabsdatak.conn.common._find import PATH_SEPARATOR, Found, find, split_pattern
from tabsdatak.conn.common.file._archives import member
from tabsdatak.conn.common.file._codecs import decompressed
from tabsdatak.conn.common.file._plugin import (
    _LASTMOD_KEY_PREFIX,
    FilePluginErrors,
    ReaderConf,
    sheet_in,
)
from tabsdatak.conn.common.file._read._blocks import Readable, close
from tabsdatak.conn.common.file._read._policy import Format, policy
from tabsdatak.conn.common.file._select import accepts, archive, plain, wrapper
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import FrameMetadata, LazyFrameInput, SrcPluginCtx

logger = logging.getLogger(__name__)

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
    SrcFileFormat.PARQUET: Format.PARQUET,
}

Origin = Callable[[FileStore, str], str]


def extension(path: str) -> str:
    name = plain(path.rpartition(PATH_SEPARATOR)[2])
    stem, dot, suffix = name.rpartition(".")
    return f".{suffix.lower()}" if dot and stem else ""


def format_of(file_format: SrcFileFormat, path: str) -> SrcFileFormat:
    if file_format is not SrcFileFormat.AUTO:
        return file_format
    found = EXTENSIONS.get(extension(path))
    if found is None:
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=[path], reason=f"no recognised format extension on {path!r}"
        )
    return found


def checked(cfg: ReaderConf, taken: list[list[str]]) -> None:
    formats = set()
    for slot, paths in enumerate(taken):
        sheet = sheet_in(cfg.format_cfg_for(slot))
        for path in paths:
            file_format = format_of(cfg.file_format, path)
            if sheet is not None and file_format is not SrcFileFormat.EXCEL:
                raise FileFormatErrorCode.FILE_FORMAT_5.exception(
                    sheet=sheet, format=file_format.value
                )
            formats.add(file_format)
    if len(formats) > 1:
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=[path for paths in taken for path in paths],
            reason=f"mixed formats inferred ({sorted(f.value for f in formats)})",
        )


def read_files(
    *,
    ctx: SrcPluginCtx,
    cfg: ReaderConf,
    store: FileStore,
    origin: Origin,
    errors: FilePluginErrors,
) -> list[Iterable[LazyFrameInput]]:
    taken: list[list[str]] = []
    purgeables: list[list[str]] = []
    for slot, entry in enumerate(cfg.paths):
        directory, pattern = split_pattern(entry.replace("\\", PATH_SEPARATOR))
        found = selected(ctx, cfg, store, slot, directory, pattern, entry, errors)
        logger.info(
            "file-reader: path %d/%d %r took %d file(s)",
            slot + 1,
            len(cfg.paths),
            entry,
            len(found.selected),
        )
        taken.append([joined(directory, file.name) for file in found.selected])
        purgeables.append([joined(directory, file.name) for file in found.purgeable])
    checked(cfg, taken)
    return [
        tables(ctx, cfg, store, origin, slot, paths, purgeable)
        for slot, (paths, purgeable) in enumerate(zip(taken, purgeables, strict=True))
    ]


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
    ctx: SrcPluginCtx,
    cfg: ReaderConf,
    store: FileStore,
    origin: Origin,
    slot: int,
    paths: list[str],
    purgeable: list[str],
) -> Iterator[LazyFrameInput]:
    slot_cfg = cfg.format_cfg_for(slot)
    sheet = sheet_in(slot_cfg)
    for path in paths:
        file_format = format_of(cfg.file_format, path)
        options = (slot_cfg or {}).get(file_format)
        table = read(ctx, store, file_format, options, path, wrapper(path))
        if cfg.drop_metadata:
            yield table
            continue
        where = origin(store, path)
        yield described(table, where if sheet is None else f"{where}#{sheet}")
    if cfg.purge and purgeable:
        store.delete(purgeable)
        logger.info("file-reader: purged %d source file(s)", len(purgeable))


def read(
    ctx: SrcPluginCtx,
    store: FileStore,
    file_format: SrcFileFormat,
    options: dict | None,
    path: str,
    wrapped: str | None,
) -> LazyFrameInput:
    inner = wrapper(plain(path))
    if wrapped is not None and inner is not None:
        raise FileCommonErrorCode.FILE_COMMON_38.exception(
            path=path, inner=inner, outer=wrapped
        )
    match file_format:
        case SrcFileFormat.EXCEL | SrcFileFormat.PARQUET if wrapped is not None:
            scratch = ctx._scratch
            staged = f"{uuid.uuid4().hex}-{plain(path.rpartition(PATH_SEPARATOR)[2])}"
            stream = opened(store, path, wrapped)
            try:
                scratch.write(staged, stream)
            finally:
                close(stream)
            reader = policy(READERS[file_format], path, options)
            return reader.read(lambda: scratch.read(staged))
        case SrcFileFormat.PARQUET:
            return LazyFrameInput(store.scan_parquet(path))
        case SrcFileFormat.EXCEL:
            reader = policy(READERS[file_format], path, options)
            return reader.read(lambda: store.ranged(path))
        case (
            SrcFileFormat.AVRO
            | SrcFileFormat.CSV
            | SrcFileFormat.JSON
            | SrcFileFormat.LOG
        ):
            name = path.rpartition(PATH_SEPARATOR)[2]
            reader = policy(READERS[file_format], name, options)
            return reader.read(lambda: opened(store, path, wrapped))
        case _:
            raise ValueError(f"The format '{file_format}' cannot be read")


def opened(store: FileStore, path: str, wrapped: str | None) -> Readable:
    if wrapped is None:
        return store.open(path)
    if archive(path) is not None:
        return member(store, path)
    return decompressed(store.open(path), wrapped)


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

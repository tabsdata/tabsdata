#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl

from tabsdatak.conn.common.file import _plugin as _file_plugin, _source, _target
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.localfile import (
    LocalFileDest,
    LocalFileDestConn,
    LocalFileSrc,
    LocalFileSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.localfile._explorer import LocalFileExplorer
from tabsdatak.conn.localfile.error import LocalFileErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    LazyFrameInput,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableMode,
)

# --- Error-code bundle (no creds → no cred_resolution code) --------


ERRORS = _file_plugin.FilePluginErrors(
    read_failed=LocalFileErrorCode.LOCALFILE_7,
    write_failed=LocalFileErrorCode.LOCALFILE_8,
)


# --- Plugins ------------------------------------------------------


class LocalFileSrcPlugin(SrcPlugin[LocalFileSrcConn, LocalFileSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: LocalFileSrcConn,
        src: LocalFileSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        base = Path(conn._base_path())
        return _source.read_files(
            ctx=ctx,
            cfg=_file_plugin.ReaderConf.from_src(conn, src),
            store=FileStore(base.as_uri()),
            origin=lambda _, path: str(base.joinpath(*path.split("/"))),
            errors=ERRORS,
        )


class LocalFileDestPlugin(
    DestPlugin[LocalFileDestConn, LocalFileDest, list[pl.LazyFrame | None]]
):
    def write_out(
        self,
        ctx: DestContext,
        conn: LocalFileDestConn,
        dest: LocalFileDest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        _target.write_files(
            ctx=ctx,
            cfg=_file_plugin.WriterConf.from_dest(conn, dest),
            store=FileStore(Path(conn._base_path()).as_uri()),
            tables=tables,
            errors=ERRORS,
        )


# --- Bindings ----------------------------------------------------


LOCALFILE_SRC = SrcDef(
    conn=LocalFileSrcConn,
    type_="local-file-in",
    system="Local File",
    src_version="v1",
    src=LocalFileSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=LocalFileSrcPlugin,
    explorer=LocalFileExplorer,
    icon="conn/localfile/resources/local-file.svg",
)


LOCALFILE_DEST = DestDef(
    conn=LocalFileDestConn,
    type_="local-file-out",
    system="Local File",
    dest_version="v1",
    dest=LocalFileDest,
    cardinality=lambda dest: len(dest.paths),
    plugin=LocalFileDestPlugin,
    explorer=LocalFileExplorer,
    icon="conn/localfile/resources/local-file.svg",
)

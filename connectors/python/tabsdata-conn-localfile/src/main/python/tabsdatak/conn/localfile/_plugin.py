#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.common.file import _plugin as _file_plugin
from tabsdatak.conn.common.types import full_path
from tabsdatak.conn.localfile import (
    LocalFileDest,
    LocalFileDestConn,
    LocalFileSrc,
    LocalFileSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.localfile._explorer import (
    LocalFileExplorer
)
from tabsdatak.conn.localfile.error import LocalFileErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
    TableMode,
    to_table_file_inputs,
)

# Conn helpers operate on both Src and Dest variants.
_AnyLocalFileConn = LocalFileSrcConn | LocalFileDestConn

# --- Path resolution + location builder ----------------------------


# Only ``*`` is supported as a wildcard. Paths containing it are URI-
# composed manually because ``Path.as_uri`` would URL-encode ``*`` as
# ``%2A`` and defeat the transporter's glob expansion.


def build_location(
    conn: _AnyLocalFileConn, entry: str, handler: TransporterHandler
) -> Any:
    # ``base_path`` is an absolute local path (leading "/"). ``full_path``
    # composes cloud-style, from a slash-stripped prefix, so it drops the
    # leading "/"; re-anchor the joined result at the filesystem root.
    resolved = "/" + full_path(conn._base_path(), entry)
    if "*" in resolved:
        head, sep, basename = resolved.rpartition("/")
        head_uri = handler.local_uri(head + sep)
        return handler.local_location(head_uri + basename)
    return handler.local_location(handler.local_uri(resolved))


# --- Error-code bundle (no creds → no cred_resolution code) --------


ERRORS = _file_plugin.FilePluginErrors(
    binary_not_found=LocalFileErrorCode.LOCALFILE_6,
    read_failed=LocalFileErrorCode.LOCALFILE_7,
    write_failed=LocalFileErrorCode.LOCALFILE_8,
    report_invalid=LocalFileErrorCode.LOCALFILE_9,
)


# --- Plugins ------------------------------------------------------


class LocalFileSrcPlugin(SrcPlugin[LocalFileSrcConn, LocalFileSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: LocalFileSrcConn,
        src: LocalFileSrc,
    ) -> list[list[TableFileInput]]:
        return to_table_file_inputs(
            _file_plugin.import_files(
                ctx=ctx,
                cfg=_file_plugin.ReaderConf.from_src(conn, src),
                build_location=build_location,
                errors=ERRORS,
            )
        )


class LocalFileDestPlugin(DestPlugin[LocalFileDestConn, LocalFileDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: LocalFileDestConn,
        dest: LocalFileDest,
        tables: list[TableFileSpec],
    ) -> None:
        _file_plugin.copy_out(
            ctx=ctx,
            cfg=_file_plugin.WriterConf.from_dest(conn, dest),
            tables=tables,
            build_location=build_location,
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

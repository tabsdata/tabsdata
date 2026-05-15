from __future__ import annotations

from typing import Any

from tabsdatak.conn.common.file import _plugin as _file_plugin, _transporter
from tabsdatak.conn.localfile import LocalFileConn, LocalFileDest, LocalFileSrc
from tabsdatak.conn.localfile.error import LocalFileErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcContext,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
    TableMode,
)

# --- Path resolution + location builder ----------------------------


# Only ``*`` is supported as a wildcard. Paths containing it are URI-
# composed manually because ``Path.as_uri`` would URL-encode ``*`` as
# ``%2A`` and defeat the transporter's glob expansion.


def build_location(
    conn: LocalFileConn, entry: str
) -> _transporter.TransporterLocalFile:
    resolved = "/" + conn.full_path(entry)
    if "*" in resolved:
        head, sep, basename = resolved.rpartition("/")
        head_uri = _transporter.path_to_file_uri(head + sep)
        return _transporter.TransporterLocalFile(head_uri + basename)
    return _transporter.TransporterLocalFile(_transporter.path_to_file_uri(resolved))


# --- Error-code bundle (no creds → no cred_resolution code) --------


ERRORS = _file_plugin.FilePluginErrors(
    binary_not_found=LocalFileErrorCode.LOCALFILE_6,
    read_failed=LocalFileErrorCode.LOCALFILE_7,
    write_failed=LocalFileErrorCode.LOCALFILE_8,
    report_invalid=LocalFileErrorCode.LOCALFILE_9,
)


# --- Plugins ------------------------------------------------------


class LocalFileSrcPlugin(SrcPlugin[LocalFileSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: LocalFileSrc,
    ) -> list[list[TableFileSpec]]:
        return _file_plugin.import_files(
            ctx=ctx,
            cfg=src.to_reader_conf(),
            build_location=build_location,
            errors=ERRORS,
        )


class LocalFileDestPlugin(DestPlugin[LocalFileDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: LocalFileDest,
        tables: list[TableFileSpec],
    ) -> None:
        _file_plugin.copy_out(
            ctx=ctx,
            cfg=dest.to_writer_conf(),
            tables=tables,
            build_location=build_location,
            errors=ERRORS,
        )


# --- Explorer placeholder ----------------------------------------


class LocalFileExplorer(Explorer[LocalFileConn]):
    def metadata(self, conn: LocalFileConn) -> Any:
        # TODO
        pass

    def data(self, conn: LocalFileConn, query: str) -> Any:
        # TODO
        pass


# --- Bindings ----------------------------------------------------


LOCALFILE_SRC = SrcDef(
    conn=LocalFileConn,
    src_type="localfile",
    src_version="v1",
    src=LocalFileSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.path),
    plugin=LocalFileSrcPlugin,
    explorer=LocalFileExplorer,
)


LOCALFILE_DEST = DestDef(
    conn=LocalFileConn,
    dest_type="localfile",
    dest_version="v1",
    dest=LocalFileDest,
    cardinality=lambda dest: len(dest.path),
    plugin=LocalFileDestPlugin,
    explorer=LocalFileExplorer,
)

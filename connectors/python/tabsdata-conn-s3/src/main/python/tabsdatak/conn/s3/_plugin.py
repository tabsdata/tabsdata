#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from tabsdatak.common._storage.transfer import (
    AWS_ACCESS_KEY_ID_OPTION,
    AWS_REGION_OPTION,
    AWS_SECRET_ACCESS_KEY_OPTION,
)
from tabsdatak.conn.common.file import _plugin, _source, _target
from tabsdatak.conn.common.file._store import FileStore, forwarded
from tabsdatak.conn.common.file._store_explorer import FileStoreExplorer

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.conn.s3.error import S3ErrorCode
from tabsdatak.error import ApiException
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

# Conn helpers operate on both Src and Dest variants.
_AnyS3Conn = S3SrcConn | S3DestConn

# --- Object store -------------------------------------------------


def file_store(conn: _AnyS3Conn) -> FileStore:
    prefix = conn._base_path().strip("/")
    root = f"s3://{conn._bucket()}/{prefix}" if prefix else f"s3://{conn._bucket()}"
    try:
        credentials = {
            AWS_ACCESS_KEY_ID_OPTION: _convert(conn.credentials.access_key_id, str),
            AWS_SECRET_ACCESS_KEY_OPTION: _convert(
                conn.credentials.secret_access_key, str
            ),
            AWS_REGION_OPTION: conn._region(),
        }
    except ApiException as e:
        raise ERRORS.cred_resolution.exception(cause=e, reason=str(e))
    return FileStore(root, {**credentials, **forwarded(root, conn.conn_cfg)})


# --- Explorer -----------------------------------------------------


class S3ConnectionExplorer(FileStoreExplorer[_AnyS3Conn]):
    """Verify connectivity to an S3 bucket and preview its objects."""

    def store(self, conn: _AnyS3Conn) -> FileStore:
        return file_store(conn)


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    read_failed=S3ErrorCode.S3_7,
    write_failed=S3ErrorCode.S3_8,
    cred_resolution=S3ErrorCode.S3_10,
)


# --- Plugins ------------------------------------------------------


class S3SrcPlugin(SrcPlugin[S3SrcConn, S3Src]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: S3SrcConn,
        src: S3Src,
    ) -> list[Iterable[LazyFrameInput]]:
        return _source.read_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(conn, src),
            store=file_store(conn),
            origin=lambda store, path: store.uri(path),
            errors=ERRORS,
        )


class S3DestPlugin(DestPlugin[S3DestConn, S3Dest, list[pl.LazyFrame | None]]):
    def write_out(
        self,
        ctx: DestContext,
        conn: S3DestConn,
        dest: S3Dest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        _target.write_files(
            ctx=ctx,
            cfg=_plugin.WriterConf.from_dest(conn, dest),
            store=file_store(conn),
            tables=tables,
            errors=ERRORS,
        )


# --- Bindings ----------------------------------------------------


S3_SRC = SrcDef(
    conn=S3SrcConn,
    type_="s3-file-in",
    system="AWS S3",
    src_version="v1",
    src=S3Src,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=S3SrcPlugin,
    explorer=S3ConnectionExplorer,
    icon="conn/s3/resources/s3.svg",
)


S3_DEST = DestDef(
    conn=S3DestConn,
    type_="s3-file-out",
    system="AWS S3",
    dest_version="v1",
    dest=S3Dest,
    cardinality=lambda dest: len(dest.paths),
    plugin=S3DestPlugin,
    explorer=S3ConnectionExplorer,
    icon="conn/s3/resources/s3.svg",
)

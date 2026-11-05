#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from tabsdatak.common._storage.transfer import GOOGLE_SERVICE_ACCOUNT_KEY_OPTION
from tabsdatak.conn.common.file import _plugin, _source, _target
from tabsdatak.conn.common.file._store import FileStore, forwarded

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import GCPJson, _convert
from tabsdatak.conn.gcs import GCSDest, GCSDestConn, GCSSrc, GCSSrcConn
from tabsdatak.conn.gcs._explorer import GCSConnectionExplorer
from tabsdatak.conn.gcs.error import GCSErrorCode
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
_AnyGCSConn = GCSSrcConn | GCSDestConn

# --- Object store -------------------------------------------------


def credentials(conn: _AnyGCSConn) -> GCPJson:
    creds = conn.credentials
    if not isinstance(creds, GCPJson):
        raise TypeError(
            f"GCSConn.credentials must be GCPJson; got "
            f"{type(creds).__name__}. Other GCPCredentials variants "
            f"are not yet supported."
        )
    return creds


def file_store(conn: _AnyGCSConn) -> FileStore:
    prefix = conn._base_path().strip("/")
    root = f"gs://{conn._bucket()}/{prefix}" if prefix else f"gs://{conn._bucket()}"
    try:
        key = _convert(credentials(conn).json, str)
    except ApiException as e:
        raise ERRORS.cred_resolution.exception(cause=e, reason=str(e))
    options = {GOOGLE_SERVICE_ACCOUNT_KEY_OPTION: key}
    return FileStore(root, {**options, **forwarded(root, conn.conn_cfg)})


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    binary_not_found=GCSErrorCode.GCS_6,
    read_failed=GCSErrorCode.GCS_7,
    write_failed=GCSErrorCode.GCS_8,
    report_invalid=GCSErrorCode.GCS_9,
    cred_resolution=GCSErrorCode.GCS_10,
)


# --- Plugins ------------------------------------------------------


class GCSSrcPlugin(SrcPlugin[GCSSrcConn, GCSSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: GCSSrcConn,
        src: GCSSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        return _source.read_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(conn, src),
            store=file_store(conn),
            origin=lambda store, path: store.uri(path),
            errors=ERRORS,
        )


class GCSDestPlugin(DestPlugin[GCSDestConn, GCSDest, list[pl.LazyFrame]]):
    def write_out(
        self,
        ctx: DestContext,
        conn: GCSDestConn,
        dest: GCSDest,
        tables: list[pl.LazyFrame],
    ) -> None:
        _target.write_files(
            ctx=ctx,
            cfg=_plugin.WriterConf.from_dest(conn, dest),
            store=file_store(conn),
            tables=tables,
            errors=ERRORS,
        )


# --- Bindings ----------------------------------------------------


GCS_SRC = SrcDef(
    conn=GCSSrcConn,
    type_="gcs-file-in",
    system="Google Storage",
    src_version="v1",
    src=GCSSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=GCSSrcPlugin,
    explorer=GCSConnectionExplorer,
    icon="conn/gcs/resources/gcs.svg",
)


GCS_DEST = DestDef(
    conn=GCSDestConn,
    type_="gcs-file-out",
    system="Google Storage",
    dest_version="v1",
    dest=GCSDest,
    cardinality=lambda dest: len(dest.paths),
    plugin=GCSDestPlugin,
    explorer=GCSConnectionExplorer,
    icon="conn/gcs/resources/gcs.svg",
)

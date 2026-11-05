#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from tabsdatak.common._storage.transfer import (
    AZURE_STORAGE_ACCOUNT_KEY_OPTION,
    AZURE_STORAGE_ACCOUNT_NAME_OPTION,
)
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode
from tabsdatak.conn.common.file import _plugin, _source, _target
from tabsdatak.conn.common.file._store import FileStore, forwarded

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import AzureAccountKey, _convert
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
_AnyAzureBlobConn = AzureBlobSrcConn | AzureBlobDestConn

# --- Object store -------------------------------------------------


def credentials(conn: _AnyAzureBlobConn) -> AzureAccountKey:
    creds = conn.credentials
    if not isinstance(creds, AzureAccountKey):
        raise TypeError(
            f"AzureBlob conn credentials must be AzureAccountKey; got "
            f"{type(creds).__name__}. Other AzureCredentials variants "
            f"are not yet supported."
        )
    return creds


def file_store(conn: _AnyAzureBlobConn) -> FileStore:
    container = conn._container()
    prefix = conn._base_path().strip("/")
    root = f"az://{container}/{prefix}" if prefix else f"az://{container}"
    try:
        account = credentials(conn)
        options = {
            AZURE_STORAGE_ACCOUNT_NAME_OPTION: _convert(account.account_name, str),
            AZURE_STORAGE_ACCOUNT_KEY_OPTION: _convert(account.account_key, str),
        }
    except ApiException as e:
        raise ERRORS.cred_resolution.exception(cause=e, reason=str(e))
    return FileStore(root, {**options, **forwarded(root, conn.conn_cfg)})


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    read_failed=AzureBlobErrorCode.AZURE_BLOB_7,
    write_failed=AzureBlobErrorCode.AZURE_BLOB_8,
    cred_resolution=AzureBlobErrorCode.AZURE_BLOB_10,
)


# --- Plugins ------------------------------------------------------


class AzureBlobSrcPlugin(SrcPlugin[AzureBlobSrcConn, AzureBlobSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: AzureBlobSrcConn,
        src: AzureBlobSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        return _source.read_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(conn, src),
            store=file_store(conn),
            origin=lambda store, path: store.uri(path),
            errors=ERRORS,
        )


class AzureBlobDestPlugin(
    DestPlugin[AzureBlobDestConn, AzureBlobDest, list[pl.LazyFrame]]
):
    def write_out(
        self,
        ctx: DestContext,
        conn: AzureBlobDestConn,
        dest: AzureBlobDest,
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


AZURE_BLOB_SRC = SrcDef(
    conn=AzureBlobSrcConn,
    type_="az-file-in",
    system="Azure Storage",
    src_version="v1",
    src=AzureBlobSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=AzureBlobSrcPlugin,
    explorer=AzureBlobConnectionExplorer,
    icon="conn/azureblob/resources/azure.svg",
)

AZURE_BLOB_DEST = DestDef(
    conn=AzureBlobDestConn,
    type_="az-file-out",
    system="Azure Storage",
    dest_version="v1",
    dest=AzureBlobDest,
    cardinality=lambda dest: len(dest.paths),
    plugin=AzureBlobDestPlugin,
    explorer=AzureBlobConnectionExplorer,
    icon="conn/azureblob/resources/azure.svg",
)

from __future__ import annotations

from typing import Any

from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode
from tabsdatak.conn.common.file import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import AzureAccountKey, _convert
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
_AnyAzureBlobConn = AzureBlobSrcConn | AzureBlobDestConn

# --- URI / location builder ---------------------------------------


def _az_uri(container: str, path_entry: str) -> str:
    if path_entry.startswith("az://") or path_entry.startswith("abfss://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"az://{container}/{key}"


def build_location(
    conn: _AnyAzureBlobConn, path_entry: str, handler: TransporterHandler
) -> Any:
    container = _convert(conn.container, str)
    if not (path_entry.startswith("az://") or path_entry.startswith("abfss://")):
        path_entry = conn.full_path(path_entry)
    return handler.azure_location(
        uri=_az_uri(container, path_entry),
        account_name_env=handler.azure_account_name_env,
        account_key_env=handler.azure_account_key_env,
    )


def credentials(conn: _AnyAzureBlobConn) -> AzureAccountKey:
    creds = conn.credentials
    if not isinstance(creds, AzureAccountKey):
        raise TypeError(
            f"AzureBlob conn credentials must be AzureAccountKey; got "
            f"{type(creds).__name__}. Other AzureCredentials variants "
            f"are not yet supported by the transporter binary."
        )
    return creds


def run_env(conn: _AnyAzureBlobConn, handler: TransporterHandler) -> dict[str, str]:
    c = credentials(conn)
    return {
        handler.azure_account_name_env: _convert(c.account_name, str),
        handler.azure_account_key_env: _convert(c.account_key, str),
    }


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    binary_not_found=AzureBlobErrorCode.AZURE_BLOB_6,
    read_failed=AzureBlobErrorCode.AZURE_BLOB_7,
    write_failed=AzureBlobErrorCode.AZURE_BLOB_8,
    report_invalid=AzureBlobErrorCode.AZURE_BLOB_9,
    cred_resolution=AzureBlobErrorCode.AZURE_BLOB_10,
)


# --- Plugins ------------------------------------------------------


class AzureBlobSrcPlugin(SrcPlugin[AzureBlobSrcConn, AzureBlobSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: AzureBlobSrcConn,
        src: AzureBlobSrc,
    ) -> list[list[TableFileInput]]:
        return to_table_file_inputs(
            _plugin.import_files(
                ctx=ctx,
                cfg=_plugin.ReaderConf.from_src(conn, src),
                build_location=build_location,
                run_env=run_env,
                errors=ERRORS,
            )
        )


class AzureBlobDestPlugin(DestPlugin[AzureBlobDestConn, AzureBlobDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: AzureBlobDestConn,
        dest: AzureBlobDest,
        tables: list[TableFileSpec],
    ) -> None:
        _plugin.copy_out(
            ctx=ctx,
            cfg=_plugin.WriterConf.from_dest(conn, dest),
            tables=tables,
            build_location=build_location,
            run_env=run_env,
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

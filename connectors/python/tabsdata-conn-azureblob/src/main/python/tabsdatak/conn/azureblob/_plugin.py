from __future__ import annotations

from typing import Any

from tabsdatak.conn.azureblob import (
    AzureBlobConn,
    AzureBlobDest,
    AzureBlobSrc,
)
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode
from tabsdatak.conn.common.file import _plugin, _transporter
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcPluginCtx,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
    TableMode,
)

# --- URI / location builder ---------------------------------------


def _az_uri(container: str, path_entry: str) -> str:
    if path_entry.startswith("az://") or path_entry.startswith("abfss://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"az://{container}/{key}"


def build_location(
    conn: AzureBlobConn, path_entry: str
) -> _transporter.TransporterAzure:
    container = (
        conn.container if isinstance(conn.container, str) else conn.container.value()
    )
    return _transporter.TransporterAzure(
        url=_az_uri(container, path_entry),
        account_name=_transporter.TransporterEnv(
            _transporter.SERVER_SIDE_AZURE_ACCOUNT_NAME
        ),
        account_key=_transporter.TransporterEnv(
            _transporter.SERVER_SIDE_AZURE_ACCOUNT_KEY
        ),
    )


def credentials(conn: AzureBlobConn) -> AzureAccountKey:
    creds = conn.credentials
    if not isinstance(creds, AzureAccountKey):
        raise TypeError(
            f"AzureBlobConn.credentials must be AzureAccountKey; got "
            f"{type(creds).__name__}. Other AzureCredentials variants "
            f"are not yet supported by the transporter binary."
        )
    return creds


def run_env(conn: AzureBlobConn) -> dict[str, str]:
    c = credentials(conn)
    return {
        _transporter.SERVER_SIDE_AZURE_ACCOUNT_NAME: c.account_name.value(),
        _transporter.SERVER_SIDE_AZURE_ACCOUNT_KEY: c.account_key.value(),
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


class AzureBlobSrcPlugin(SrcPlugin[AzureBlobSrc]):
    def read_in(self, ctx: SrcPluginCtx, src: AzureBlobSrc) -> list[list[TableFileSpec]]:
        return _plugin.import_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(src),
            build_location=build_location,
            run_env=run_env,
            errors=ERRORS,
        )


class AzureBlobDestPlugin(DestPlugin[AzureBlobDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: AzureBlobDest,
        tables: list[TableFileSpec],
    ) -> None:
        _plugin.copy_out(
            ctx=ctx,
            cfg=_plugin.WriterConf.from_dest(dest),
            tables=tables,
            build_location=build_location,
            run_env=run_env,
            errors=ERRORS,
        )


# --- Explorer placeholder ----------------------------------------


class AzureBlobExplorer(Explorer[AzureBlobConn]):
    def metadata(self, conn: AzureBlobConn) -> Any:
        # TODO
        pass

    def data(self, conn: AzureBlobConn, query: str) -> Any:
        # TODO
        pass


# --- Bindings ----------------------------------------------------


AZURE_BLOB_SRC = SrcDef(
    conn=AzureBlobConn,
    src_type="azureBlob",
    src_version="v1",
    src=AzureBlobSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=AzureBlobSrcPlugin,
    explorer=AzureBlobExplorer,
)

AZURE_BLOB_DEST = DestDef(
    conn=AzureBlobConn,
    dest_type="azureBlob",
    dest_version="v1",
    dest=AzureBlobDest,
    cardinality=lambda dest: len(dest.paths),
    plugin=AzureBlobDestPlugin,
    explorer=AzureBlobExplorer,
)

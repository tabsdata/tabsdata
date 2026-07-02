from __future__ import annotations

from typing import Any

from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.common.file import _plugin

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import GCPJson, _convert
from tabsdatak.conn.gcs import GCSDest, GCSDestConn, GCSSrc, GCSSrcConn
from tabsdatak.conn.gcs.error import GcsErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
    TableMode,
    to_table_file_inputs,
)

# Conn helpers operate on both Src and Dest variants.
_AnyGCSConn = GCSSrcConn | GCSDestConn

# --- URI / location builder ---------------------------------------


def _gcs_uri(bucket: str, path_entry: str) -> str:
    if path_entry.startswith("gs://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"gs://{bucket}/{key}"


def build_location(
    conn: _AnyGCSConn, path_entry: str, handler: TransporterHandler
) -> Any:
    bucket = _convert(conn.bucket, str)
    if not path_entry.startswith("gs://"):
        path_entry = conn.full_path(path_entry)
    return handler.gcs_location(
        uri=_gcs_uri(bucket, path_entry),
        service_account_key_env=handler.gcp_service_account_json_env,
    )


def credentials(conn: _AnyGCSConn) -> GCPJson:
    creds = conn.credentials
    if not isinstance(creds, GCPJson):
        raise TypeError(
            f"GCSConn.credentials must be GCPJson; got "
            f"{type(creds).__name__}. Other GCPCredentials variants "
            f"are not yet supported by the transporter binary."
        )
    return creds


def run_env(conn: _AnyGCSConn, handler: TransporterHandler) -> dict[str, str]:
    return {
        handler.gcp_service_account_json_env: _convert(credentials(conn).json, str),
    }


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    binary_not_found=GcsErrorCode.GCS_6,
    read_failed=GcsErrorCode.GCS_7,
    write_failed=GcsErrorCode.GCS_8,
    report_invalid=GcsErrorCode.GCS_9,
    cred_resolution=GcsErrorCode.GCS_10,
)


# --- Plugins ------------------------------------------------------


class GCSSrcPlugin(SrcPlugin[GCSSrcConn, GCSSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: GCSSrcConn,
        src: GCSSrc,
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


class GCSDestPlugin(DestPlugin[GCSDestConn, GCSDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: GCSDestConn,
        dest: GCSDest,
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


# --- Explorer placeholder ----------------------------------------


class GCSSrcExplorer(Explorer[GCSSrcConn]):
    def metadata(self, conn: GCSSrcConn) -> Any:
        # TODO
        pass

    def data(self, conn: GCSSrcConn, query: str) -> Any:
        # TODO
        pass


class GCSDestExplorer(Explorer[GCSDestConn]):
    def metadata(self, conn: GCSDestConn) -> Any:
        # TODO
        pass

    def data(self, conn: GCSDestConn, query: str) -> Any:
        # TODO
        pass


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
    explorer=GCSSrcExplorer,
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
    explorer=GCSDestExplorer,
    icon="conn/gcs/resources/gcs.svg",
)

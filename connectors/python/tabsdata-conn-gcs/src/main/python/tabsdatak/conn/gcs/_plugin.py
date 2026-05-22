from __future__ import annotations

from typing import Any

from tabsdatak.conn.common.file import _plugin, _transporter
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSConn, GCSDest, GCSSrc
from tabsdatak.conn.gcs.error import GcsErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileSpec,
    TableMode,
)

# --- URI / location builder ---------------------------------------


def _gcs_uri(bucket: str, path_entry: str) -> str:
    if path_entry.startswith("gs://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"gs://{bucket}/{key}"


def build_location(conn: GCSConn, path_entry: str) -> _transporter.TransporterGCS:
    bucket = conn.bucket if isinstance(conn.bucket, str) else conn.bucket.value()
    return _transporter.TransporterGCS(
        url=_gcs_uri(bucket, path_entry),
        service_account_key=_transporter.TransporterEnv(
            _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON
        ),
    )


def credentials(conn: GCSConn) -> GCPJson:
    creds = conn.credentials
    if not isinstance(creds, GCPJson):
        raise TypeError(
            f"GCSConn.credentials must be GCPJson; got "
            f"{type(creds).__name__}. Other GCPCredentials variants "
            f"are not yet supported by the transporter binary."
        )
    return creds


def run_env(conn: GCSConn) -> dict[str, str]:
    return {
        _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON: credentials(
            conn
        ).json.value(),
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


class GCSSrcPlugin(SrcPlugin[GCSSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        src: GCSSrc,
    ) -> list[list[TableFileSpec]]:
        return _plugin.import_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(src),
            build_location=build_location,
            run_env=run_env,
            errors=ERRORS,
        )


class GCSDestPlugin(DestPlugin[GCSDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: GCSDest,
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


class GCSExplorer(Explorer[GCSConn]):
    def metadata(self, conn: GCSConn) -> Any:
        # TODO
        pass

    def data(self, conn: GCSConn, query: str) -> Any:
        # TODO
        pass


# --- Bindings ----------------------------------------------------


GCS_SRC = SrcDef(
    conn=GCSConn,
    src_type="gcs",
    src_version="v1",
    src=GCSSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=GCSSrcPlugin,
    explorer=GCSExplorer,
)


GCS_DEST = DestDef(
    conn=GCSConn,
    dest_type="gcs",
    dest_version="v1",
    dest=GCSDest,
    cardinality=lambda dest: len(dest.paths),
    plugin=GCSDestPlugin,
    explorer=GCSExplorer,
)

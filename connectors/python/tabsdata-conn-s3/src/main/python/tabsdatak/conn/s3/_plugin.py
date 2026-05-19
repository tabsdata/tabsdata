from __future__ import annotations

from typing import Any

from tabsdatak.conn.common.file import _plugin, _transporter
from tabsdatak.conn.s3 import S3Conn, S3Dest, S3Src
from tabsdatak.conn.s3.error import S3ErrorCode
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

# --- URI / location builder ---------------------------------------


def _s3_uri(bucket: str, path_entry: str) -> str:
    if path_entry.startswith("s3://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"s3://{bucket}/{key}"


def _resolve(value) -> str:
    """``S3Conn`` fields accept ``str | Secret | Env``; ``str`` values
    are stored verbatim, Secret/Env resolve through ``.value()``."""
    return value if isinstance(value, str) else value.value()


def build_location(conn: S3Conn, path_entry: str) -> _transporter.TransporterS3:
    return _transporter.TransporterS3(
        url=_s3_uri(_resolve(conn.bucket), path_entry),
        access_key=_transporter.TransporterEnv(
            _transporter.SERVER_SIDE_AWS_ACCESS_KEY_ID
        ),
        secret_key=_transporter.TransporterEnv(
            _transporter.SERVER_SIDE_AWS_SECRET_ACCESS_KEY
        ),
        region=_transporter.TransporterEnv(_transporter.SERVER_SIDE_AWS_REGION),
    )


def run_env(conn: S3Conn) -> dict[str, str]:
    creds = conn.credentials
    return {
        _transporter.SERVER_SIDE_AWS_ACCESS_KEY_ID: _resolve(creds.access_key_id),
        _transporter.SERVER_SIDE_AWS_SECRET_ACCESS_KEY: _resolve(
            creds.secret_access_key
        ),
        _transporter.SERVER_SIDE_AWS_REGION: _resolve(conn.region),
    }


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    binary_not_found=S3ErrorCode.S3_6,
    read_failed=S3ErrorCode.S3_7,
    write_failed=S3ErrorCode.S3_8,
    report_invalid=S3ErrorCode.S3_9,
    cred_resolution=S3ErrorCode.S3_10,
)


# --- Plugins ------------------------------------------------------


class S3SrcPlugin(SrcPlugin[S3Src]):
    def read_in(
        self,
        ctx: SrcContext,
        src: S3Src,
    ) -> list[list[TableFileSpec]]:
        return _plugin.import_files(
            ctx=ctx,
            cfg=_plugin.ReaderConf.from_src(src),
            build_location=build_location,
            run_env=run_env,
            errors=ERRORS,
        )


class S3DestPlugin(DestPlugin[S3Dest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: S3Dest,
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


class S3Explorer(Explorer[S3Conn]):
    def metadata(self, conn: S3Conn) -> Any:
        # TODO
        pass

    def data(self, conn: S3Conn, query: str) -> Any:
        # TODO
        pass


# --- Bindings ----------------------------------------------------


S3_SRC = SrcDef(
    conn=S3Conn,
    src_type="s3",
    src_version="v1",
    src=S3Src,
    table_mode=TableMode.ARRAY,
    cardinality=lambda src: len(src.paths),
    plugin=S3SrcPlugin,
    explorer=S3Explorer,
)


S3_DEST = DestDef(
    conn=S3Conn,
    dest_type="s3",
    dest_version="v1",
    dest=S3Dest,
    cardinality=lambda dest: len(dest.paths),
    plugin=S3DestPlugin,
    explorer=S3Explorer,
)

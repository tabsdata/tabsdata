#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from tabsdatak.common._storage.transfer import (
    AWS_ACCESS_KEY_ID_OPTION,
    AWS_REGION_OPTION,
    AWS_SECRET_ACCESS_KEY_OPTION,
)
from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.common.file import _plugin, _source
from tabsdatak.conn.common.file._store import FileStore, forwarded

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert, cloud_full_path
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.conn.s3._explorer import S3ConnectionExplorer
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
    TableFileSpec,
    TableMode,
)

# Conn helpers operate on both Src and Dest variants.
_AnyS3Conn = S3SrcConn | S3DestConn

# --- URI / location builder ---------------------------------------


def _s3_uri(bucket: str, path_entry: str) -> str:
    if path_entry.startswith("s3://"):
        return path_entry
    key = path_entry.lstrip("/")
    return f"s3://{bucket}/{key}"


def build_location(
    conn: _AnyS3Conn, path_entry: str, handler: TransporterHandler
) -> Any:
    if not path_entry.startswith("s3://"):
        path_entry = cloud_full_path(conn._base_path(), path_entry)
    return handler.s3_location(
        uri=_s3_uri(conn._bucket(), path_entry),
        access_key_env=handler.aws_access_key_id_env,
        secret_key_env=handler.aws_secret_access_key_env,
        region_env=handler.aws_region_env,
    )


def run_env(conn: _AnyS3Conn, handler: TransporterHandler) -> dict[str, str]:
    creds = conn.credentials
    return {
        handler.aws_access_key_id_env: _convert(creds.access_key_id, str),
        handler.aws_secret_access_key_env: _convert(creds.secret_access_key, str),
        handler.aws_region_env: conn._region(),
    }


def file_store(conn: S3SrcConn) -> FileStore:
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


# --- Error-code bundle --------------------------------------------


ERRORS = _plugin.FilePluginErrors(
    binary_not_found=S3ErrorCode.S3_6,
    read_failed=S3ErrorCode.S3_7,
    write_failed=S3ErrorCode.S3_8,
    report_invalid=S3ErrorCode.S3_9,
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


class S3DestPlugin(DestPlugin[S3DestConn, S3Dest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: S3DestConn,
        dest: S3Dest,
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

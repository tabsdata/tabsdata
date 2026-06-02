"""S3 connector — public API.

* :class:`S3SrcConn` / :class:`S3DestConn` -- per-S3 connection (bucket, region,
  credentials (AwsAccessSecretKey), base_path, conn_cfg).
* :class:`S3Src` -- source: list of `paths` (glob-style);
  one parquet slot per path.
* :class:`S3Dest` -- destination: list of `paths`; one
  parquet slot per path. Tokens like ``${date}`` and ``${trx_id}``
  in dest paths are resolved at write time.

File transport goes through the shared ``td_transporter`` binary.
String, ``Var``, and ``Secret`` are accepted on credential and
identifier fields.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Conn,
    Dest,
    Src,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    DestFormatCfgSpec,
    DestPathsSpec,
    FileInfo,
    SrcFileFormat,
    SrcFormatCfgSpec,
    SrcPathsSpec,
)
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    BasePathSpec,
    CfgSpec,
    S3BucketSpec,
    full_path,
)
from tabsdatak.conn.s3.error import S3ErrorCode

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = AwsAccessSecretKey


# --- S3Src.src_cfg keys ---------------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- S3Dest.dest_cfg keys -------------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_1, kw_only=True)
class S3SrcConn(Conn):
    """S3 connection used by :class:`S3Src`.

    Holds the bucket / region / credentials / base_path. The src
    class wraps this conn and adds the per-flow ``paths`` list.
    """

    __kind__ = "s3SrcConn"

    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: CredentialsSpec
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        """Resolve ``path`` against ``base_path`` and return the
        joined result."""
        return full_path(self.base_path, path)


@_api.dataclass(S3ErrorCode.S3_1, kw_only=True)
class S3DestConn(Conn):
    """S3 connection used by :class:`S3Dest`.

    Attribute-identical to :class:`S3SrcConn` but a distinct class
    so source and destination connections cannot be confused at
    the wire-format level.
    """

    __kind__ = "s3DestConn"

    bucket: S3BucketSpec
    region: AwsRegionSpec
    credentials: CredentialsSpec
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


# --- Source ----------------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_2, kw_only=True)
class S3Src(Src):
    """S3 source — one parquet slot per path.

    :param paths: List of file paths (or glob patterns) relative
        to the conn's root. One parquet slot per entry.
    :param format: Optional file-format override (default AUTO).
    :param initial_last_modified: Optional cutoff for incremental
        ingestion. On the first run, the connector imports only
        files modified at or after this timestamp; on each
        subsequent run, the cutoff advances automatically so that
        only files newer than the previous run are re-imported.
    :param src_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    paths: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    file_info: FileInfo | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise S3ErrorCode.S3_3.exception(cause=e, side="src")


# --- Destination -----------------------------------------------------


@_api.dataclass(S3ErrorCode.S3_4, kw_only=True)
class S3Dest(Dest):
    """S3 destination — write one parquet slot per path.

    :param paths: List of target file paths relative to the
        conn's root. One parquet slot per entry. Tokens like
        ``${date}`` / ``${trx_id}`` are resolved at write time.
    :param format: Optional output-format override (default AUTO).
    :param dest_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise S3ErrorCode.S3_5.exception(cause=e, side="dest")

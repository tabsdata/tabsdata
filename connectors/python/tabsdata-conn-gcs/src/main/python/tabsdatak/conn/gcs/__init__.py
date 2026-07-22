#
# Copyright 2026 Tabsdata Inc.
#

"""GCS connector — public API.

* :class:`GCSSrcConn` / :class:`GCSDestConn` -- per-GCS connection
  (bucket, credentials (GCPJson), project, base_path, conn_cfg).
* :class:`GCSSrc` -- source: list of `paths` (glob-style);
  one parquet slot per path.
* :class:`GCSDest` -- destination: list of `paths`; one
  parquet slot per path. The tokens ``${EXEC_PLAN_ID}``, ``${TRX_ID}``
  and ``${EXEC_PLAN_TS}`` in dest paths are resolved at write time.

File transport goes through the shared ``td_transporter`` binary.
String and ``Secret`` are accepted on credential and
identifier fields.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Dest,
    Src,
    StrOrSecretSpec,
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
    CfgSpec,
    FullPathSpec,
    GCPJson,
    GCSBucketSpec,
    StrOrSecretConstraints,
    _convert,
)
from tabsdatak.conn.gcs.error import GcsErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

# GCS currently supports a single credential shape -- a GCP
# service-account JSON. Adding a new credential type later (e.g.
# Workload Identity) means widening this alias to
# ``Union[GCPJson, NewCredentials]`` without touching ``GCSConn``'s
# signature.
CredentialsSpec: TypeAlias = GCPJson


# GCP project id: no regex shape; permissive ``.+`` enables the
# bare-string shortcut at the field site.
ProjectSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=r".+")
]


# --- GCSSrc.src_cfg keys --------------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- GCSDest.dest_cfg keys ------------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_1, kw_only=True)
class GCSSrcConn(Conn):
    """GCS connection used by :class:`GCSSrc`.

    :param bucket: GCS bucket name (e.g. ``"my-data-bucket"``).
    :param credentials: :class:`GCPJson` service-account credentials.
    :param project: Optional GCP project id.
    :param base_path: Full path (object-key prefix) from the bucket
        root: starts with ``/``, no trailing ``/``. The leading ``/`` is
        trimmed when forming the object key. Defaults to ``/``.
    :param conn_cfg: Optional driver-side cfg dict.
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: FullPathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


@_api.dataclass(GcsErrorCode.GCS_1, kw_only=True)
class GCSDestConn(Conn):
    """GCS connection used by :class:`GCSDest`.

    Attribute-identical to :class:`GCSSrcConn` but a distinct
    class so source and destination connections cannot be
    confused at the wire-format level.
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: FullPathSpec = "/"
    conn_cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _project(self) -> str | None:
        return _convert(self.project, str) if self.project is not None else None

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Source ----------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_2, kw_only=True)
class GCSSrc(Src):
    """GCS source — one parquet slot per path.

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
            raise GcsErrorCode.GCS_3.exception(cause=e, side="src")


# --- Destination -----------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_4, kw_only=True)
class GCSDest(Dest):
    """GCS destination — write one parquet slot per path.

    :param paths: List of target file paths relative to the
        conn's root. One parquet slot per entry. The tokens
        ``${EXEC_PLAN_ID}``, ``${TRX_ID}`` and ``${EXEC_PLAN_TS}``
        are resolved at write time.
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
            raise GcsErrorCode.GCS_5.exception(cause=e, side="dest")

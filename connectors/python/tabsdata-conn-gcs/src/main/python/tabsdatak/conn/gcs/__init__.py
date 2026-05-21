"""GCS connector — public API.

* :class:`GCSConn` -- per-GCS connection (bucket, credentials (GCPJson),
  project, base_path, conn_cfg).
* :class:`GCSSrc` -- source: list of `paths` (glob-style);
  one parquet slot per path.
* :class:`GCSDest` -- destination: list of `paths`; one
  parquet slot per path. Tokens like ``${date}`` and ``${trx_id}``
  in dest paths are resolved at write time.

File transport goes through the shared ``td_transporter`` binary.
String, ``Env``, and ``Secret`` are accepted on credential and
identifier fields.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Conn,
    Dest,
    Env,
    NamedConn,
    Secret,
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
    BasePathSpec,
    CfgSpec,
    GCPJson,
    GCSBucketSpec,
    full_path,
)
from tabsdatak.conn.gcs.error import GcsErrorCode

# --- Type aliases ----------------------------------------------------

# GCS currently supports a single credential shape -- a GCP
# service-account JSON. Adding a new credential type later (e.g.
# Workload Identity) means widening this alias to
# ``Union[GCPJson, NewCredentials]`` without touching ``GCSConn``'s
# signature.
CredentialsSpec: TypeAlias = GCPJson


# GCP project id: no regex shape; permissive ``.+`` enables the
# bare-string shortcut at the field site.
ProjectSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")],
    Annotated[Env, _api.EnvValueConstraints(pattern=r".+")],
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
class GCSConn(Conn):
    """Google Cloud Storage connection.

    :param bucket: GCS bucket name (e.g. ``"my-data-bucket"``).
    :param credentials: GCP service-account credentials. Either
        :class:`GCPJson` (service-account JSON, native API path,
        recommended) or HMAC keys for the S3-compatible endpoint.
        ``Env`` / ``Secret`` references resolved at session open.
    :param project: Optional GCP project id; required only when the
        credentials don't carry an embedded project (HMAC keys), or
        when you want to bill / quota a different project from the
        one the service account belongs to.
    :param base_path: Optional object-key prefix under the bucket
        (e.g. ``"datalake/raw/"``); src/dest ``paths`` resolve
        against it. Defaults to bucket root.
    :param conn_cfg: Optional driver-side cfg dict forwarded to
        the underlying GCS client.
    """

    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        """Resolve ``path`` against ``base_path`` and return the
        joined result.

        ``path`` is always treated as relative to ``base_path``,
        even when it starts with ``/`` -- the leading slash is
        stripped before joining, so ``"/foo"`` and ``"foo"``
        produce identical output. Trailing slashes on ``path`` /
        ``base_path`` and on the joined result are also stripped,
        so the return value never starts or ends with ``/``.
        """
        return full_path(self.base_path, path)


ConnSpec: TypeAlias = Union[NamedConn, GCSConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_2, kw_only=True)
class GCSSrc(Src):
    """GCS source — one parquet slot per path.

    :param conn: :class:`GCSConn` (or :class:`NamedConn`).
    :param paths: List of file paths (or glob patterns) relative
        to the conn's root. One parquet slot per entry.
    :param format: Optional file-format override (default AUTO).
    :param initial_last_modified: Optional cutoff; the runtime
        re-imports only files newer than this.
    :param src_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    conn: ConnSpec
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

    :param conn: :class:`GCSConn` (or :class:`NamedConn`).
    :param paths: List of target file paths relative to the
        conn's root. One parquet slot per entry. Tokens like
        ``${date}`` / ``${trx_id}`` are resolved at write time.
    :param format: Optional output-format override (default AUTO).
    :param dest_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    conn: ConnSpec
    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise GcsErrorCode.GCS_5.exception(cause=e, side="dest")

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
    bucket: GCSBucketSpec
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


ConnSpec: TypeAlias = Union[NamedConn, GCSConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(GcsErrorCode.GCS_2, kw_only=True)
class GCSSrc(Src):
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

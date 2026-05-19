from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Conn,
    Dest,
    NamedConn,
    Src,
)
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode

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
    AzContainerSpec,
    AzureAccountKey,
    BasePathSpec,
    CfgSpec,
    full_path,
)

# --- Type aliases ----------------------------------------------------

# AzureBlob currently supports a single credential shape -- account
# name + account key. Adding a new credential type later means
# widening this alias to ``Union[AzureAccountKey, NewCredentials]``
# without touching ``AzureBlobConn``'s signature.
CredentialsSpec: TypeAlias = AzureAccountKey


# --- AzureBlobSrc.src_cfg keys --------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- AzureBlobDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_1, kw_only=True)
class AzureBlobConn(Conn):
    container: AzContainerSpec
    credentials: CredentialsSpec
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


ConnSpec: TypeAlias = Union[NamedConn, AzureBlobConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_2, kw_only=True)
class AzureBlobSrc(Src):
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
            raise AzureBlobErrorCode.AZURE_BLOB_3.exception(cause=e)


# --- Destination -----------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_4, kw_only=True)
class AzureBlobDest(Dest):
    conn: ConnSpec
    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_5.exception(cause=e)

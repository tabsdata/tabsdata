"""Azure Blob connector — public API.

* :class:`AzureBlobSrcConn` / :class:`AzureBlobDestConn` -- per-Azure Blob connection (container,
  credentials (AzureAccountKey), base_path, conn_cfg).
* :class:`AzureBlobSrc` -- source: list of `paths` (glob-style);
  one parquet slot per path.
* :class:`AzureBlobDest` -- destination: list of `paths`; one
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
class AzureBlobSrcConn(Conn):
    """Azure Blob Storage connection used by :class:`AzureBlobSrc`.

    :param container: Azure Blob container name.
    :param credentials: Storage-account key credentials.
    :param base_path: Optional path prefix under the container.
    :param conn_cfg: Optional driver-side cfg dict.
    """

    __kind__ = "azureBlobSrcConn"

    container: AzContainerSpec
    credentials: CredentialsSpec
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_1, kw_only=True)
class AzureBlobDestConn(Conn):
    """Azure Blob Storage connection used by :class:`AzureBlobDest`.

    Attribute-identical to :class:`AzureBlobSrcConn` but a distinct
    class so source and destination connections cannot be confused
    at the wire-format level.
    """

    __kind__ = "azureBlobDestConn"

    container: AzContainerSpec
    credentials: CredentialsSpec
    base_path: BasePathSpec = ""
    conn_cfg: CfgSpec | None = None

    def full_path(self, path: str) -> str:
        return full_path(self.base_path, path)


SrcConnSpec: TypeAlias = Union[NamedConn, AzureBlobSrcConn]
DestConnSpec: TypeAlias = Union[NamedConn, AzureBlobDestConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_2, kw_only=True)
class AzureBlobSrc(Src):
    """Azure Blob source — one parquet slot per path.

    :param conn: :class:`AzureBlobSrcConn` (or :class:`NamedConn`).
    :param paths: List of file paths (or glob patterns) relative
        to the conn's root. One parquet slot per entry.
    :param format: Optional file-format override (default AUTO).
    :param initial_last_modified: Optional cutoff; the runtime
        re-imports only files newer than this.
    :param src_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    conn: SrcConnSpec
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
    """Azure Blob destination — write one parquet slot per path.

    :param conn: :class:`AzureBlobDestConn` (or :class:`NamedConn`).
    :param paths: List of target file paths relative to the
        conn's root. One parquet slot per entry. Tokens like
        ``${date}`` / ``${trx_id}`` are resolved at write time.
    :param format: Optional output-format override (default AUTO).
    :param dest_cfg: Optional ``Mapping`` over
        ``{"tabsdata.file.chunkSize"}``.
    """

    conn: DestConnSpec
    paths: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_5.exception(cause=e)

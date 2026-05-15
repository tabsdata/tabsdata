from __future__ import annotations

from datetime import datetime
from typing import TypeAlias, Union

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
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin, _validation
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
    AzureCredentials,
    BasePathSpec,
    CfgSpec,
    full_path,
)

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = AzureCredentials


# --- Connection ------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_1, kw_only=True)
class AzureBlobConn(Conn):
    container: AzContainerSpec
    credentials: CredentialsSpec
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


ConnSpec: TypeAlias = Union[NamedConn, AzureBlobConn]


# --- Source ----------------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_2, kw_only=True)
class AzureBlobSrc(Src):
    conn: ConnSpec
    path: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    file_info: FileInfo | None = None
    # TODO: document additional valid keys for this cfg in the
    # docstring above (see MySQLSrc src_cfg for the pattern).
    src_cfg: CfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_3.exception(cause=e)

    def to_reader_conf(self) -> _file_plugin.ReaderConf:
        return _file_plugin.ReaderConf.from_src(self)


# --- Destination -----------------------------------------------------


@_api.dataclass(AzureBlobErrorCode.AZURE_BLOB_4, kw_only=True)
class AzureBlobDest(Dest):
    conn: ConnSpec
    path: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    # TODO: document additional valid keys for this cfg in the
    # docstring above (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None

    def validate(self) -> None:
        try:
            _validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise AzureBlobErrorCode.AZURE_BLOB_5.exception(cause=e)

    def to_writer_conf(self) -> _file_plugin.WriterConf:
        return _file_plugin.WriterConf.from_dest(self)

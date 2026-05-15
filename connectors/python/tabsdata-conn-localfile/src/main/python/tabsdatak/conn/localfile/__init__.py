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

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import (
    _plugin as _file_plugin,
    _validation as _file_validation,
)
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    DestFormatCfgSpec,
    DestPathsSpec,
    FileInfo,
    SrcFileFormat,
    SrcFormatCfgSpec,
    SrcPathsSpec,
)
from tabsdatak.conn.common.types import BasePathSpec, CfgSpec, full_path
from tabsdatak.conn.localfile.error import LocalFileErrorCode

# --- Connection -----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_1, kw_only=True)
class LocalFileConn(Conn):
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


ConnSpec: TypeAlias = Union[NamedConn, LocalFileConn]


# --- Source ---------------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_2, kw_only=True)
class LocalFileSrc(Src):
    conn: ConnSpec
    path: SrcPathsSpec
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: SrcFormatCfgSpec | None = None
    initial_last_modified: datetime | None = None
    file_info: FileInfo | None = None
    src_cfg: CfgSpec | None = None

    def validate(self) -> None:
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_3.exception(cause=e)

    def to_reader_conf(self) -> _file_plugin.ReaderConf:
        return _file_plugin.ReaderConf.from_src(self)


# --- Destination ----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_4, kw_only=True)
class LocalFileDest(Dest):
    conn: ConnSpec
    path: DestPathsSpec
    format: DestFileFormat = DestFileFormat.AUTO
    format_cfg: DestFormatCfgSpec | None = None
    dest_cfg: CfgSpec | None = None

    def validate(self) -> None:
        try:
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_5.exception(cause=e)

    def to_writer_conf(self) -> _file_plugin.WriterConf:
        return _file_plugin.WriterConf.from_dest(self)

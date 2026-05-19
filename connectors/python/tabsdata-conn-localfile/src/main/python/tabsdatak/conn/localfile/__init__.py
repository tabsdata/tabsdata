"""LocalFile connector — public API.

* :class:`LocalFileConn` -- per-LocalFile connection (base_path, conn_cfg).
* :class:`LocalFileSrc` -- source: list of `paths` (glob-style);
  one parquet slot per path.
* :class:`LocalFileDest` -- destination: list of `paths`; one
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

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation as _file_validation
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


# --- LocalFileSrc.src_cfg keys --------------------------------------
SrcCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- LocalFileDest.dest_cfg keys ------------------------------------
DestCfgKey: TypeAlias = Literal["tabsdata.file.chunkSize"]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection -----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_1, kw_only=True)
class LocalFileConn(Conn):
    """LocalFile connection.

    Holds the bucket/container/region or base_path and the
    credentials. The src/dest classes wrap this conn and add
    the per-flow ``paths`` list. See module docstring for the
    full field set.
    """

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
    """LocalFile source — one parquet slot per path.

    :param conn: :class:`LocalFileConn` (or :class:`NamedConn`).
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
            _file_validation.validate_format_cfg(self.format_cfg, side="src")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_3.exception(cause=e)


# --- Destination ----------------------------------------------------


@_api.dataclass(LocalFileErrorCode.LOCALFILE_4, kw_only=True)
class LocalFileDest(Dest):
    """LocalFile destination — write one parquet slot per path.

    :param conn: :class:`LocalFileConn` (or :class:`NamedConn`).
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
            _file_validation.validate_format_cfg(self.format_cfg, side="dest")
        except Exception as e:
            raise LocalFileErrorCode.LOCALFILE_5.exception(cause=e)

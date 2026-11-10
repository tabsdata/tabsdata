#
# Copyright 2026 Tabsdata Inc.
#

from collections.abc import Callable
from dataclasses import field

import tabsdatak._api as _api
from tabsdatak.conn.common._explorer import BaseConnMetaModel, Column
from tabsdatak.conn.common.file._read._blocks import Seekable
from tabsdatak.conn.common.file._read._format import Format
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import (
    Entity,
)


@_api.dataclass(FileCommonErrorCode.FILE_COMMON_19)
class FileStorageConnMetaModel(BaseConnMetaModel):
    """File/object storage: bucket → prefix → file, joined by ``/``."""

    type: str = "filestorage"
    description: str = (
        "File and object storage systems with"
        " directory structures, files, and file types"
    )
    hierarchy: list[str] = field(default_factory=lambda: ["bucket", "prefix", "file"])
    entity_level: str = "prefix"
    entity_fully_qualified_name: list[str] = field(
        default_factory=lambda: ["scheme", "bucket", "prefix", "file"]
    )

    def entity_fqn(self, entity: dict) -> str:
        # ``scheme://bucket/prefix/file`` -- the scheme joins with
        # ``://``; the remaining path parts join with ``/``. Empty parts
        # are dropped so no stray separators appear.
        scheme = entity.get("scheme", "")
        path = "/".join(
            entity.get(k, "")
            for k in self.entity_fully_qualified_name
            if k != "scheme" and entity.get(k, "")
        )
        return f"{scheme}://{path}" if scheme else path


@_api.dataclass(FileCommonErrorCode.FILE_COMMON_21)
class FilePattern:
    """A filename glob and its format inside a file-storage entity."""

    pattern: str
    format: str

    def to_dict(self) -> dict:
        """Return a compact dict representation."""
        return {"pattern": self.pattern, "format": self.format}

    @classmethod
    def from_dict(cls, data: dict) -> "FilePattern":
        """Rebuild a :class:`FilePattern` from its :meth:`to_dict` output."""
        return cls(pattern=data.get("pattern", ""), format=data.get("format", ""))


@_api.dataclass(FileCommonErrorCode.FILE_COMMON_20)
class FileStorageEntity(Entity):
    """A single file inside a file- or object-storage system.

    ``columns`` is the file's schema, read at metadata time; when it was read
    from another file of the same prefix and format, ``schema_from`` names
    that file.
    """

    scheme: str = ""
    bucket: str = ""
    prefix: str = ""
    file: FilePattern | None = None
    size: int | None = None  # object size in bytes
    last_modified: str | None = None  # ISO-8601 last-modified timestamp
    columns: list[Column] = field(default_factory=list)
    schema_from: str = ""

    def fqn(self) -> str:
        return self.connection_type.entity_fqn({
            "scheme": self.scheme,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "file": self.file.pattern if self.file is not None else "",
        })

    def object_key(self) -> str:
        """``<prefix>/<file>``, or the file alone when there is no prefix."""
        return (
            f"{self.prefix}/{self.file.pattern}" if self.prefix else self.file.pattern
        )

    def field_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict:
        d = {}
        if self.scheme:
            d["scheme"] = self.scheme
        if self.bucket:
            d["bucket"] = self.bucket
        if self.prefix:
            d["prefix"] = self.prefix
        d["fqn"] = self.fqn()
        if self.file is not None:
            d["file"] = self.file.to_dict()
        if self.size is not None:
            d["size"] = self.size
        if self.last_modified is not None:
            d["last_modified"] = self.last_modified
        if self.columns:
            d["columns"] = [c.to_dict() for c in self.columns]
        if self.schema_from:
            d["schema_from"] = self.schema_from
        return d

    @classmethod
    def from_dict(
        cls, data: dict, connection_type: "BaseConnMetaModel"
    ) -> "FileStorageEntity":
        file = data.get("file")
        return cls(
            connection_type=connection_type,
            bucket=data.get("bucket", ""),
            prefix=data.get("prefix", ""),
            scheme=data.get("scheme", ""),
            file=FilePattern.from_dict(file) if file else None,
            size=data.get("size"),
            last_modified=data.get("last_modified"),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
            schema_from=data.get("schema_from", ""),
        )


_PREVIEW_FORMATS = {
    "avro": Format.AVRO,
    "json": Format.NDJSON,
    "jsonl": Format.NDJSON,
    "ndjson": Format.NDJSON,
    "parquet": Format.PARQUET,
    "xlsx": Format.EXCEL,
}


def preview_format(entity: FileStorageEntity) -> Format:
    """How ``data`` previews the entity's file: by its extension, and as CSV
    for any extension without a reader of its own (csv, log, ...)."""
    return _PREVIEW_FORMATS.get((entity.file.format or "").lower(), Format.CSV)


def read_excel_rows(open_: Callable[[], Seekable], limit: int) -> list[dict]:
    from tabsdatak.conn.common.file._read._excel import Excel

    frame = Excel(batch_rows=max(limit, 1)).read(open_).frame
    return frame.head(limit).collect().to_dicts()

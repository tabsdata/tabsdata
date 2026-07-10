#
# Copyright (c) 2026 Tabsdata Inc.
#

from dataclasses import field

import tabsdatak._api as _api
from tabsdatak.conn.common._explorer import BaseConnMetaModel, Column
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import (
    Conn,
    Entity,
    Explorer,
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
    """A single file inside a file- or object-storage system."""

    scheme: str = ""
    bucket: str = ""
    prefix: str = ""
    file: FilePattern | None = None

    def fqn(self) -> str:
        return self.connection_type.entity_fqn(
            {
                "scheme": self.scheme,
                "bucket": self.bucket,
                "prefix": self.prefix,
                "file": self.file.pattern if self.file is not None else "",
            }
        )

    def field_names(self) -> list[str]:
        return [self.file.pattern] if self.file is not None else []

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
        )

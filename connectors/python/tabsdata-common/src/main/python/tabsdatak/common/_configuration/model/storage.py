#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pydantic import Field

from tabsdatak.common._configuration.document.loader import DocumentModel
from tabsdatak.common._storage.mounts import MountError, NamedMountDeclaration, text

Scalar = str | bool | int | float


class Mount(DocumentModel):
    id: str
    path: str | None = None
    uri: str
    options: dict[str, Scalar] = Field(default_factory=dict)

    def declaration(self) -> NamedMountDeclaration:
        return NamedMountDeclaration(
            id=self.id,
            uri=self.uri,
            options={
                key: text(value) for key, value in self.options.items() if text(value)
            },
        )


class Storage(DocumentModel):
    mounts: list[Mount] = Field(default_factory=list)


class StorageConfiguration(DocumentModel):
    storage: Storage

    def mount(self, mount: str) -> Mount:
        for declared in self.storage.mounts:
            if declared.id == mount:
                return declared
        raise MountError(f"Mount {mount} is not declared in the storage configuration")

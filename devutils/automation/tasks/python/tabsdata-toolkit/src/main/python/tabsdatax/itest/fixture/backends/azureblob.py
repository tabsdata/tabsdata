#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``azureblob`` backend."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, StorageOptions, System, require


@dataclass(frozen=True)
class AzureblobSystem(System):
    system_type: ClassVar[str] = "azureblob"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    container: str
    account_name: str
    account_key: str
    location: str
    storage_options: StorageOptions | None = None  # absent from an older descriptor


@dataclass(frozen=True)
class AzureblobDomain(Domain):
    system_type: ClassVar[str] = "azureblob"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    base_path: PurePosixPath
    prefix: str  # the domain's object-store prefix (== the entity container)
    uri: str
    container: str
    account_name: str
    account_key: str
    storage_options: StorageOptions | None = None  # absent from an older descriptor
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def azureblob_system(name: str):
    return require(name, AzureblobSystem)


def azureblob_domain(name: str):
    return require(name, AzureblobDomain)

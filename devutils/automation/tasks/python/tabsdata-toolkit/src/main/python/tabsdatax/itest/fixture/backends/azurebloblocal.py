#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``azurebloblocal`` backend: the local twin of
``azureblob``, a container on this machine. Its descriptor carries the endpoint and
the polars / obstore ``storage_options``, so a test reads it exactly as it reads
``azureblob``."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from tabsdatax.itest.fixture.backend import Domain, StorageOptions, System, require


@dataclass(frozen=True)
class AzureBlobLocalSystem(System):
    system_type: ClassVar[str] = "azurebloblocal"
    name: str
    system_kind: str
    docker_container_name: str
    image: str
    host: str
    port: str
    endpoint: str
    container: str
    account_name: str
    account_key: str
    location: str
    storage_options: StorageOptions


@dataclass(frozen=True)
class AzureBlobLocalDomain(Domain):
    system_type: ClassVar[str] = "azurebloblocal"
    name: str
    system_kind: str
    base_path: PurePosixPath
    prefix: str  # the domain's object-store prefix (== the entity container)
    uri: str
    endpoint: str
    container: str
    account_name: str
    account_key: str
    storage_options: StorageOptions
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def azurebloblocal_system(name: str):
    return require(name, AzureBlobLocalSystem)


def azurebloblocal_domain(name: str):
    return require(name, AzureBlobLocalDomain)

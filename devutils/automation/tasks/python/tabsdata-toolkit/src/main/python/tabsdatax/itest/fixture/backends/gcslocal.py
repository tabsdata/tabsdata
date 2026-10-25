#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``gcslocal`` backend: the local twin of
``gcs``, a container on this machine. Its descriptor carries the endpoint and
the polars / obstore ``storage_options``, so a test reads it exactly as it reads
``gcs``."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, StorageOptions, System, require


@dataclass(frozen=True)
class GcsLocalSystem(System):
    system_type: ClassVar[str] = "gcslocal"
    name: str
    system_kind: str
    docker_container_name: str
    image: str
    host: str
    port: str
    endpoint: str
    bucket: str
    project: str
    location: str
    storage_options: StorageOptions


@dataclass(frozen=True)
class GcsLocalDomain(Domain):
    system_type: ClassVar[str] = "gcslocal"
    name: str
    system_kind: str
    base_path: PurePosixPath
    prefix: str  # the domain's object-store prefix (== the entity container)
    uri: str
    endpoint: str
    bucket: str
    project: str
    storage_options: StorageOptions
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def gcslocal_system(name: str):
    return require(name, GcsLocalSystem)


def gcslocal_domain(name: str):
    return require(name, GcsLocalDomain)

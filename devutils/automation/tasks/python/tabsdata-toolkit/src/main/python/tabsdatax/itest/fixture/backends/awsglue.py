#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``awsglue`` backend."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from tabsdatax.itest.fixture.backend import Domain, StorageOptions, System, require


@dataclass(frozen=True)
class AwsGlueSystem(System):
    system_type: ClassVar[str] = "awsglue"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    bucket: str
    region: str
    access_key: str
    secret_key: str
    location: str
    storage_options: StorageOptions
    catalog_definition: StorageOptions  # pyiceberg's load_catalog keywords
    catalog_id: str | None = None  # blank: the catalog of the caller's account


@dataclass(frozen=True)
class AwsGlueDomain(Domain):
    system_type: ClassVar[str] = "awsglue"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    base_path: PurePosixPath
    uri: str
    bucket: str
    region: str
    access_key: str
    secret_key: str
    storage_options: StorageOptions
    catalog_definition: StorageOptions  # pyiceberg's load_catalog keywords
    glue_database: str  # the Glue database of the domain
    table_location: str  # where the database's tables are stored (== uri)
    catalog_id: str | None = None  # blank: the catalog of the caller's account


def awsglue_system(name: str):
    return require(name, AwsGlueSystem)


def awsglue_domain(name: str):
    return require(name, AwsGlueDomain)

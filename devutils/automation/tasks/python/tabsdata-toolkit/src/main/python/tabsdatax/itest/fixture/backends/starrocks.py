#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``starrocks`` backend."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class StarrocksSystem(System):
    system_type: ClassVar[str] = "starrocks"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str
    sys_path: Path | None = None  # v2 loads via SQL, not a staging root


@dataclass(frozen=True)
class StarrocksDomain(Domain):
    system_type: ClassVar[str] = "starrocks"
    name: str
    system_kind: str
    image: str
    host: str
    port: str
    user: str
    password: str
    db: str
    sqlalchemy_uri: str
    database: str  # starrocks calls the entity container a database (== db here)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def starrocks_system(name: str):
    return require(name, StarrocksSystem)


def starrocks_domain(name: str):
    return require(name, StarrocksDomain)

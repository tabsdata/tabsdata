#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``clickhouse`` backend."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class ClickhouseSystem(System):
    system_type: ClassVar[str] = "clickhouse"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str
    sys_path: Path


@dataclass(frozen=True)
class ClickhouseDomain(Domain):
    system_type: ClassVar[str] = "clickhouse"
    name: str
    system_kind: str
    image: str
    host: str
    port: str
    user: str
    password: str
    db: str
    sqlalchemy_uri: str
    database: str  # clickhouse calls the entity container a database (== db here)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def clickhouse_system(name: str):
    return require(name, ClickhouseSystem)


def clickhouse_domain(name: str):
    return require(name, ClickhouseDomain)

#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``snowflake`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class SnowflakeSystem(System):
    system_type: ClassVar[str] = "snowflake"
    name: str
    system_kind: str
    account: str
    username: str
    pat: str
    warehouse: str
    database: str


@dataclass(frozen=True)
class SnowflakeDomain(Domain):
    system_type: ClassVar[str] = "snowflake"
    name: str
    system_kind: str
    schema: str
    account: str
    username: str
    pat: str
    warehouse: str
    database: str
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def snowflake_system(name: str):
    return require(name, SnowflakeSystem)


def snowflake_domain(name: str):
    return require(name, SnowflakeDomain)
